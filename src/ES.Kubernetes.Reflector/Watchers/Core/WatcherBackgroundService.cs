using System.Diagnostics;
using System.Threading.Channels;
using ES.Kubernetes.Reflector.Configuration;
using ES.Kubernetes.Reflector.Watchers.Core.Events;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Options;

namespace ES.Kubernetes.Reflector.Watchers.Core;

public abstract class WatcherBackgroundService<TResource, TResourceList>(
    ILogger logger,
    IOptionsMonitor<ReflectorOptions> options,
    IEnumerable<IWatcherEventHandler> watcherEventHandlers,
    IEnumerable<IWatcherClosedHandler> watcherClosedHandlers)
    : BackgroundService
    where TResource : IKubernetesObject<V1ObjectMeta>
{
    protected int WatcherTimeout => options.CurrentValue.Watcher?.Timeout ?? 3600;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var sessionStopwatch = new Stopwatch();
        while (!stoppingToken.IsCancellationRequested)
        {
            var sessionFaulted = false;
            sessionStopwatch.Restart();

            using var absoluteTimeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(WatcherTimeout + 3));
            using var cancellationCts =
                CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, absoluteTimeoutCts.Token);
            var cancellationToken = cancellationCts.Token;

            var eventChannel = Channel.CreateBounded<WatcherEvent>(new BoundedChannelOptions(256)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

            Task? consumerTask = null;
            long producedCount = 0;
            long consumedCount = 0;
            try
            {
                logger.LogInformation("Requesting {type} resources", typeof(TResource).Name);

                //Read using a separate task so the watcher doesn't get stuck waiting on subscribers to handle the event
                consumerTask = Task.Run(async () =>
                {
                    logger.LogDebug("[Consumer:{type}] Consumer task started.", typeof(TResource).Name);
                    var handlerStopwatch = new Stopwatch();
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        logger.LogTrace("[Consumer:{type}] Waiting to read from channel. Pending items: {count}",
                            typeof(TResource).Name, eventChannel.Reader.Count);
                        var watcherEvent = await eventChannel.Reader.ReadAsync(cancellationToken)
                            .ConfigureAwait(false);
                        var eventCount = Interlocked.Increment(ref consumedCount);
                        logger.LogDebug("[Consumer:{type}] Dequeued event #{count}: {eventType} for {resource}",
                            typeof(TResource).Name, eventCount, watcherEvent.EventType,
                            (watcherEvent.Item as IKubernetesObject<V1ObjectMeta>)?.Metadata?.Name ?? "unknown");
                        foreach (var watcherEventHandler in watcherEventHandlers)
                            try
                            {
                                var handlerName = watcherEventHandler.GetType().Name;
                                handlerStopwatch.Restart();
                                logger.LogTrace("[Consumer:{type}] Invoking handler {handler} for event #{count}",
                                    typeof(TResource).Name, handlerName, eventCount);
                                await watcherEventHandler.Handle(new WatcherEvent
                                {
                                    Item = watcherEvent.Item,
                                    EventType = watcherEvent.EventType
                                }, cancellationToken);
                                handlerStopwatch.Stop();
                                if (handlerStopwatch.Elapsed > TimeSpan.FromSeconds(5))
                                    logger.LogWarning(
                                        "[Consumer:{type}] Handler {handler} took {elapsed} for event #{count} ({eventType})",
                                        typeof(TResource).Name, handlerName, handlerStopwatch.Elapsed, eventCount,
                                        watcherEvent.EventType);
                                else
                                    logger.LogDebug(
                                        "[Consumer:{type}] Handler {handler} completed in {elapsed} for event #{count}",
                                        typeof(TResource).Name, handlerName, handlerStopwatch.Elapsed, eventCount);
                            }
                            catch (Exception ex) when (ex is not OperationCanceledException)
                            {
                                logger.LogError(ex,
                                    "Error handling {eventType} event for {resourceType}",
                                    watcherEvent.EventType, typeof(TResource).Name);
                            }
                    }
                    logger.LogDebug("[Consumer:{type}] Consumer loop exited (cancellation requested).", typeof(TResource).Name);
                }, cancellationToken);

                var watchList = OnGetWatcher(cancellationToken);

                try
                {
                    var writeStopwatch = new Stopwatch();
                    await foreach (var (type, item) in watchList)
                    {
                        if (consumerTask.IsCompleted)
                        {
                            logger.LogWarning(
                                "[Producer:{type}] Event consumer task has stopped unexpectedly (Status={status}). Forcing session reconnect.",
                                typeof(TResource).Name, consumerTask.Status);
                            if (consumerTask.Exception is not null)
                                logger.LogWarning(consumerTask.Exception,
                                    "[Producer:{type}] Consumer task exception details:", typeof(TResource).Name);
                            await cancellationCts.CancelAsync();
                            break;
                        }

                        if (await OnResourceIgnoreCheck(item)) continue;

                        var itemName = item.Metadata?.Name ?? "unknown";
                        var channelCount = eventChannel.Reader.Count;
                        var currentProduced = Interlocked.Increment(ref producedCount);
                        if (channelCount > 200)
                            logger.LogWarning(
                                "[Producer:{type}] Channel near capacity: {count}/256 before writing event #{eventNum} ({eventType} {name})",
                                typeof(TResource).Name, channelCount, currentProduced, type, itemName);
                        else
                            logger.LogDebug(
                                "[Producer:{type}] Writing event #{eventNum} ({eventType} {name}). Channel depth: {count}/256",
                                typeof(TResource).Name, currentProduced, type, itemName, channelCount);

                        writeStopwatch.Restart();
                        await eventChannel.Writer.WriteAsync(new WatcherEvent
                        {
                            Item = item,
                            EventType = type
                        }, cancellationToken).ConfigureAwait(false);
                        writeStopwatch.Stop();
                        if (writeStopwatch.Elapsed > TimeSpan.FromSeconds(1))
                            logger.LogWarning(
                                "[Producer:{type}] WriteAsync blocked for {elapsed} on event #{eventNum} — channel was full (consumer may be stalled)",
                                typeof(TResource).Name, writeStopwatch.Elapsed, currentProduced);
                    }

                    logger.LogDebug("[Producer:{type}] Watch stream ended. Total events produced: {count}",
                        typeof(TResource).Name, Volatile.Read(ref producedCount));
                }
                catch (OperationCanceledException)
                {
                    logger.LogTrace("Event channel writing canceled.");
                }
            }
            catch (TaskCanceledException)
            {
                logger.LogTrace("Session canceled using token.");
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Faulted due to exception.");
                sessionFaulted = true;
            }
            finally
            {
                eventChannel.Writer.Complete();
                logger.LogDebug("[Cleanup:{type}] Channel writer completed. Produced={produced}, Consumed={consumed}",
                    typeof(TResource).Name, Volatile.Read(ref producedCount), Volatile.Read(ref consumedCount));

                if (consumerTask is not null)
                {
                    logger.LogDebug("[Cleanup:{type}] Awaiting consumer task (Status={status})...",
                        typeof(TResource).Name, consumerTask.Status);
                    var drainStopwatch = Stopwatch.StartNew();
                    try
                    {
                        // Give the consumer a bounded time to drain before we give up
                        var completed = await Task.WhenAny(consumerTask, Task.Delay(TimeSpan.FromSeconds(30), CancellationToken.None))
                            .ConfigureAwait(false);
                        if (completed != consumerTask)
                            logger.LogWarning(
                                "[Cleanup:{type}] Consumer task did not finish within 30s drain timeout (Status={status}). Possible deadlock.",
                                typeof(TResource).Name, consumerTask.Status);
                        else
                            await consumerTask.ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Event consumer faulted for {type}.", typeof(TResource).Name);
                    }
                    drainStopwatch.Stop();
                    logger.LogDebug("[Cleanup:{type}] Consumer task drain took {elapsed}. Final status: {status}",
                        typeof(TResource).Name, drainStopwatch.Elapsed, consumerTask.Status);
                }

                var drained = 0;
                while (eventChannel.Reader.TryRead(out _)) drained++;
                if (drained > 0)
                    logger.LogWarning("[Cleanup:{type}] Drained {count} unconsumed events from channel.",
                        typeof(TResource).Name, drained);

                var sessionElapsed = sessionStopwatch.Elapsed;
                sessionStopwatch.Stop();
                logger.LogInformation("Session closed. Duration: {duration}. Faulted: {faulted}.", sessionElapsed,
                    sessionFaulted);

                foreach (var handler in watcherClosedHandlers)
                    await handler.Handle(new WatcherClosed
                    {
                        ResourceType = typeof(TResource),
                        Faulted = sessionFaulted
                    }, stoppingToken);
            }
        }
    }

    protected abstract IAsyncEnumerable<(WatchEventType, TResource)> OnGetWatcher(CancellationToken cancellationToken);

    protected virtual Task<bool> OnResourceIgnoreCheck(TResource item) => Task.FromResult(false);
}