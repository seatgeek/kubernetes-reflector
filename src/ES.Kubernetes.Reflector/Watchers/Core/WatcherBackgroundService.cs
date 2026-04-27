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

            var eventChannel = Channel.CreateBounded<WatcherEvent>(new BoundedChannelOptions(1024)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

            // Kubernetes namespace names must be valid DNS-1123 labels, which are lowercase-only,
            // so normalizing the configured exclusion patterns to lowercase ensures comparisons
            // against Metadata.NamespaceProperty are consistent without changing semantics.
            var excludedNamespacePatterns = GlobMatcher.ParseGlobPatterns(options.CurrentValue.Watcher?.ExcludedNamespaces?.ToLowerInvariant());
            long namespaceExcludedCount = 0;

            Task? consumerTask = null;
            try
            {
                if (excludedNamespacePatterns.Length > 0)
                    logger.LogInformation(
                        "Requesting {type} resources (excluding namespaces matching: {patterns})",
                        typeof(TResource).Name, options.CurrentValue.Watcher?.ExcludedNamespaces);
                else
                    logger.LogInformation("Requesting {type} resources", typeof(TResource).Name);

                //Read using a separate task so the watcher doesn't get stuck waiting on subscribers to handle the event
                consumerTask = Task.Run(async () =>
                {
                    await foreach (var watcherEvent in eventChannel.Reader.ReadAllAsync(cancellationToken))
                    {
                        foreach (var watcherEventHandler in watcherEventHandlers)
                            try
                            {
                                await watcherEventHandler.Handle(new WatcherEvent
                                {
                                    Item = watcherEvent.Item,
                                    EventType = watcherEvent.EventType
                                }, cancellationToken);
                            }
                            catch (Exception ex) when (ex is not OperationCanceledException)
                            {
                                logger.LogError(ex,
                                    "Error handling {eventType} event for {resourceType}",
                                    watcherEvent.EventType, typeof(TResource).Name);
                            }
                    }
                }, cancellationToken);

                var watchList = OnGetWatcher(cancellationToken);

                try
                {
                    await foreach (var (type, item) in watchList)
                    {
                        if (consumerTask.IsCompleted)
                        {
                            logger.LogWarning(
                                "Event consumer task has stopped unexpectedly for {type}. Forcing session reconnect.",
                                typeof(TResource).Name);
                            await cancellationCts.CancelAsync();
                            break;
                        }

                        // For cluster-scoped resources like V1Namespace, Metadata.NamespaceProperty is null,
                        // so this exclusion check intentionally becomes a no-op and namespace events
                        // continue flowing to support auto-reflection on new namespace creation.
                        if (GlobMatcher.IsNamespaceExcluded(item.Metadata?.NamespaceProperty, excludedNamespacePatterns))
                        {
                            namespaceExcludedCount++;
                            continue;
                        }

                        if (await OnResourceIgnoreCheck(item)) continue;
                        await eventChannel.Writer.WriteAsync(new WatcherEvent
                        {
                            Item = item,
                            EventType = type
                        }, cancellationToken).ConfigureAwait(false);
                    }
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

                if (consumerTask is not null)
                {
                    try
                    {
                        await consumerTask.ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Event consumer faulted for {type}.", typeof(TResource).Name);
                    }
                }

                var sessionElapsed = sessionStopwatch.Elapsed;
                sessionStopwatch.Stop();
                if (namespaceExcludedCount > 0)
                    logger.LogInformation(
                        "Session closed. Duration: {duration}. Faulted: {faulted}. Namespace-excluded events: {excluded}.",
                        sessionElapsed, sessionFaulted, namespaceExcludedCount);
                else
                    logger.LogInformation("Session closed. Duration: {duration}. Faulted: {faulted}.",
                        sessionElapsed, sessionFaulted);

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
