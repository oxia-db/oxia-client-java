/*
 * Copyright © 2022-2026 The Oxia Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.oxia.client;

import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.buffer.ByteBufUtil;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.RangeScanConsumer;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.DeleteRangeOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.GetSequenceUpdatesOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import io.oxia.client.batch.BatchManager;
import io.oxia.client.batch.BatcherPool;
import io.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.client.metrics.Counter;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.metrics.LatencyHistogram;
import io.oxia.client.metrics.Unit;
import io.oxia.client.metrics.UpDownCounter;
import io.oxia.client.notify.NotificationManager;
import io.oxia.client.operation.rangescan.CompositeRangeScanConsumer;
import io.oxia.client.options.GetOptions;
import io.oxia.client.session.SessionManager;
import io.oxia.client.shard.ShardManager;
import io.oxia.client.util.PendingBytesLimiter;
import io.oxia.proto.KeyComparisonType;
import io.oxia.proto.ListRequest;
import io.oxia.proto.ListResponse;
import io.oxia.proto.RangeScanRequest;
import io.oxia.proto.RangeScanResponse;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.NonNull;

class AsyncOxiaClientImpl implements AsyncOxiaClient {

    static @NonNull CompletableFuture<AsyncOxiaClient> newInstance(@NonNull ClientConfig config) {
        final ScheduledExecutorService asyncExecutor =
                Executors.newScheduledThreadPool(
                        Runtime.getRuntime().availableProcessors(),
                        new DefaultThreadFactory("oxia-client-async"));
        var instrumentProvider = new InstrumentProvider(config.openTelemetry(), config.namespace());
        var shardManagerRef = new AtomicReference<ShardManager>();
        var rpcProvider =
                RpcProvider.create(config, asyncExecutor, shardId -> shardManagerRef.get().leader(shardId));
        var shardManager =
                new ShardManager(asyncExecutor, rpcProvider, instrumentProvider, config.namespace());
        shardManagerRef.set(shardManager);
        var notificationManager =
                new NotificationManager(asyncExecutor, rpcProvider, shardManager, instrumentProvider);
        shardManager.addCallback(notificationManager);
        var readBatcherPool = new BatcherPool("oxia-read-batcher", config.batchingThreads());
        var readBatchManager =
                BatchManager.newReadBatchManager(
                        config, rpcProvider, instrumentProvider, readBatcherPool, true);
        var sessionManager = new SessionManager(asyncExecutor, config, rpcProvider, instrumentProvider);
        shardManager.addCallback(sessionManager);
        var writeBatcherPool = new BatcherPool("oxia-write-batcher", config.batchingThreads());
        var writeBatchManager =
                BatchManager.newWriteBatchManager(
                        config, rpcProvider, sessionManager, instrumentProvider, writeBatcherPool, true);

        var client =
                new AsyncOxiaClientImpl(
                        config.clientIdentifier(),
                        asyncExecutor,
                        instrumentProvider,
                        rpcProvider,
                        shardManager,
                        notificationManager,
                        readBatchManager,
                        writeBatchManager,
                        sessionManager,
                        config.requestTimeout(),
                        config.maxPendingBytes(),
                        true);
        return shardManager.start().thenApply(v -> client);
    }

    /**
     * Create a client that borrows its executor, connection pool and shard-assignment stream from a
     * shared {@link SharedResourcesImpl} pool. Closing the returned client only tears down its own
     * per-client state (sessions, notifications, batchers); the shared resources live until the pool
     * itself is closed.
     */
    static @NonNull CompletableFuture<AsyncOxiaClient> newInstance(
            @NonNull ClientConfig config, @NonNull SharedResourcesImpl sharedResources) {
        final ScheduledExecutorService asyncExecutor = sharedResources.executor();
        final var connectionManager = sharedResources.connectionManager();
        var instrumentProvider = new InstrumentProvider(config.openTelemetry(), config.namespace());
        return sharedResources
                .getOrCreateShardManager(config)
                .thenApply(
                        shardManager -> {
                            var rpcProvider =
                                    RpcProvider.create(
                                            config, asyncExecutor, connectionManager, shardManager::leader);
                            var notificationManager =
                                    new NotificationManager(
                                            asyncExecutor, rpcProvider, shardManager, instrumentProvider);
                            shardManager.addCallback(notificationManager);
                            var readBatchManager =
                                    BatchManager.newReadBatchManager(
                                            config,
                                            rpcProvider,
                                            instrumentProvider,
                                            sharedResources.readBatcherPool(),
                                            false);
                            var sessionManager =
                                    new SessionManager(asyncExecutor, config, rpcProvider, instrumentProvider);
                            shardManager.addCallback(sessionManager);
                            var writeBatchManager =
                                    BatchManager.newWriteBatchManager(
                                            config,
                                            rpcProvider,
                                            sessionManager,
                                            instrumentProvider,
                                            sharedResources.writeBatcherPool(),
                                            false);
                            return new AsyncOxiaClientImpl(
                                    config.clientIdentifier(),
                                    asyncExecutor,
                                    instrumentProvider,
                                    rpcProvider,
                                    shardManager,
                                    notificationManager,
                                    readBatchManager,
                                    writeBatchManager,
                                    sessionManager,
                                    config.requestTimeout(),
                                    config.maxPendingBytes(),
                                    false);
                        });
    }

    private final @NonNull String clientIdentifier;
    private final @NonNull InstrumentProvider instrumentProvider;
    private final @NonNull RpcProvider rpcProvider;
    private final @NonNull ShardManager shardManager;
    private final @NonNull NotificationManager notificationManager;
    private final @NonNull BatchManager readBatchManager;
    private final @NonNull BatchManager writeBatchManager;
    private final @NonNull SessionManager sessionManager;
    private final long requestTimeoutMs;
    private final @NonNull PendingBytesLimiter pendingBytesLimiter;
    private volatile boolean closed;

    private final Counter counterPutBytes;
    private final Counter counterGetBytes;
    private final Counter counterListBytes;
    private final Counter counterRangeScanBytes;

    private final UpDownCounter gaugePendingPutRequests;
    private final UpDownCounter gaugePendingGetRequests;
    private final UpDownCounter gaugePendingListRequests;
    private final UpDownCounter gaugePendingRangeScanRequests;
    private final UpDownCounter gaugePendingDeleteRequests;
    private final UpDownCounter gaugePendingDeleteRangeRequests;

    private final UpDownCounter gaugePendingPutBytes;

    private final LatencyHistogram histogramPutLatency;
    private final LatencyHistogram histogramGetLatency;
    private final LatencyHistogram histogramDeleteLatency;
    private final LatencyHistogram histogramDeleteRangeLatency;
    private final LatencyHistogram histogramListLatency;
    private final LatencyHistogram histogramRangeScanLatency;

    private final ScheduledExecutorService scheduledExecutor;

    /**
     * Whether this client owns the {@link #scheduledExecutor} and {@link #shardManager}. {@code true}
     * for standalone clients (they close them on {@link #close()}); {@code false} for clients backed
     * by a shared-resources pool (which owns and closes them).
     */
    private final boolean ownsResources;

    AsyncOxiaClientImpl(
            @NonNull String clientIdentifier,
            @NonNull ScheduledExecutorService scheduledExecutor,
            @NonNull InstrumentProvider instrumentProvider,
            @NonNull RpcProvider rpcProvider,
            @NonNull ShardManager shardManager,
            @NonNull NotificationManager notificationManager,
            @NonNull BatchManager readBatchManager,
            @NonNull BatchManager writeBatchManager,
            @NonNull SessionManager sessionManager,
            Duration requestTimeout,
            long maxPendingBytes,
            boolean ownsResources) {
        this.clientIdentifier = clientIdentifier;
        this.pendingBytesLimiter = new PendingBytesLimiter(maxPendingBytes);
        this.instrumentProvider = instrumentProvider;
        this.rpcProvider = rpcProvider;
        this.shardManager = shardManager;
        this.notificationManager = notificationManager;
        this.readBatchManager = readBatchManager;
        this.writeBatchManager = writeBatchManager;
        this.sessionManager = sessionManager;
        this.scheduledExecutor = scheduledExecutor;
        this.ownsResources = ownsResources;
        this.requestTimeoutMs = requestTimeout.toMillis();

        counterPutBytes =
                instrumentProvider.newCounter(
                        "oxia.client.ops.size",
                        Unit.Bytes,
                        "Total number of bytes in operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "put"));
        counterGetBytes =
                instrumentProvider.newCounter(
                        "oxia.client.ops.size",
                        Unit.Bytes,
                        "Total number of bytes in operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "get"));
        counterListBytes =
                instrumentProvider.newCounter(
                        "oxia.client.ops.size",
                        Unit.Bytes,
                        "Total number of bytes in operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "list"));
        counterRangeScanBytes =
                instrumentProvider.newCounter(
                        "oxia.client.ops.size",
                        Unit.Bytes,
                        "Total number of bytes in operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "range-scan"));

        gaugePendingPutRequests =
                instrumentProvider.newUpDownCounter(
                        "oxia.client.ops.pending",
                        Unit.Events,
                        "Current number of outstanding requests",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "put"));
        gaugePendingGetRequests =
                instrumentProvider.newUpDownCounter(
                        "oxia.client.ops.pending",
                        Unit.Events,
                        "Current number of outstanding requests",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "get"));
        gaugePendingListRequests =
                instrumentProvider.newUpDownCounter(
                        "oxia.client.ops.pending",
                        Unit.Events,
                        "Current number of outstanding requests",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "list"));
        gaugePendingRangeScanRequests =
                instrumentProvider.newUpDownCounter(
                        "oxia.client.ops.pending",
                        Unit.Events,
                        "Current number of outstanding requests",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "range-scan"));
        gaugePendingDeleteRequests =
                instrumentProvider.newUpDownCounter(
                        "oxia.client.ops.pending",
                        Unit.Events,
                        "Current number of outstanding requests",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "delete"));
        gaugePendingDeleteRangeRequests =
                instrumentProvider.newUpDownCounter(
                        "oxia.client.ops.pending",
                        Unit.Events,
                        "Current number of outstanding requests",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "delete-range"));

        gaugePendingPutBytes =
                instrumentProvider.newUpDownCounter(
                        "oxia.client.ops.outstanding",
                        Unit.Bytes,
                        "Current number of outstanding bytes in put operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "put"));

        histogramPutLatency =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops",
                        "Duration of operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "put"));

        histogramGetLatency =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops",
                        "Duration of operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "get"));

        histogramDeleteLatency =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops",
                        "Duration of operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "delete"));

        histogramDeleteRangeLatency =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops",
                        "Duration of operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "delete-range"));

        histogramListLatency =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops",
                        "Duration of operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "list"));
        histogramRangeScanLatency =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops",
                        "Duration of operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "range-scan"));
    }

    @Override
    public @NonNull CompletableFuture<PutResult> put(String key, byte[] value) {
        return put(key, value, Collections.emptySet());
    }

    @Override
    public @NonNull CompletableFuture<PutResult> put(
            String key, byte[] value, Set<PutOption> options) {
        long startTime = System.nanoTime();
        CompletableFuture<PutResult> callback;

        long acquiredBytes = 0;
        try {
            checkIfClosed();
            Objects.requireNonNull(key);
            Objects.requireNonNull(value);

            long size = ByteBufUtil.utf8Bytes(key) + value.length;
            pendingBytesLimiter.acquire(size);
            acquiredBytes = size;

            callback = internalPut(key, value, options);
        } catch (RuntimeException e) {
            callback = CompletableFuture.failedFuture(e);
        }
        final long pendingBytes = acquiredBytes;
        return callback
                .orTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .whenComplete(
                        (putResult, throwable) -> {
                            if (pendingBytes > 0) {
                                pendingBytesLimiter.release(pendingBytes);
                            }
                            gaugePendingPutRequests.decrement();
                            gaugePendingPutBytes.add(-value.length);

                            if (throwable == null) {
                                counterPutBytes.add(value.length);
                                histogramPutLatency.recordSuccess(System.nanoTime() - startTime);
                            } else {
                                histogramPutLatency.recordFailure(System.nanoTime() - startTime);
                            }
                        });
    }

    private CompletableFuture<PutResult> internalPut(
            String key, byte[] value, Set<PutOption> options) {
        gaugePendingPutRequests.increment();
        gaugePendingPutBytes.add(value.length);

        var partitionKey = OptionsUtils.getPartitionKey(options);
        var shardId = shardManager.getShardForKey(partitionKey.orElse(key));
        var versionId = OptionsUtils.getVersionId(options);
        var sequenceKeysDeltas = OptionsUtils.getSequenceKeysDeltas(options);
        var secondaryIndexes = OptionsUtils.getSecondaryIndexes(options);
        var overrideVersionId = OptionsUtils.getOverrideVersionId(options);
        var overrideModificationsCount = OptionsUtils.getOverrideModificationsCount(options);

        CompletableFuture<PutResult> future = new CompletableFuture<>();

        if (!OptionsUtils.isEphemeral(options)) {
            var op =
                    new PutOperation(
                            shardId,
                            future,
                            key,
                            partitionKey,
                            sequenceKeysDeltas,
                            value,
                            versionId,
                            OptionalLong.empty(),
                            Optional.empty(),
                            secondaryIndexes,
                            overrideVersionId,
                            overrideModificationsCount);
            writeBatchManager.add(op);
        } else {
            // The put operation is trying to write an ephemeral record. We need to have a valid session
            // id for this
            sessionManager
                    .getSession(shardId)
                    .thenAccept(
                            session -> {
                                var op =
                                        new PutOperation(
                                                shardId,
                                                future,
                                                key,
                                                partitionKey,
                                                sequenceKeysDeltas,
                                                value,
                                                versionId,
                                                OptionalLong.of(session.getSessionId()),
                                                Optional.of(clientIdentifier),
                                                secondaryIndexes,
                                                overrideVersionId,
                                                overrideModificationsCount);
                                writeBatchManager.add(op);
                            })
                    .exceptionally(
                            ex -> {
                                future.completeExceptionally(ex);
                                return null;
                            });
        }

        return future;
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(String key) {
        return delete(key, Collections.emptySet());
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(String key, Set<DeleteOption> options) {
        long startTime = System.nanoTime();

        gaugePendingDeleteRequests.increment();

        var callback = new CompletableFuture<Boolean>();
        long acquiredBytes = 0;
        try {
            checkIfClosed();
            Objects.requireNonNull(key);

            OptionalLong versionId = OptionsUtils.getVersionId(options);
            var partitionKey = OptionsUtils.getPartitionKey(options);
            var shardId = shardManager.getShardForKey(partitionKey.orElse(key));

            long size = ByteBufUtil.utf8Bytes(key);
            pendingBytesLimiter.acquire(size);
            acquiredBytes = size;

            writeBatchManager.add(new DeleteOperation(shardId, callback, key, versionId));
        } catch (RuntimeException e) {
            callback.completeExceptionally(e);
        }
        final long pendingBytes = acquiredBytes;
        return callback
                .orTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .whenComplete(
                        (putResult, throwable) -> {
                            if (pendingBytes > 0) {
                                pendingBytesLimiter.release(pendingBytes);
                            }
                            gaugePendingDeleteRequests.decrement();
                            if (throwable == null) {
                                histogramDeleteLatency.recordSuccess(System.nanoTime() - startTime);
                            } else {
                                histogramDeleteLatency.recordFailure(System.nanoTime() - startTime);
                            }
                        });
    }

    @Override
    public @NonNull CompletableFuture<Void> deleteRange(
            String startKeyInclusive, String endKeyExclusive) {
        return deleteRange(startKeyInclusive, endKeyExclusive, Collections.emptySet());
    }

    @Override
    public @NonNull CompletableFuture<Void> deleteRange(
            String startKeyInclusive, String endKeyExclusive, Set<DeleteRangeOption> options) {
        long startTime = System.nanoTime();
        gaugePendingDeleteRangeRequests.increment();
        CompletableFuture<Void> callback;
        long acquiredBytes = 0;
        try {
            checkIfClosed();
            Objects.requireNonNull(startKeyInclusive);
            Objects.requireNonNull(endKeyExclusive);

            long size = ByteBufUtil.utf8Bytes(startKeyInclusive) + ByteBufUtil.utf8Bytes(endKeyExclusive);
            pendingBytesLimiter.acquire(size);
            acquiredBytes = size;

            var partitionKey = OptionsUtils.getPartitionKey(options);
            if (partitionKey.isPresent()) {
                // When partition key is present, we only need to send the request to a single shard
                var shardId = shardManager.getShardForKey(partitionKey.get());
                callback = new CompletableFuture<>();
                writeBatchManager.add(
                        new DeleteRangeOperation(shardId, callback, startKeyInclusive, endKeyExclusive));
            } else {
                // Perform the delete range on all the shards
                var shardDeletes =
                        shardManager.allShardIds().stream()
                                .map(
                                        shardId -> {
                                            var shardCallback = new CompletableFuture<Void>();
                                            writeBatchManager.add(
                                                    new DeleteRangeOperation(
                                                            shardId, shardCallback, startKeyInclusive, endKeyExclusive));
                                            return shardCallback;
                                        })
                                .toArray(CompletableFuture[]::new);
                callback = CompletableFuture.allOf(shardDeletes);
            }
        } catch (RuntimeException e) {
            callback = CompletableFuture.failedFuture(e);
        }
        final long pendingBytes = acquiredBytes;
        return callback
                .orTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .whenComplete(
                        (putResult, throwable) -> {
                            if (pendingBytes > 0) {
                                pendingBytesLimiter.release(pendingBytes);
                            }
                            gaugePendingDeleteRangeRequests.decrement();
                            if (throwable == null) {
                                histogramDeleteRangeLatency.recordSuccess(System.nanoTime() - startTime);
                            } else {
                                histogramDeleteRangeLatency.recordFailure(System.nanoTime() - startTime);
                            }
                        });
    }

    @Override
    public @NonNull CompletableFuture<GetResult> get(String key) {
        return get(key, Collections.emptySet());
    }

    @Override
    public @NonNull CompletableFuture<GetResult> get(String key, Set<GetOption> options) {
        final GetOptions internalOptions = GetOptions.parseFrom(options);
        long startTime = System.nanoTime();
        gaugePendingGetRequests.increment();
        var callback = new CompletableFuture<GetResult>();
        long acquiredBytes = 0;
        try {
            checkIfClosed();
            Objects.requireNonNull(key);

            long size = ByteBufUtil.utf8Bytes(key);
            pendingBytesLimiter.acquire(size);
            acquiredBytes = size;

            internalGet(key, internalOptions, callback);
        } catch (RuntimeException e) {
            callback.completeExceptionally(e);
        }
        final long pendingBytes = acquiredBytes;
        return callback
                .orTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .whenComplete(
                        (getResult, throwable) -> {
                            if (pendingBytes > 0) {
                                pendingBytesLimiter.release(pendingBytes);
                            }
                            gaugePendingGetRequests.decrement();
                            if (throwable == null) {
                                if (getResult != null) {
                                    counterGetBytes.add(getResult.value().length);
                                }
                                histogramGetLatency.recordSuccess(System.nanoTime() - startTime);
                            } else {
                                histogramGetLatency.recordFailure(System.nanoTime() - startTime);
                            }
                        });
    }

    private void internalGet(String key, GetOptions options, CompletableFuture<GetResult> result) {
        if (options.partitionKey() == null
                && (options.comparisonType() != KeyComparisonType.EQUAL
                        || options.secondaryIndexName() != null)) {
            internalGetMultiShards(key, options, result);
        } else {
            // Single shard get operation
            long shardId =
                    shardManager.getShardForKey(Optional.ofNullable(options.partitionKey()).orElse(key));
            readBatchManager.add(new GetOperation(shardId, result, key, options));
        }
    }

    private void internalGetMultiShards(
            String key, GetOptions options, CompletableFuture<GetResult> result) {
        // We need check on all the shards for a floor/ceiling query
        List<CompletableFuture<GetResult>> futures = new ArrayList<>();
        for (long shardId : shardManager.allShardIds()) {
            CompletableFuture<GetResult> f = new CompletableFuture<>();
            readBatchManager.add(new GetOperation(shardId, f, key, options));
            futures.add(f);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete(
                        (v, ex) -> {
                            if (ex != null) {
                                result.completeExceptionally(ex);
                                return;
                            }

                            try {
                                List<GetResult> results =
                                        futures.stream()
                                                .map(CompletableFuture::join)
                                                .filter(Objects::nonNull)
                                                .sorted((o1, o2) -> CompareWithSlash.INSTANCE.compare(o1.key(), o2.key()))
                                                .toList();
                                if (results.isEmpty()) {
                                    result.complete(null);
                                    return;
                                }

                                GetResult gr =
                                        switch (options.comparisonType()) {
                                            case EQUAL, CEILING, HIGHER -> results.get(0);
                                            case FLOOR, LOWER -> results.get(results.size() - 1);
                                            default -> null;
                                        };

                                result.complete(gr);
                            } catch (Throwable t) {
                                result.completeExceptionally(t);
                            }
                        });
    }

    @Override
    public @NonNull CompletableFuture<List<String>> list(
            String startKeyInclusive, String endKeyExclusive) {
        return list(startKeyInclusive, endKeyExclusive, Collections.emptySet());
    }

    @Override
    public @NonNull CompletableFuture<List<String>> list(
            String startKeyInclusive, String endKeyExclusive, Set<ListOption> options) {
        long startTime = System.nanoTime();
        gaugePendingListRequests.increment();
        CompletableFuture<List<String>> callback;
        try {
            checkIfClosed();
            Objects.requireNonNull(startKeyInclusive);
            Objects.requireNonNull(endKeyExclusive);

            Optional<String> partitionKey = OptionsUtils.getPartitionKey(options);
            Optional<String> secondaryIndex = OptionsUtils.getSecondaryIndexName(options);
            if (partitionKey.isPresent()) {
                long shardId = shardManager.getShardForKey(partitionKey.get());
                callback = internalShardlist(shardId, startKeyInclusive, endKeyExclusive, secondaryIndex);
            } else {
                callback = internalListMultiShards(startKeyInclusive, endKeyExclusive, secondaryIndex);
            }
        } catch (Exception e) {
            callback = CompletableFuture.failedFuture(e);
        }
        return callback
                .orTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .whenComplete(
                        (listResult, throwable) -> {
                            gaugePendingListRequests.decrement();
                            if (throwable == null) {
                                counterListBytes.add(listResult.stream().mapToInt(String::length).sum());
                                histogramListLatency.recordSuccess(System.nanoTime() - startTime);
                            } else {
                                histogramListLatency.recordFailure(System.nanoTime() - startTime);
                            }
                        });
    }

    @Override
    public void notifications(@NonNull Consumer<Notification> notificationCallback) {
        checkIfClosed();
        notificationManager.registerCallback(notificationCallback);
    }

    private CompletableFuture<List<String>> internalListMultiShards(
            String startKeyInclusive, String endKeyExclusive, Optional<String> secondaryIndex) {
        List<CompletableFuture<List<String>>> futures = new ArrayList<>();
        for (long shardId : shardManager.allShardIds()) {
            futures.add(internalShardlist(shardId, startKeyInclusive, endKeyExclusive, secondaryIndex));
        }

        CompletableFuture<List<String>> result = new CompletableFuture<>();
        List<String> list = new ArrayList<>();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(
                        () -> {
                            for (var future : futures) {
                                list.addAll(future.join());
                            }

                            list.sort(CompareWithSlash.INSTANCE);
                            result.complete(list);
                        })
                .exceptionally(
                        ex -> {
                            result.completeExceptionally(ex);
                            return null;
                        });

        return result;
    }

    private CompletableFuture<List<String>> internalShardlist(
            long shardId,
            String startKeyInclusive,
            String endKeyExclusive,
            Optional<String> secondaryIndexName) {
        var request = new ListRequest();
        request.setShard(shardId).setStartInclusive(startKeyInclusive).setEndExclusive(endKeyExclusive);
        secondaryIndexName.ifPresent(request::setSecondaryIndexName);

        CompletableFuture<List<String>> future = new CompletableFuture<>();
        List<String> result = new ArrayList<>();
        rpcProvider.list(
                request,
                new CancelableStreamObserver<>() {
                    @Override
                    protected void handleNext(@NonNull ListResponse response) {
                        for (int i = 0; i < response.getKeysCount(); i++) {
                            result.add(response.getKeyAt(i));
                        }
                    }

                    @Override
                    protected void handleError(@NonNull Throwable t) {
                        future.completeExceptionally(t);
                    }

                    @Override
                    protected void handleComplete() {
                        future.complete(result);
                    }
                });
        return future;
    }

    @Override
    public Closeable getSequenceUpdates(
            @NonNull String key,
            @NonNull Consumer<String> listener,
            @NonNull Set<GetSequenceUpdatesOption> options) {
        checkIfClosed();
        var partitionKey = OptionsUtils.getPartitionKey(options);
        if (partitionKey.isEmpty()) {
            throw new IllegalArgumentException("partitionKey must be present");
        }

        return new SequenceUpdates(
                key,
                partitionKey.get(),
                listener,
                this.rpcProvider,
                this.shardManager,
                this.instrumentProvider,
                x -> closed);
    }

    @Override
    public void rangeScan(
            @NonNull String startKeyInclusive,
            @NonNull String endKeyExclusive,
            @NonNull RangeScanConsumer consumer) {
        rangeScan(startKeyInclusive, endKeyExclusive, consumer, Collections.emptySet());
    }

    @Override
    public void rangeScan(
            @NonNull String startKeyInclusive,
            @NonNull String endKeyExclusive,
            @NonNull RangeScanConsumer consumer,
            @NonNull Set<RangeScanOption> options) {
        gaugePendingRangeScanRequests.increment();

        final RangeScanConsumer timedConsumer =
                new RangeScanConsumer() {
                    final long startTime = System.nanoTime();
                    final AtomicLong totalSize = new AtomicLong();

                    @Override
                    public boolean onNext(GetResult result) {
                        totalSize.addAndGet(result.value().length);
                        return consumer.onNext(result);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        gaugePendingRangeScanRequests.decrement();
                        histogramRangeScanLatency.recordFailure(System.nanoTime() - startTime);
                        consumer.onError(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        gaugePendingRangeScanRequests.decrement();
                        counterRangeScanBytes.add(totalSize.longValue());
                        histogramRangeScanLatency.recordSuccess(System.nanoTime() - startTime);
                        consumer.onCompleted();
                    }
                };

        try {
            checkIfClosed();
            Objects.requireNonNull(startKeyInclusive);
            Objects.requireNonNull(endKeyExclusive);

            final var flowControl = consumer instanceof FlowControlledRangeScanConsumer fc ? fc : null;

            final Optional<String> partitionKey = OptionsUtils.getPartitionKey(options);
            final Optional<String> secondaryIndexName = OptionsUtils.getSecondaryIndexName(options);
            if (partitionKey.isPresent()) {
                long shardId = shardManager.getShardForKey(partitionKey.get());
                internalShardRangeScan(
                        shardId,
                        startKeyInclusive,
                        endKeyExclusive,
                        secondaryIndexName,
                        timedConsumer,
                        flowControl);
                return;
            }
            final Set<Long> shardIds = shardManager.allShardIds();
            final CompositeRangeScanConsumer multiShardConsumer =
                    new CompositeRangeScanConsumer(shardIds.size(), timedConsumer);
            for (Long shardId : shardIds) {
                internalShardRangeScan(
                        shardId,
                        startKeyInclusive,
                        endKeyExclusive,
                        secondaryIndexName,
                        multiShardConsumer,
                        flowControl);
            }
        } catch (Exception e) {
            consumer.onError(e);
        }
    }

    private void internalShardRangeScan(
            long shardId,
            String startKeyInclusive,
            String endKeyExclusive,
            Optional<String> secondaryIndexName,
            RangeScanConsumer consumer,
            FlowControlledRangeScanConsumer flowControl) {
        var request = new RangeScanRequest();
        request.setShard(shardId).setStartInclusive(startKeyInclusive).setEndExclusive(endKeyExclusive);
        secondaryIndexName.ifPresent(request::setSecondaryIndexName);
        var observer = new RangeScanShardObserver(consumer, flowControl);
        rpcProvider.rangeScan(request, observer);
        if (flowControl != null) {
            flowControl.onStreamStarted(observer);
        }
    }

    private static final class RangeScanShardObserver
            extends CancelableStreamObserver<RangeScanResponse>
            implements FlowControlledRangeScanConsumer.StreamHandle {
        private final RangeScanConsumer consumer;
        private final FlowControlledRangeScanConsumer flowControl;

        RangeScanShardObserver(
                RangeScanConsumer consumer, FlowControlledRangeScanConsumer flowControl) {
            super(flowControl != null);
            this.consumer = consumer;
            this.flowControl = flowControl;
        }

        @Override
        protected void handleNext(RangeScanResponse response) {
            for (int i = 0; i < response.getRecordsCount(); i++) {
                final boolean needNext =
                        consumer.onNext(ProtoUtil.getResultFromProto("", response.getRecordAt(i)));
                if (!needNext) {
                    cancelAndComplete();
                    return;
                }
            }
            if (flowControl != null) {
                flowControl.onStreamIdle(this);
            }
        }

        @Override
        protected void handleError(Throwable t) {
            consumer.onError(t);
        }

        @Override
        protected void handleComplete() {
            consumer.onCompleted();
        }

        @Override
        public void requestNext() {
            requestNextMessage();
        }

        // StreamHandle.cancel() is satisfied by CancelableStreamObserver.cancel()
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        if (!ownsResources) {
            // Detach from the shared shard-assignment stream so it stops dispatching to this client.
            shardManager.removeCallback(sessionManager);
            shardManager.removeCallback(notificationManager);
        }
        readBatchManager.close();
        writeBatchManager.close();
        sessionManager.close();
        notificationManager.close();
        if (ownsResources) {
            shardManager.close();
        }
        // In shared mode the RpcProvider does not own the connection pool, so this only closes the
        // per-client write streams; the shared connections stay open for other clients.
        rpcProvider.close();
        if (ownsResources) {
            scheduledExecutor.shutdownNow();
        }
    }

    private void checkIfClosed() {
        if (closed) {
            throw new IllegalStateException("Client has been closed");
        }
    }
}
