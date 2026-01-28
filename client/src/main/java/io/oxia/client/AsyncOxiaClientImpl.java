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
import io.grpc.stub.StreamObserver;
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
import io.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.oxia.client.grpc.OxiaStubManager;
import io.oxia.client.grpc.OxiaStubProvider;
import io.oxia.client.metrics.Counter;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.metrics.LatencyHistogram;
import io.oxia.client.metrics.Unit;
import io.oxia.client.metrics.UpDownCounter;
import io.oxia.client.notify.NotificationManager;
import io.oxia.client.options.GetOptions;
import io.oxia.client.session.SessionManager;
import io.oxia.client.shard.ShardManager;
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
import java.util.function.Consumer;
import lombok.NonNull;

class AsyncOxiaClientImpl implements AsyncOxiaClient {

    static @NonNull CompletableFuture<AsyncOxiaClient> newInstance(@NonNull ClientConfig config) {
        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("oxia-client"));
        var stubManager = new OxiaStubManager(config);

        var instrumentProvider = new InstrumentProvider(config.openTelemetry(), config.namespace());
        var serviceAddrStub = stubManager.getStub(config.serviceAddress());
        var shardManager =
                new ShardManager(executor, serviceAddrStub, instrumentProvider, config.namespace());
        var notificationManager =
                new NotificationManager(executor, stubManager, shardManager, instrumentProvider);
        final var stubProvider = new OxiaStubProvider(config.namespace(), stubManager, shardManager);
        shardManager.addCallback(notificationManager);
        var readBatchManager =
                BatchManager.newReadBatchManager(config, stubProvider, instrumentProvider);
        var sessionManager = new SessionManager(executor, config, stubProvider, instrumentProvider);
        shardManager.addCallback(sessionManager);
        var writeBatchManager =
                BatchManager.newWriteBatchManager(config, stubProvider, sessionManager, instrumentProvider);

        var client =
                new AsyncOxiaClientImpl(
                        config.clientIdentifier(),
                        executor,
                        instrumentProvider,
                        stubManager,
                        shardManager,
                        notificationManager,
                        readBatchManager,
                        writeBatchManager,
                        sessionManager,
                        config.requestTimeout());
        return shardManager.start().thenApply(v -> client);
    }

    private final @NonNull String clientIdentifier;
    private final @NonNull InstrumentProvider instrumentProvider;
    private final @NonNull OxiaStubManager stubManager;
    private final @NonNull ShardManager shardManager;
    private final @NonNull NotificationManager notificationManager;
    private final @NonNull BatchManager readBatchManager;
    private final @NonNull BatchManager writeBatchManager;
    private final @NonNull SessionManager sessionManager;
    private final long requestTimeoutMs;
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

    AsyncOxiaClientImpl(
            @NonNull String clientIdentifier,
            @NonNull ScheduledExecutorService scheduledExecutor,
            @NonNull InstrumentProvider instrumentProvider,
            @NonNull OxiaStubManager stubManager,
            @NonNull ShardManager shardManager,
            @NonNull NotificationManager notificationManager,
            @NonNull BatchManager readBatchManager,
            @NonNull BatchManager writeBatchManager,
            @NonNull SessionManager sessionManager,
            Duration requestTimeout) {
        this.clientIdentifier = clientIdentifier;
        this.instrumentProvider = instrumentProvider;
        this.stubManager = stubManager;
        this.shardManager = shardManager;
        this.notificationManager = notificationManager;
        this.readBatchManager = readBatchManager;
        this.writeBatchManager = writeBatchManager;
        this.sessionManager = sessionManager;
        this.scheduledExecutor = scheduledExecutor;
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

        try {
            checkIfClosed();
            Objects.requireNonNull(key);
            Objects.requireNonNull(value);

            callback = internalPut(key, value, options);
        } catch (RuntimeException e) {
            callback = CompletableFuture.failedFuture(e);
        }
        return callback
                .orTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .whenComplete(
                        (putResult, throwable) -> {
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
            writeBatchManager.getBatcher(shardId).add(op);
        } else {
            // The put operation is trying to write an ephemeral record. We need to have a valid session
            // id for this
            sessionManager
                    .getSession(shardId)
                    .thenAccept(
                            session -> {
                                var op =
                                        new PutOperation(
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
                                writeBatchManager.getBatcher(shardId).add(op);
                            })
                    .exceptionally(
                            ex -> {
                                future.completeExceptionally(ex);
                                return null;
                            });
        }

        return future.orTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS);
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
        try {
            checkIfClosed();
            Objects.requireNonNull(key);

            OptionalLong versionId = OptionsUtils.getVersionId(options);
            var partitionKey = OptionsUtils.getPartitionKey(options);
            var shardId = shardManager.getShardForKey(partitionKey.orElse(key));
            writeBatchManager.getBatcher(shardId).add(new DeleteOperation(callback, key, versionId));
        } catch (RuntimeException e) {
            callback.completeExceptionally(e);
        }
        return callback
                .orTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .whenComplete(
                        (putResult, throwable) -> {
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
        try {
            checkIfClosed();
            Objects.requireNonNull(startKeyInclusive);
            Objects.requireNonNull(endKeyExclusive);

            var partitionKey = OptionsUtils.getPartitionKey(options);
            if (partitionKey.isPresent()) {
                // When partition key is present, we only need to send the request to a single shard
                var shardId = shardManager.getShardForKey(partitionKey.get());
                callback = new CompletableFuture<>();
                writeBatchManager
                        .getBatcher(shardId)
                        .add(new DeleteRangeOperation(callback, startKeyInclusive, endKeyExclusive));
            } else {
                // Perform the delete range on all the shards
                var shardDeletes =
                        shardManager.allShardIds().stream()
                                .map(writeBatchManager::getBatcher)
                                .map(
                                        b -> {
                                            var shardCallback = new CompletableFuture<Void>();
                                            b.add(
                                                    new DeleteRangeOperation(
                                                            shardCallback, startKeyInclusive, endKeyExclusive));
                                            return shardCallback;
                                        })
                                .toArray(CompletableFuture[]::new);
                callback = CompletableFuture.allOf(shardDeletes);
            }
        } catch (RuntimeException e) {
            callback = CompletableFuture.failedFuture(e);
        }
        return callback
                .orTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .whenComplete(
                        (putResult, throwable) -> {
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
        try {
            checkIfClosed();
            Objects.requireNonNull(key);
            internalGet(key, internalOptions, callback);
        } catch (RuntimeException e) {
            callback.completeExceptionally(e);
        }
        return callback
                .orTimeout(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .whenComplete(
                        (getResult, throwable) -> {
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
            readBatchManager.getBatcher(shardId).add(new GetOperation(result, key, options));
        }
    }

    private void internalGetMultiShards(
            String key, GetOptions options, CompletableFuture<GetResult> result) {
        // We need check on all the shards for a floor/ceiling query
        List<CompletableFuture<GetResult>> futures = new ArrayList<>();
        for (long shardId : shardManager.allShardIds()) {
            CompletableFuture<GetResult> f = new CompletableFuture<>();
            readBatchManager.getBatcher(shardId).add(new GetOperation(f, key, options));
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
                                            case UNRECOGNIZED -> null;
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
        var leader = shardManager.leader(shardId);
        var stub = stubManager.getStub(leader);
        var requestBuilder =
                ListRequest.newBuilder()
                        .setShard(shardId)
                        .setStartInclusive(startKeyInclusive)
                        .setEndExclusive(endKeyExclusive);
        secondaryIndexName.ifPresent(requestBuilder::setSecondaryIndexName);
        var request = requestBuilder.build();

        CompletableFuture<List<String>> future = new CompletableFuture<>();
        List<String> result = new ArrayList<>();
        stub.async()
                .list(
                        request,
                        new StreamObserver<ListResponse>() {
                            @Override
                            public void onNext(ListResponse response) {
                                for (int i = 0; i < response.getKeysCount(); i++) {
                                    result.add(response.getKeys(i));
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                                future.completeExceptionally(t);
                            }

                            @Override
                            public void onCompleted() {
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
                this.stubManager,
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
                    public void onNext(GetResult result) {
                        totalSize.addAndGet(result.value().length);
                        consumer.onNext(result);
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

            Optional<String> partitionKey = OptionsUtils.getPartitionKey(options);
            Optional<String> secondaryIndexName = OptionsUtils.getSecondaryIndexName(options);
            if (partitionKey.isPresent()) {
                long shardId = shardManager.getShardForKey(partitionKey.get());
                internalShardRangeScan(
                        shardId, startKeyInclusive, endKeyExclusive, secondaryIndexName, timedConsumer);
            } else {
                internalRangeScanMultiShards(
                        startKeyInclusive, endKeyExclusive, secondaryIndexName, timedConsumer);
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
            RangeScanConsumer consumer) {
        var leader = shardManager.leader(shardId);
        var stub = stubManager.getStub(leader);
        var requestBuilder =
                RangeScanRequest.newBuilder()
                        .setShard(shardId)
                        .setStartInclusive(startKeyInclusive)
                        .setEndExclusive(endKeyExclusive);

        secondaryIndexName.ifPresent(requestBuilder::setSecondaryIndexName);
        var request = requestBuilder.build();

        stub.async()
                .rangeScan(
                        request,
                        new StreamObserver<>() {
                            @Override
                            public void onNext(RangeScanResponse response) {
                                for (int i = 0; i < response.getRecordsCount(); i++) {
                                    consumer.onNext(ProtoUtil.getResultFromProto("", response.getRecords(i)));
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                                consumer.onError(t);
                            }

                            @Override
                            public void onCompleted() {
                                consumer.onCompleted();
                            }
                        });
    }

    private void internalRangeScanMultiShards(
            String startKeyInclusive,
            String endKeyExclusive,
            Optional<String> secondaryIndexName,
            RangeScanConsumer consumer) {
        final Set<Long> shardIds = shardManager.allShardIds();
        final RangeScanConsumer multiShardConsumer =
                new SharedRangeScanConsumer(shardIds.size(), consumer);
        for (long shardId : shardIds) {
            internalShardRangeScan(
                    shardId, startKeyInclusive, endKeyExclusive, secondaryIndexName, multiShardConsumer);
        }
    }

    static class SharedRangeScanConsumer implements RangeScanConsumer {
        private final RangeScanConsumer delegate;

        private int pendingCompletedRequests;
        private boolean completed = false;
        private Throwable completedException = null;

        SharedRangeScanConsumer(int shards, RangeScanConsumer delegate) {
            this.pendingCompletedRequests = shards;
            this.delegate = delegate;
        }

        @Override
        public synchronized void onNext(GetResult result) {
            if (completed) {
                return;
            }
            delegate.onNext(result);
        }

        @Override
        public synchronized void onError(Throwable throwable) {
            if (completedException == null) {
                completedException = throwable;
            } else {
                completedException.addSuppressed(throwable);
            }
            if (completed) {
                return;
            }
            completed = true;
            delegate.onError(throwable);
        }

        @Override
        public synchronized void onCompleted() {
            if (completed) {
                return;
            }
            pendingCompletedRequests -= 1;
            if (pendingCompletedRequests == 0) {
                completed = true;
                delegate.onCompleted();
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        readBatchManager.close();
        writeBatchManager.close();
        sessionManager.close();
        notificationManager.close();
        shardManager.close();
        stubManager.close();
        scheduledExecutor.shutdownNow();
    }

    private void checkIfClosed() {
        if (closed) {
            throw new IllegalStateException("Client has been closed");
        }
    }
}
