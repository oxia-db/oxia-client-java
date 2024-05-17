/*
 * Copyright © 2022-2024 StreamNative Inc.
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
package io.streamnative.oxia.client;

import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.api.DeleteRangeOption;
import io.streamnative.oxia.client.api.GetOption;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.ListOption;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.batch.BatchManager;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.streamnative.oxia.client.grpc.OxiaStubManager;
import io.streamnative.oxia.client.grpc.OxiaStubProvider;
import io.streamnative.oxia.client.metrics.Counter;
import io.streamnative.oxia.client.metrics.InstrumentProvider;
import io.streamnative.oxia.client.metrics.LatencyHistogram;
import io.streamnative.oxia.client.metrics.Unit;
import io.streamnative.oxia.client.metrics.UpDownCounter;
import io.streamnative.oxia.client.notify.NotificationManager;
import io.streamnative.oxia.client.session.SessionManager;
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.KeyComparisonType;
import io.streamnative.oxia.proto.ListRequest;
import io.streamnative.oxia.proto.ListResponse;
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
import java.util.function.Consumer;
import lombok.NonNull;

class AsyncOxiaClientImpl implements AsyncOxiaClient {

    static @NonNull CompletableFuture<AsyncOxiaClient> newInstance(@NonNull ClientConfig config) {
        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("oxia-client"));
        var stubManager = new OxiaStubManager();

        var instrumentProvider = new InstrumentProvider(config.openTelemetry(), config.namespace());
        var serviceAddrStub = stubManager.getStub(config.serviceAddress());
        var shardManager =
                new ShardManager(executor, serviceAddrStub, instrumentProvider, config.namespace());
        var notificationManager =
                new NotificationManager(executor, stubManager, shardManager, instrumentProvider);

        OxiaStubProvider stubProvider = new OxiaStubProvider(stubManager, shardManager);

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
                        sessionManager);

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
    private volatile boolean closed;

    private final Counter counterPutBytes;
    private final Counter counterGetBytes;
    private final Counter counterListBytes;

    private final UpDownCounter gaugePendingPutRequests;
    private final UpDownCounter gaugePendingGetRequests;
    private final UpDownCounter gaugePendingListRequests;
    private final UpDownCounter gaugePendingDeleteRequests;
    private final UpDownCounter gaugePendingDeleteRangeRequests;

    private final UpDownCounter gaugePendingPutBytes;

    private final LatencyHistogram histogramPutLatency;
    private final LatencyHistogram histogramGetLatency;
    private final LatencyHistogram histogramDeleteLatency;
    private final LatencyHistogram histogramDeleteRangeLatency;
    private final LatencyHistogram histogramListLatency;

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
            @NonNull SessionManager sessionManager) {
        this.clientIdentifier = clientIdentifier;
        this.instrumentProvider = instrumentProvider;
        this.stubManager = stubManager;
        this.shardManager = shardManager;
        this.notificationManager = notificationManager;
        this.readBatchManager = readBatchManager;
        this.writeBatchManager = writeBatchManager;
        this.sessionManager = sessionManager;
        this.scheduledExecutor = scheduledExecutor;

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
                        "oxia.client.list.size",
                        Unit.Bytes,
                        "Total number of bytes read in list operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "list"));

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
        return callback.whenComplete(
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
                            Optional.empty());
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
                                                Optional.of(clientIdentifier));
                                writeBatchManager.getBatcher(shardId).add(op);
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
        return callback.whenComplete(
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
        return callback.whenComplete(
                (putResult, throwable) -> {
                    gaugePendingDeleteRequests.decrement();
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
        long startTime = System.nanoTime();
        gaugePendingGetRequests.increment();
        var callback = new CompletableFuture<GetResult>();
        try {
            checkIfClosed();
            Objects.requireNonNull(key);
            internalGet(key, options, callback);
        } catch (RuntimeException e) {
            callback.completeExceptionally(e);
        }
        return callback.whenComplete(
                (getResult, throwable) -> {
                    gaugePendingGetRequests.decrement();
                    if (throwable == null) {
                        if (getResult != null) {
                            counterGetBytes.add(getResult.getValue().length);
                        }
                        histogramGetLatency.recordSuccess(System.nanoTime() - startTime);
                    } else {
                        histogramGetLatency.recordFailure(System.nanoTime() - startTime);
                    }
                });
    }

    private void internalGet(
            String key, Set<GetOption> options, CompletableFuture<GetResult> result) {
        KeyComparisonType comparisonType = OptionsUtils.getComparisonType(options);
        Optional<String> partitionKey = OptionsUtils.getPartitionKey(options);
        if (comparisonType == KeyComparisonType.EQUAL || partitionKey.isPresent()) {
            // Single shard get operation
            long shardId = shardManager.getShardForKey(partitionKey.orElse(key));
            readBatchManager.getBatcher(shardId).add(new GetOperation(result, key, comparisonType));
        } else {
            internalGetFloorCeiling(key, comparisonType, result);
        }
    }

    private void internalGetFloorCeiling(
            String key, KeyComparisonType comparisonType, CompletableFuture<GetResult> result) {
        // We need check on all the shards for a floor/ceiling query
        List<CompletableFuture<GetResult>> futures = new ArrayList<>();
        for (long shardId : shardManager.allShardIds()) {
            CompletableFuture<GetResult> f = new CompletableFuture<>();
            readBatchManager.getBatcher(shardId).add(new GetOperation(f, key, comparisonType));
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
                                                .sorted(
                                                        (o1, o2) -> CompareWithSlash.INSTANCE.compare(o1.getKey(), o2.getKey()))
                                                .toList();
                                if (results.isEmpty()) {
                                    result.complete(null);
                                    return;
                                }

                                GetResult gr =
                                        switch (comparisonType) {
                                            case EQUAL,
                                                    UNRECOGNIZED -> null; // This would be handled withing context of single
                                                // shard
                                            case FLOOR, LOWER -> results.get(results.size() - 1);
                                            case CEILING, HIGHER -> results.get(0);
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
            if (partitionKey.isPresent()) {
                long shardId = shardManager.getShardForKey(partitionKey.get());
                callback = internalShardlist(shardId, startKeyInclusive, endKeyExclusive);
            } else {
                callback = internalListMultiShards(startKeyInclusive, endKeyExclusive);
            }
        } catch (Exception e) {
            callback = CompletableFuture.failedFuture(e);
        }
        return callback.whenComplete(
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
            String startKeyInclusive, String endKeyExclusive) {
        List<CompletableFuture<List<String>>> futures = new ArrayList<>();
        for (long shardId : shardManager.allShardIds()) {
            futures.add(internalShardlist(shardId, startKeyInclusive, endKeyExclusive));
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
            long shardId, String startKeyInclusive, String endKeyExclusive) {
        var leader = shardManager.leader(shardId);
        var stub = stubManager.getStub(leader);
        var request =
                ListRequest.newBuilder()
                        .setShardId(shardId)
                        .setStartInclusive(startKeyInclusive)
                        .setEndExclusive(endKeyExclusive)
                        .build();

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
