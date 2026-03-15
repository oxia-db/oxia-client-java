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
package io.oxia.client.batch;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.StreamObserver;
import io.oxia.client.grpc.OxiaStatus;
import io.oxia.client.grpc.OxiaStubProvider;
import io.oxia.client.util.Backoff;
import io.oxia.proto.ReadRequest;
import io.oxia.proto.ReadResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class ReadBatch extends BatchBase implements Batch {

    private final ReadBatchFactory factory;

    @VisibleForTesting final List<Operation.ReadOperation.GetOperation> gets = new ArrayList<>();

    private final Duration requestTimeout;
    long startSendTimeNanos;

    ReadBatch(
            ReadBatchFactory factory,
            OxiaStubProvider stubProvider,
            long shardId,
            @NonNull Duration requestTimeout) {
        super(stubProvider, shardId);
        this.factory = factory;
        this.requestTimeout = requestTimeout;
    }

    @Override
    public boolean canAdd(@NonNull Operation<?> operation) {
        return true;
    }

    public void add(@NonNull Operation<?> operation) {
        if (operation instanceof Operation.ReadOperation.GetOperation g) {
            gets.add(g);
        }
    }

    @Override
    public int size() {
        return gets.size();
    }

    @Override
    public void send() {
        startSendTimeNanos = System.nanoTime();
        ReadRequest request = toProto();
        long deadlineNanos = System.nanoTime() + requestTimeout.toNanos();

        doRequestWithRetries(request, deadlineNanos, new Backoff())
                .thenAccept(
                        response -> {
                            factory
                                    .getReadRequestLatencyHistogram()
                                    .recordSuccess(System.nanoTime() - startSendTimeNanos);

                            for (int i = 0; i < response.getGetsCount(); i++) {
                                gets.get(i).complete(response.getGets(i));
                            }
                        })
                .exceptionally(
                        ex -> {
                            onError(ex);
                            return null;
                        });
    }

    CompletableFuture<ReadResponse> doRequestWithRetries(
            ReadRequest request, long deadlineNanos, Backoff backoff) {
        return doRequest(request)
                .exceptionallyCompose(
                        ex -> {
                            if (!OxiaStatus.isRetriable(ex)) {
                                return CompletableFuture.failedFuture(ex);
                            }

                            long remainingMillis =
                                    TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
                            if (remainingMillis <= 0) {
                                return CompletableFuture.failedFuture(ex);
                            }

                            long delayMillis = Math.min(backoff.nextDelayMillis(), remainingMillis);
                            log.warn(
                                    "Read request failed, retrying. shard={} retry-after={}ms error={}",
                                    getShardId(),
                                    delayMillis,
                                    ex.getMessage());

                            Executor delayed =
                                    CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS);
                            return CompletableFuture.supplyAsync(() -> null, delayed)
                                    .thenCompose(__ -> doRequestWithRetries(request, deadlineNanos, backoff));
                        });
    }

    private CompletableFuture<ReadResponse> doRequest(ReadRequest request) {
        CompletableFuture<ReadResponse> future = new CompletableFuture<>();
        try {
            getStub()
                    .async()
                    .read(
                            request,
                            new StreamObserver<>() {
                                ReadResponse result;

                                @Override
                                public void onNext(ReadResponse response) {
                                    result = response;
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
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    public void onError(Throwable batchError) {
        factory.getReadRequestLatencyHistogram().recordFailure(System.nanoTime() - startSendTimeNanos);
        gets.forEach(g -> g.fail(batchError));
    }

    @NonNull
    ReadRequest toProto() {
        return ReadRequest.newBuilder()
                .setShard(getShardId())
                .addAllGets(gets.stream().map(Operation.ReadOperation.GetOperation::toProto).toList())
                .build();
    }
}
