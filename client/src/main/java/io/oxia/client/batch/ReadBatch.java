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
import io.oxia.proto.LeaderHint;
import io.oxia.proto.ReadRequest;
import io.oxia.proto.ReadResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
            Duration requestTimeout) {
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

        try {
            ReadResponse response = doRequestWithRetries(request);
            factory
                    .getReadRequestLatencyHistogram()
                    .recordSuccess(System.nanoTime() - startSendTimeNanos);
            handle(response);
        } catch (Throwable t) {
            onError(t);
        }
    }

    ReadResponse doRequestWithRetries(ReadRequest request) throws Exception {
        long deadlineNanos = System.nanoTime() + requestTimeout.toNanos();
        Backoff backoff = new Backoff();
        LeaderHint hint = null;

        while (true) {
            try {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new TimeoutException("Request timed out after " + requestTimeout);
                }
                return doRequest(request, hint, remainingNanos);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (!OxiaStatus.isRetriable(cause)) {
                    throw e;
                }
                LeaderHint leaderHint = OxiaStatus.findLeaderHint(cause);
                if (leaderHint != null) {
                    hint = leaderHint;
                }

                long delayMillis = backoff.nextDelayMillis();
                long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
                if (remainingMillis <= 0) {
                    throw e;
                }

                log.warn(
                        "Failed to perform read request, retrying later. shard={} retry-after={}ms error={}",
                        getShardId(),
                        Math.min(delayMillis, remainingMillis),
                        cause.getMessage());

                Thread.sleep(Math.min(delayMillis, remainingMillis));
            } catch (TimeoutException e) {
                throw e;
            }
        }
    }

    private ReadResponse doRequest(ReadRequest request, LeaderHint hint, long remainingNanos)
            throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<ReadResponse> future = new CompletableFuture<>();
        ReadResponse accumulated = ReadResponse.newBuilder().build();

        getStub(hint)
                .async()
                .read(
                        request,
                        new StreamObserver<>() {
                            private ReadResponse.Builder builder = ReadResponse.newBuilder();

                            @Override
                            public void onNext(ReadResponse response) {
                                builder.addAllGets(response.getGetsList());
                            }

                            @Override
                            public void onError(Throwable t) {
                                future.completeExceptionally(t);
                            }

                            @Override
                            public void onCompleted() {
                                future.complete(builder.build());
                            }
                        });

        return future.get(remainingNanos, TimeUnit.NANOSECONDS);
    }

    void onError(Throwable batchError) {
        Throwable error = unwrap(batchError);
        gets.forEach(g -> g.fail(error));
        factory.getReadRequestLatencyHistogram().recordFailure(System.nanoTime() - startSendTimeNanos);
    }

    private static Throwable unwrap(Throwable t) {
        if (t instanceof ExecutionException && t.getCause() != null) {
            return t.getCause();
        }
        return t;
    }

    private void handle(ReadResponse response) {
        for (int i = 0; i < gets.size(); i++) {
            gets.get(i).complete(response.getGets(i));
        }
    }

    @NonNull
    ReadRequest toProto() {
        return ReadRequest.newBuilder()
                .setShard(getShardId())
                .addAllGets(gets.stream().map(Operation.ReadOperation.GetOperation::toProto).toList())
                .build();
    }
}
