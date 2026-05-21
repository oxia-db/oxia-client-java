/*
 * Copyright © 2026 The Oxia Authors
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
package io.oxia.client.grpc;

import static io.oxia.client.constants.Constants.MAXIMUM_FRAME_SIZE;

import io.github.merlimat.slog.Logger;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.oxia.client.util.Backoff;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongFunction;

public final class ManagedWriteStream implements AutoCloseable, StreamObserver<WriteResponse> {
    private static final Metadata.Key<String> NAMESPACE_KEY =
            Metadata.Key.of("namespace", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> SHARD_ID_KEY =
            Metadata.Key.of("shard-id", Metadata.ASCII_STRING_MARSHALLER);

    private final Logger log;

    record InflightWrite(WriteRequest request, CompletableFuture<WriteResponse> future) {}

    private final String namespace;
    private final long shardId;
    private final LongFunction<String> shardLeaderProvider;
    private final ConnectionManager connectionManager;
    private final ScheduledExecutorService asyncExecutor;
    private final Backoff backoff;

    private final ReentrantLock lock;
    private boolean closed;
    private final Queue<InflightWrite> inflightWrites;
    private StreamObserver<WriteRequest> subStreamObserver;

    public ManagedWriteStream(
            String namespace,
            long shardId,
            ConnectionManager connectionManager,
            LongFunction<String> shardLeaderProvider,
            ScheduledExecutorService asyncExecutor) {
        this.namespace = namespace;
        this.shardId = shardId;
        this.log = Logger.get(ManagedWriteStream.class).with().attr("shard", shardId).build();
        this.inflightWrites = new ConcurrentLinkedQueue<>();
        this.shardLeaderProvider = shardLeaderProvider;
        this.connectionManager = connectionManager;
        this.lock = new ReentrantLock();
        this.asyncExecutor = asyncExecutor;
        this.backoff = new Backoff();
        this.closed = false;
    }

    private void resetSubObserver() {
        lock.lock();
        try {
            subStreamObserver = null;
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<WriteResponse> sendWithRecovery(WriteRequest request) {
        lock.lock();
        final CompletableFuture<WriteResponse> future = new CompletableFuture<>();
        final InflightWrite inflightWrite = new InflightWrite(request, future);
        try {
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("Stream is closed"));
            }
            if (request.getSerializedSize() > MAXIMUM_FRAME_SIZE) {
                return CompletableFuture.failedFuture(
                        new OxiaStatusException(
                                OxiaStatusCode.UNKNOWN,
                                Map.of(),
                                "Write request exceeds maximum frame size",
                                new IllegalArgumentException("Write request exceeds maximum frame size")));
            }
            if (subStreamObserver == null) {
                initWithRecovery(null);
            }
            inflightWrites.add(inflightWrite);
            log.debug().attr("pendingWrites", inflightWrites.size()).log("Sending write request");
            subStreamObserver.onNext(request);
            return future;
        } catch (Throwable ex) {
            inflightWrites.remove(inflightWrite);
            future.completeExceptionally(ex);
            return future;
        } finally {
            lock.unlock();
        }
    }

    private void initWithRecovery(OxiaStatusException leaderHint) {
        final var leader =
                leaderHint == null
                        ? shardLeaderProvider.apply(shardId)
                        : leaderHint.getLeaderHint(shardId).orElseGet(() -> shardLeaderProvider.apply(shardId));
        final var headers = new Metadata();
        headers.put(NAMESPACE_KEY, namespace);
        headers.put(SHARD_ID_KEY, Long.toString(shardId));
        subStreamObserver =
                connectionManager
                        .getConnection(leader)
                        .stub()
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
                        .writeStream(this);
        log.info()
                .attr("leader", leader)
                .attr("pendingWrites", inflightWrites.size())
                .log("Opened write stream");

        final var streamObserverRef = subStreamObserver;
        for (InflightWrite inflightWrite : inflightWrites) {
            streamObserverRef.onNext(inflightWrite.request);
        }
        backoff.reset();
    }

    @Override
    public void onNext(WriteResponse value) {
        final InflightWrite inflight = inflightWrites.poll();
        if (inflight != null) {
            log.debug().attr("pendingWrites", inflightWrites.size()).log("Received write response");
            inflight.future.complete(value);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (!OxiaStatusException.isRetryable(t)) {
            final InflightWrite inflight = inflightWrites.poll();
            if (inflight != null) {
                log.warn()
                        .attr("pendingWrites", inflightWrites.size())
                        .exceptionMessage(t)
                        .log("Write stream failed with non-retryable error; failing head write");
                inflight.future.completeExceptionally(OxiaStatusException.toException(t));
            }
        } else {
            log.warn()
                    .attr("pendingWrites", inflightWrites.size())
                    .exceptionMessage(t)
                    .log("Write stream failed with retryable error; scheduling recovery");
        }
        resetSubObserver();
        if (inflightWrites.isEmpty()) {
            return;
        }
        scheduleRetry((OxiaStatusException) OxiaStatusException.toException(t), 0); // retry immediately
    }

    @Override
    public void onCompleted() {
        log.info()
                .attr("pendingWrites", inflightWrites.size())
                .log("Write stream completed; scheduling recovery");
        resetSubObserver();
        if (inflightWrites.isEmpty()) {
            return;
        }
        scheduleRetry(null, 0); // retry immediately
    }

    private void scheduleRetry(OxiaStatusException leaderHint, long backoffMills) {
        log.info()
                .attr("retryDelayMillis", backoffMills)
                .attr("pendingWrites", inflightWrites.size())
                .log("Scheduling write stream recovery");
        asyncExecutor.schedule(
                () -> {
                    lock.lock();
                    try {
                        if (closed) {
                            log.info("Skipping write stream recovery after close");
                            return;
                        }
                        if (subStreamObserver != null) {
                            return;
                        }
                        initWithRecovery(leaderHint);
                    } catch (Throwable ex) {
                        log.error().exceptionMessage(ex).log("Failed to recover write stream");
                        scheduleRetry(null, backoff.nextDelayMillis());
                    } finally {
                        lock.unlock();
                    }
                },
                backoffMills,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        lock.lock();
        try {
            closed = true;
            if (subStreamObserver != null) {
                subStreamObserver.onCompleted();
                subStreamObserver = null;
            }
            log.info().attr("pendingWrites", inflightWrites.size()).log("Closing write stream");
            inflightWrites.forEach(
                    inflight -> {
                        inflight.future.completeExceptionally(new CancellationException("Stream is closed"));
                    });
            inflightWrites.clear();
        } catch (Throwable ex) {
            inflightWrites.forEach(
                    inflight -> {
                        inflight.future.completeExceptionally(ex);
                    });
            inflightWrites.clear();
        } finally {
            lock.unlock();
        }
    }
}
