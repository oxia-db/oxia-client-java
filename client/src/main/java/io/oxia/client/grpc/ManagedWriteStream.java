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

import io.github.merlimat.slog.Logger;
import io.grpc.stub.StreamObserver;
import io.oxia.client.util.Backoff;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public final class ManagedWriteStream implements AutoCloseable, StreamObserver<WriteResponse> {
    private final Logger log;

    record InflightWrite(WriteRequest request, CompletableFuture<WriteResponse> future) {}

    private final long shardId;
    private final RpcProvider rpcProvider;
    private final ScheduledExecutorService asyncExecutor;
    private final Backoff backoff;

    private final ReentrantLock lock;
    private boolean closed;
    private final Queue<InflightWrite> inflightWrites;
    private StreamObserver<WriteRequest> subStreamObserver;

    public ManagedWriteStream(
            long shardId, RpcProvider rpcProvider, ScheduledExecutorService asyncExecutor) {
        this.shardId = shardId;
        this.log = Logger.get(ManagedWriteStream.class).with().attr("shard", shardId).build();
        this.inflightWrites = new ConcurrentLinkedQueue<>();
        this.rpcProvider = rpcProvider;
        this.lock = new ReentrantLock();
        this.asyncExecutor = asyncExecutor;
        this.backoff = new Backoff();
        this.closed = false;
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
        resetSubObserver();
        if (inflightWrites.isEmpty()) {
            return;
        }
        scheduleRetry(t, 0); // retry immediately
    }

    @Override
    public void onCompleted() {
        resetSubObserver();
        if (inflightWrites.isEmpty()) {
            return;
        }
        scheduleRetry(null, 0); // retry immediately
    }

    public CompletableFuture<WriteResponse> send(WriteRequest request) {
        lock.lock();
        final CompletableFuture<WriteResponse> future = new CompletableFuture<>();
        final InflightWrite inflightWrite = new InflightWrite(request, future);
        try {
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("Stream is closed"));
            }
            inflightWrites.add(inflightWrite);
            try {
                if (subStreamObserver == null) {
                    initWithRecovery(null);
                } else {
                    subStreamObserver.onNext(inflightWrite.request);
                }
            } catch (Throwable ex) {
                log.warn().exceptionMessage(ex)
                        .log("Failed to send write request, retrying");
                subStreamObserver = null;
                // we are using null here to avoid the new request exception discard old.
                scheduleRetry(null, 0);
            }
            return future;
        } finally {
            lock.unlock();
        }
    }

    private void scheduleRetry(Throwable error, long backoffMills) {
        OxiaStatusException leaderHint = null;
        if (error != null) {
            final var translated = OxiaStatusException.toException(error);
            if (OxiaStatusException.isRetryable(translated)) {
                leaderHint = translated instanceof OxiaStatusException oxiaError ? oxiaError : null;
            } else {
                final InflightWrite inflight = inflightWrites.poll();
                if (inflight != null) {
                    inflight.future.completeExceptionally(translated);
                }
            }
        }
        log.info()
                .exceptionMessage(error)
                .attr("retryDelayMillis", backoffMills)
                .attr("pendingWrites", inflightWrites.size())
                .log("Scheduling write stream recovery");
        final OxiaStatusException fLeaderHint = leaderHint;
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
                        initWithRecovery(fLeaderHint);
                    } catch (Throwable ex) {
                        log.error().exceptionMessage(ex).log("Failed to recover write stream");
                        scheduleRetry(ex, backoff.nextDelayMillis());
                    } finally {
                        lock.unlock();
                    }
                },
                backoffMills,
                TimeUnit.MILLISECONDS);
    }

    private void resetSubObserver() {
        lock.lock();
        try {
            subStreamObserver = null;
        } finally {
            lock.unlock();
        }
    }

    private void initWithRecovery(OxiaStatusException leaderHint) {
        subStreamObserver = rpcProvider.writeStream(shardId, leaderHint, this);
        log.info().attr("pendingWrites", inflightWrites.size()).log("Opened write stream");
        for (InflightWrite inflightWrite : inflightWrites) {
            subStreamObserver.onNext(inflightWrite.request);
        }
        backoff.reset();
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
