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
import io.oxia.client.util.Backoff;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public final class ManagedWriteStream implements AutoCloseable {
    private final Logger log;

    record InflightWrite(
            Supplier<WriteRequest> requestSupplier, CompletableFuture<WriteResponse> future) {}

    private final long shardId;
    private final RpcProvider rpcProvider;
    private final ScheduledExecutorService asyncExecutor;
    private final Backoff backoff;

    private final ReentrantLock lock;
    private boolean closed;
    private final Deque<InflightWrite> inflightWrites;
    private ManagedSubWriteStream subStreamObserver;

    public ManagedWriteStream(
            long shardId, RpcProvider rpcProvider, ScheduledExecutorService asyncExecutor) {
        this.shardId = shardId;
        this.log = Logger.get(ManagedWriteStream.class).with().attr("shard", shardId).build();
        this.inflightWrites = new ArrayDeque<>();
        this.rpcProvider = rpcProvider;
        this.lock = new ReentrantLock();
        this.asyncExecutor = asyncExecutor;
        this.backoff = new Backoff();
        this.closed = false;
    }

    void handleResponse(ManagedSubWriteStream source, WriteResponse value) {
        final InflightWrite inflight;
        final int pendingWrites;
        lock.lock();
        try {
            if (source != subStreamObserver) {
                log.debug("Ignoring write response from inactive stream");
                return;
            }
            inflight = inflightWrites.pollFirst();
            pendingWrites = inflightWrites.size();
        } finally {
            lock.unlock();
        }

        if (inflight != null) {
            log.debug(event -> event.attr("pendingWrites", pendingWrites).log("Received write response"));
            inflight.future.complete(value);
        } else {
            log.warn()
                    .attr("pendingWrites", pendingWrites)
                    .log("Received write response with no inflight write");
        }
    }

    void handleError(ManagedSubWriteStream source, Throwable t) {
        final boolean shouldRetry;
        final OxiaStatusException maybeLeaderHint;
        final Runnable deferFail;
        lock.lock();
        try {
            if (source != subStreamObserver) {
                log.debug().exceptionMessage(t).log("Ignoring error from inactive write stream");
                return;
            }
            log.warn()
                    .exceptionMessage(t)
                    .attr("pendingWrites", inflightWrites.size())
                    .attr("observerPresent", subStreamObserver != null)
                    .attr("closed", closed)
                    .log("Write stream received error");
            final OxiaStatusException oxiaStatusException = OxiaStatusException.from(t);
            maybeLeaderHint = oxiaStatusException;
            subStreamObserver = null;
            deferFail = failHeadInflightIfNonRetryable(oxiaStatusException);
            shouldRetry = !inflightWrites.isEmpty();
        } finally {
            lock.unlock();
        }
        deferFail.run(); // call it without lock
        if (shouldRetry) {
            scheduleRetry(maybeLeaderHint, 0); // retry immediately
        }
    }

    void handleCompleted(ManagedSubWriteStream source) {
        boolean shouldRetry;
        lock.lock();
        try {
            if (source != subStreamObserver) {
                log.debug("Ignoring completion from inactive write stream");
                return;
            }
            log.info()
                    .attr("pendingWrites", inflightWrites.size())
                    .attr("observerPresent", subStreamObserver != null)
                    .attr("closed", closed)
                    .log("Write stream completed");
            subStreamObserver = null;
            shouldRetry = !inflightWrites.isEmpty();
        } finally {
            lock.unlock();
        }
        if (shouldRetry) {
            scheduleRetry(null, 0); // retry immediately
        }
    }

    public CompletableFuture<WriteResponse> send(Supplier<WriteRequest> requestSupplier) {
        lock.lock();
        final CompletableFuture<WriteResponse> future = new CompletableFuture<>();
        final InflightWrite inflightWrite = new InflightWrite(requestSupplier, future);
        try {
            log.debug(
                    event ->
                            event
                                    .attr("pendingWritesBefore", inflightWrites.size())
                                    .attr("observerPresent", subStreamObserver != null)
                                    .attr("closed", closed)
                                    .log("Sending write request"));
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("Stream is closed"));
            }
            inflightWrites.addLast(inflightWrite);
            log.debug(
                    event ->
                            event
                                    .attr("pendingWritesAfterQueue", inflightWrites.size())
                                    .attr("observerPresent", subStreamObserver != null)
                                    .log("Queued write request"));
            try {
                if (subStreamObserver == null) {
                    initWithRecovery(null);
                } else {
                    subStreamObserver.send(inflightWrite.requestSupplier.get());
                    log.debug(
                            event ->
                                    event
                                            .attr("pendingWrites", inflightWrites.size())
                                            .log("Sent write request on existing stream"));
                }
            } catch (Throwable ex) {
                log.warn().exceptionMessage(ex).log("Failed to send write request, retrying");
                subStreamObserver = null;
                // we are using null here to avoid the new request exception discard old.
                scheduleRetry(null, 0);
            }
            return future;
        } finally {
            lock.unlock();
        }
    }

    private void scheduleRetry(OxiaStatusException maybeLeaderHint, long backoffMills) {
        log.info()
                .exceptionMessage(maybeLeaderHint)
                .attr("retryDelay", Duration.ofMillis(backoffMills).toString())
                .attr("pendingWrites", inflightWrites.size())
                .log("Scheduling write stream recovery");
        asyncExecutor.schedule(
                () -> {
                    Runnable deferFail = () -> {};
                    OxiaStatusException oxiaStatusException = null;
                    boolean shouldRetry = false;
                    lock.lock();
                    try {
                        if (closed) {
                            log.info("Skipping write stream recovery after close");
                            return;
                        }
                        if (subStreamObserver != null) {
                            log.debug(
                                    event ->
                                            event
                                                    .attr("pendingWrites", inflightWrites.size())
                                                    .log("Skipping write stream recovery because observer is present"));
                            return;
                        }
                        initWithRecovery(maybeLeaderHint);
                    } catch (Throwable exception) {
                        log.error().exceptionMessage(exception).log("Failed to recover write stream");
                        oxiaStatusException = OxiaStatusException.from(exception);
                        subStreamObserver = null;
                        deferFail = failHeadInflightIfNonRetryable(oxiaStatusException);
                        shouldRetry = !inflightWrites.isEmpty();
                    } finally {
                        lock.unlock();
                        deferFail.run();
                        if (shouldRetry) {
                            scheduleRetry(oxiaStatusException, backoff.nextDelayMillis());
                        }
                    }
                },
                backoffMills,
                TimeUnit.MILLISECONDS);
    }

    private Runnable failHeadInflightIfNonRetryable(OxiaStatusException oxiaStatusException) {
        if (!oxiaStatusException.isRetryable()) {
            final InflightWrite inflightWrite = inflightWrites.pollFirst();
            if (inflightWrite != null) {
                return () -> {
                    try {
                        inflightWrite.future.completeExceptionally(oxiaStatusException);
                    } catch (Throwable ex) {
                        log.warn().exceptionMessage(ex).log("Failed to complete non-retryable inflight write");
                    }
                };
            }
        }
        return () -> {};
    }

    private void initWithRecovery(OxiaStatusException leaderHint) {
        subStreamObserver = new ManagedSubWriteStream(this, rpcProvider, shardId, leaderHint);
        log.info().attr("pendingWrites", inflightWrites.size()).log("Opened write stream");
        log.debug(
                event ->
                        event
                                .attr("pendingWrites", inflightWrites.size())
                                .attr("leaderHint", leaderHint)
                                .log("Replaying inflight writes on opened stream"));
        for (InflightWrite inflightWrite : inflightWrites) {
            subStreamObserver.send(inflightWrite.requestSupplier.get());
        }
        log.debug(
                event ->
                        event.attr("pendingWrites", inflightWrites.size()).log("Replayed inflight writes"));
        backoff.reset();
    }

    @Override
    public void close() {
        var inflightsToFail = new ArrayList<InflightWrite>();
        Throwable closeError = null;
        lock.lock();
        try {
            closed = true;
            if (subStreamObserver != null) {
                subStreamObserver.complete();
                subStreamObserver = null;
            }
            log.info().attr("pendingWrites", inflightWrites.size()).log("Closing write stream");
            inflightsToFail.addAll(inflightWrites);
            inflightWrites.clear();
        } catch (Throwable ex) {
            closeError = ex;
            inflightsToFail.addAll(inflightWrites);
            inflightWrites.clear();
        } finally {
            lock.unlock();
        }
        if (closeError != null) {
            final var error = closeError;
            inflightsToFail.forEach(inflight -> inflight.future.completeExceptionally(error));
            return;
        }
        inflightsToFail.forEach(
                inflight ->
                        inflight.future.completeExceptionally(new CancellationException("Stream is closed")));
    }
}
