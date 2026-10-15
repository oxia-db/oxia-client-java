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
import io.oxia.client.util.TimeoutSweeper;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import lombok.NonNull;

public final class ManagedWriteStream implements AutoCloseable {
    private final Logger log;

    private final long shardId;
    private final RpcProvider rpcProvider;
    private final TimeoutSweeper requestTimeouts;

    private final ReentrantLock lock;
    private boolean closed;
    private final Deque<CompletableFuture<WriteResponse>> inflightWrites;
    private ManagedSubWriteStream subStreamObserver;

    public ManagedWriteStream(
            long shardId, RpcProvider rpcProvider, @NonNull TimeoutSweeper requestTimeouts) {
        this.shardId = shardId;
        this.log = Logger.get(ManagedWriteStream.class).with().attr("shard", shardId).build();
        this.inflightWrites = new ArrayDeque<>();
        this.rpcProvider = rpcProvider;
        this.requestTimeouts = requestTimeouts;
        this.lock = new ReentrantLock();
        this.closed = false;
    }

    void handleResponse(ManagedSubWriteStream source, WriteResponse value) {
        final CompletableFuture<WriteResponse> inflight;
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
            inflight.complete(value);
        } else {
            log.warn()
                    .attr("pendingWrites", pendingWrites)
                    .log("Received write response with no inflight write");
        }
    }

    void handleError(ManagedSubWriteStream source, Throwable t) {
        // The writes of a failed stream have an unknown outcome: the server may have applied them
        rolloverSubStream(
                source,
                new OxiaStatusException(
                        OxiaStatusCode.RESOURCE_UNAVAILABLE, Map.of(), "Write stream failed", t));
    }

    void handleCompleted(ManagedSubWriteStream source) {
        rolloverSubStream(source, OxiaStatusException.resourceUnavailable("Write stream completed"));
    }

    public CompletableFuture<WriteResponse> send(WriteRequest request) {
        // Serialize the request before taking the lock, which every response for the shard needs
        final InputStream serializedRequest = ManagedSubWriteStream.serialize(request);
        final CompletableFuture<WriteResponse> future = new CompletableFuture<>();
        ManagedSubWriteStream source = null;
        try {
            lock.lock();
            try {
                if (closed) {
                    return CompletableFuture.failedFuture(
                            OxiaStatusException.resourceUnavailable("Write stream is closed"));
                }
                inflightWrites.addLast(future);
                requestTimeouts
                        .add(future.thenRun(() -> {}))
                        .whenComplete(
                                (__, ex) -> {
                                    if (ex instanceof TimeoutException) {
                                        handleTimeout(future);
                                    }
                                });
                if (subStreamObserver == null) {
                    subStreamObserver = new ManagedSubWriteStream(this, rpcProvider, shardId);
                }
                source = subStreamObserver;
                source.send(serializedRequest);
            } finally {
                lock.unlock();
            }
        } catch (Throwable ex) {
            rolloverSubStream(
                    source,
                    new OxiaStatusException(
                            OxiaStatusCode.RESOURCE_UNAVAILABLE, Map.of(), "Write stream failed", ex));
        }
        return future;
    }

    // A write without response after the request timeout: the stream may be stuck without failing,
    // e.g. behind a proxy whose connection to the server is dead
    private void handleTimeout(CompletableFuture<WriteResponse> timedOut) {
        final ManagedSubWriteStream source;
        lock.lock();
        try {
            if (!inflightWrites.contains(timedOut)) {
                return;
            }
            source = subStreamObserver;
        } finally {
            lock.unlock();
        }
        rolloverSubStream(source, OxiaStatusException.timeout(new TimeoutException()));
    }

    @Override
    public void close() {
        final ManagedSubWriteStream source;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            source = subStreamObserver;
        } finally {
            lock.unlock();
        }
        rolloverSubStream(source, OxiaStatusException.resourceUnavailable("Write stream is closed"));
    }

    // Replaces the sub-stream, unless it was already replaced: the next write opens a new one. Fails
    // the inflight writes instead of sending them again on the new one: the server may have applied
    // them, and sending them again could apply them twice
    private void rolloverSubStream(ManagedSubWriteStream source, OxiaStatusException error) {
        final List<CompletableFuture<WriteResponse>> inflightsToFail;
        lock.lock();
        try {
            if (source != subStreamObserver) {
                return;
            }
            log.info()
                    .exceptionMessage(error)
                    .attr("pendingWrites", inflightWrites.size())
                    .log("Rolling over write stream");
            subStreamObserver = null;
            inflightsToFail = new ArrayList<>(inflightWrites);
            inflightWrites.clear();
        } finally {
            lock.unlock();
        }
        if (source != null) {
            try {
                source.complete();
            } catch (Throwable ex) {
                log.debug().exceptionMessage(ex).log("Failed to complete write stream");
            }
        }
        for (CompletableFuture<WriteResponse> inflight : inflightsToFail) {
            try {
                inflight.completeExceptionally(error);
            } catch (Throwable ex) {
                log.warn().exceptionMessage(ex).log("Failed to complete inflight write");
            }
        }
    }
}
