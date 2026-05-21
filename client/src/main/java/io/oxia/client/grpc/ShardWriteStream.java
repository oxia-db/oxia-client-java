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
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public final class ShardWriteStream implements StreamObserver<WriteResponse> {
    private final Logger log;
    private final StreamObserver<WriteRequest> requestStream;
    private final ReentrantLock streamLock;
    private final AtomicReference<Throwable> terminalException;
    private volatile Queue<CompletableFuture<WriteResponse>> pendingWrites;

    ShardWriteStream(OxiaClientGrpc.OxiaClientStub stub, long shardId) {
        this.log = Logger.get(ShardWriteStream.class).with().attr("shard", shardId).build();
        this.streamLock = new ReentrantLock();
        this.pendingWrites = new ConcurrentLinkedQueue<>();
        this.terminalException = new AtomicReference<>();
        this.requestStream = stub.writeStream(this);
    }

    boolean isValid() {
        return terminalException.get() == null;
    }

    @Override
    public void onNext(WriteResponse value) {
        final var pendingWrites = this.pendingWrites;
        if (pendingWrites == null) {
            return;
        }
        final var future = pendingWrites.poll();
        if (future != null) {
            future.complete(value);
        }
    }

    @Override
    public void onError(Throwable error) {
        completePendingWrites(
                OxiaStatusException.toException(error),
                "Receive error when writing data to server through the stream, prepare to fail pending requests");
    }

    @Override
    public void onCompleted() {
        completePendingWrites(
                new CancellationException(),
                "Receive stream close signal when writing data to server through the stream, prepare to cancel pending requests");
    }

    void close() {
        if (completePendingWrites(
                new CancellationException(),
                "Receive stream close signal when writing data to server through the stream, prepare to cancel pending requests")) {
            requestStream.onCompleted();
        }
    }

    public CompletableFuture<WriteResponse> send(WriteRequest request) {
        streamLock.lock();
        try {
            final var exception = terminalException.get();
            if (exception != null) {
                return CompletableFuture.failedFuture(exception);
            }
            final CompletableFuture<WriteResponse> future = new CompletableFuture<>();
            pendingWrites.add(future);
            try {
                log.debug().attr("request", request).log("Sending request");
                requestStream.onNext(request);
            } catch (Exception e) {
                if (pendingWrites.remove(future)) {
                    future.completeExceptionally(OxiaStatusException.toException(e));
                }
            }
            return future;
        } finally {
            streamLock.unlock();
        }
    }

    private boolean completePendingWrites(Throwable exception, String message) {
        final Queue<CompletableFuture<WriteResponse>> pendingWrites;
        streamLock.lock();
        try {
            if (!terminalException.compareAndSet(null, exception)) {
                return false;
            }
            pendingWrites = this.pendingWrites;
            this.pendingWrites = null;
        } finally {
            streamLock.unlock();
        }
        if (pendingWrites != null && !pendingWrites.isEmpty()) {
            log.warn()
                    .attr("pendingWrites", pendingWrites.size())
                    .exceptionMessage(exception)
                    .log(message);
            pendingWrites.forEach(f -> f.completeExceptionally(exception));
        }
        return true;
    }
}
