/*
 * Copyright © 2022-2025 The Oxia Authors
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

import io.grpc.stub.StreamObserver;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class WriteStreamWrapper implements StreamObserver<WriteResponse> {

    private final StreamObserver<WriteRequest> clientStream;
    private final Deque<CompletableFuture<WriteResponse>> pendingWrites;

    private volatile boolean completed;
    private volatile Throwable completedException;

    public WriteStreamWrapper(OxiaClientGrpc.OxiaClientStub stub) {
        this.pendingWrites = new ArrayDeque<>();
        this.completed = false;
        this.completedException = null;
        this.clientStream = stub.writeStream(this);
    }

    public boolean isValid() {
        return !completed;
    }

    @Override
    public void onNext(WriteResponse value) {
        synchronized (WriteStreamWrapper.this) {
            final var future = pendingWrites.poll();
            if (future != null) {
                future.complete(value);
            }
        }
    }

    @Override
    public void onError(Throwable error) {
        synchronized (WriteStreamWrapper.this) {
            completedException = error;
            completed = true;
            if (!pendingWrites.isEmpty()) {
                log.warn(
                        "Receive error when writing data to server through the stream, prepare to fail pending requests. pendingWrites={} {}",
                        pendingWrites.size(),
                        completedException.getMessage());
            }
            pendingWrites.forEach(f -> f.completeExceptionally(completedException));
            pendingWrites.clear();
        }
    }

    @Override
    public void onCompleted() {
        synchronized (WriteStreamWrapper.this) {
            completed = true;
            if (!pendingWrites.isEmpty()) {
                log.info(
                        "Receive stream close signal when writing data to server through the stream, prepare to cancel pending requests. pendingWrites={}",
                        pendingWrites.size());
            }
            pendingWrites.forEach(f -> f.completeExceptionally(new CancellationException()));
            pendingWrites.clear();
        }
    }

    public CompletableFuture<WriteResponse> send(WriteRequest request) {
        if (completed) {
            return CompletableFuture.failedFuture(
                    Optional.ofNullable(completedException).orElseGet(CancellationException::new));
        }
        synchronized (WriteStreamWrapper.this) {
            if (completed) {
                return CompletableFuture.failedFuture(
                        Optional.ofNullable(completedException).orElseGet(CancellationException::new));
            }
            final CompletableFuture<WriteResponse> future = new CompletableFuture<>();
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Sending request {}", request);
                }
                clientStream.onNext(request);
                pendingWrites.add(future);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
            return future;
        }
    }
}
