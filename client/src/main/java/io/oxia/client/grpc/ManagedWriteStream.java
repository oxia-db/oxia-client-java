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
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongFunction;

final class ManagedWriteStream implements AutoCloseable {
    private static final Metadata.Key<String> NAMESPACE_KEY =
            Metadata.Key.of("namespace", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> SHARD_ID_KEY =
            Metadata.Key.of("shard-id", Metadata.ASCII_STRING_MARSHALLER);

    private final Map<Long, ShardWriteStream> streams = new ConcurrentHashMap<>();
    private final String namespace;
    private final LongFunction<OxiaClientGrpc.OxiaClientStub> stubProvider;

    ManagedWriteStream(String namespace, LongFunction<OxiaClientGrpc.OxiaClientStub> stubProvider) {
        this.namespace = namespace;
        this.stubProvider = stubProvider;
    }

    CompletableFuture<WriteResponse> write(WriteRequest request) {
        return stream(request.getShard()).send(request);
    }

    private ShardWriteStream stream(long shardId) {
        ShardWriteStream stream = null;
        for (int i = 0; i < 2; i++) {
            stream = streams.get(shardId);
            if (stream == null) {
                stream = streams.computeIfAbsent(shardId, this::newStream);
            }
            if (stream.isValid()) {
                break;
            }
            streams.remove(shardId, stream);
        }
        return stream;
    }

    private ShardWriteStream newStream(long shardId) {
        Metadata headers = new Metadata();
        headers.put(NAMESPACE_KEY, namespace);
        headers.put(SHARD_ID_KEY, Long.toString(shardId));
        return new ShardWriteStream(
                stubProvider
                        .apply(shardId)
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers)),
                shardId);
    }

    @Override
    public void close() {
        streams.values().forEach(ShardWriteStream::close);
        streams.clear();
    }

    private static final class ShardWriteStream implements StreamObserver<WriteResponse> {
        private final Logger log;
        private final StreamObserver<WriteRequest> requestStream;
        private final Deque<CompletableFuture<WriteResponse>> pendingWrites = new ArrayDeque<>();

        private volatile boolean completed;
        private volatile Throwable completedException;

        ShardWriteStream(OxiaClientGrpc.OxiaClientStub stub, long shardId) {
            this.log = Logger.get(ManagedWriteStream.class).with().attr("shard", shardId).build();
            this.requestStream = stub.writeStream(this);
        }

        boolean isValid() {
            return !completed;
        }

        @Override
        public void onNext(WriteResponse value) {
            synchronized (this) {
                final var future = pendingWrites.poll();
                if (future != null) {
                    future.complete(value);
                }
            }
        }

        @Override
        public void onError(Throwable error) {
            synchronized (this) {
                completedException = error;
                completed = true;
                if (!pendingWrites.isEmpty()) {
                    log.warn()
                            .attr("pendingWrites", pendingWrites.size())
                            .exceptionMessage(completedException)
                            .log(
                                    "Receive error when writing data to server through the stream, prepare to fail pending requests");
                }
                pendingWrites.forEach(f -> f.completeExceptionally(completedException));
                pendingWrites.clear();
            }
        }

        @Override
        public void onCompleted() {
            synchronized (this) {
                completed = true;
                if (!pendingWrites.isEmpty()) {
                    log.info()
                            .attr("pendingWrites", pendingWrites.size())
                            .log(
                                    "Receive stream close signal when writing data to server through the stream, prepare to cancel pending requests");
                }
                pendingWrites.forEach(f -> f.completeExceptionally(new CancellationException()));
                pendingWrites.clear();
            }
        }

        CompletableFuture<WriteResponse> send(WriteRequest request) {
            if (completed) {
                return CompletableFuture.failedFuture(
                        Optional.ofNullable(completedException).orElseGet(CancellationException::new));
            }
            synchronized (this) {
                if (completed) {
                    return CompletableFuture.failedFuture(
                            Optional.ofNullable(completedException).orElseGet(CancellationException::new));
                }
                final CompletableFuture<WriteResponse> future = new CompletableFuture<>();
                pendingWrites.add(future);
                try {
                    log.debug().attr("request", request).log("Sending request");
                    requestStream.onNext(request);
                } catch (Exception e) {
                    pendingWrites.remove(future);
                    future.completeExceptionally(e);
                }
                return future;
            }
        }

        void close() {
            synchronized (this) {
                if (completed) {
                    return;
                }
                completed = true;
                pendingWrites.forEach(f -> f.completeExceptionally(new CancellationException()));
                pendingWrites.clear();
            }
            requestStream.onCompleted();
        }
    }
}
