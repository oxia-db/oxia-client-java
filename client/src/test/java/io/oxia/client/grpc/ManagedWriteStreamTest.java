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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.util.TimeoutSweeper;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ManagedWriteStreamTest {

    @Test
    void completesWritesInResponseOrder() throws Exception {
        var requests = new ConcurrentLinkedQueue<WriteRequest>();
        Server server = writeServer(respondingWriteService(requests));
        var address = "localhost:" + server.getPort();
        var executor = Executors.newSingleThreadScheduledExecutor();
        var config = clientConfig(address);

        try (var provider = new GrpcRpcProvider(config, executor, shard -> address);
                var stream =
                        new ManagedWriteStream(
                                1, provider, new TimeoutSweeper(executor, config.requestTimeout()))) {
            var first = writeRequest(1);
            var second = writeRequest(2);

            var firstFuture = stream.send(first);
            var secondFuture = stream.send(second);

            firstFuture.get(5, TimeUnit.SECONDS);
            secondFuture.get(5, TimeUnit.SECONDS);

            await().untilAsserted(() -> assertThat(keys(requests)).containsExactly("key-1", "key-2"));
        } finally {
            executor.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void handlesResponseWhileAnotherWriteIsSerialized() throws Exception {
        var requests = new LinkedBlockingQueue<WriteRequest>();
        var responses = new CompletableFuture<StreamObserver<WriteResponse>>();
        Server server =
                writeServer(
                        new OxiaClientGrpc.OxiaClientImplBase() {
                            @Override
                            public StreamObserver<WriteRequest> writeStream(
                                    StreamObserver<WriteResponse> responseObserver) {
                                responses.complete(responseObserver);
                                return new StreamObserver<>() {
                                    @Override
                                    public void onNext(WriteRequest value) {
                                        requests.add(value);
                                    }

                                    @Override
                                    public void onError(Throwable t) {}

                                    @Override
                                    public void onCompleted() {}
                                };
                            }
                        });
        var address = "localhost:" + server.getPort();
        var executor = Executors.newSingleThreadScheduledExecutor();
        var sender = Executors.newSingleThreadExecutor();
        var config = clientConfig(address);
        var value = new BlockingValue("value-2".getBytes(StandardCharsets.UTF_8));

        try (var provider = new GrpcRpcProvider(config, executor, shard -> address);
                var stream =
                        new ManagedWriteStream(
                                1, provider, new TimeoutSweeper(executor, config.requestTimeout()))) {
            var first = stream.send(writeRequest(1));
            assertThat(requests.poll(5, TimeUnit.SECONDS)).isNotNull();

            var request = new WriteRequest().setShard(1);
            request.addPut().setKey("key-2").setValue(value);
            var second = sender.submit(() -> stream.send(request));
            try {
                assertThat(value.serializing.await(5, TimeUnit.SECONDS)).isTrue();
                // The response to the first write doesn't wait for the second one to be serialized
                responses.get().onNext(new WriteResponse());
                first.get(5, TimeUnit.SECONDS);
            } finally {
                value.proceed.countDown();
            }

            var secondRequest = requests.poll(5, TimeUnit.SECONDS);
            assertThat(secondRequest.getPutAt(0).getValue())
                    .isEqualTo("value-2".getBytes(StandardCharsets.UTF_8));
            responses.get().onNext(new WriteResponse());
            second.get(5, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        } finally {
            sender.shutdownNow();
            executor.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void closeFailsPendingWritesAndRejectsNewWrites() throws Exception {
        Server server =
                writeServer(
                        new OxiaClientGrpc.OxiaClientImplBase() {
                            @Override
                            public StreamObserver<WriteRequest> writeStream(
                                    StreamObserver<WriteResponse> responseObserver) {
                                return new StreamObserver<>() {
                                    @Override
                                    public void onNext(WriteRequest value) {}

                                    @Override
                                    public void onError(Throwable t) {}

                                    @Override
                                    public void onCompleted() {}
                                };
                            }
                        });
        var address = "localhost:" + server.getPort();
        var executor = Executors.newSingleThreadScheduledExecutor();
        var config = clientConfig(address);

        try (var provider = new GrpcRpcProvider(config, executor, shard -> address);
                var stream =
                        new ManagedWriteStream(
                                1, provider, new TimeoutSweeper(executor, config.requestTimeout()))) {
            var pending = stream.send(writeRequest(1));

            stream.close();

            assertThat(pending).isCompletedExceptionally();
            assertThatThrownBy(pending::get)
                    .hasCauseInstanceOf(OxiaStatusException.class)
                    .satisfies(
                            error -> {
                                var oxiaError = (OxiaStatusException) error.getCause();
                                assertThat(oxiaError.getStatusCode())
                                        .isEqualTo(OxiaStatusCode.RESOURCE_UNAVAILABLE);
                            });
            assertThat(stream.send(writeRequest(2))).isCompletedExceptionally();
        } finally {
            executor.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void ignoresSynchronousResponseDuringClose() throws Exception {
        var rpcProvider = mock(RpcProvider.class);
        when(rpcProvider.writeStream(anyLong(), any()))
                .thenAnswer(
                        invocation -> {
                            var responseObserver = invocation.<StreamObserver<WriteResponse>>getArgument(1);
                            return new StreamObserver<InputStream>() {
                                @Override
                                public void onNext(InputStream value) {}

                                @Override
                                public void onError(Throwable t) {}

                                @Override
                                public void onCompleted() {
                                    responseObserver.onNext(new WriteResponse());
                                }
                            };
                        });

        var executor = Executors.newSingleThreadScheduledExecutor();
        try (var stream =
                new ManagedWriteStream(
                        1, rpcProvider, new TimeoutSweeper(executor, Duration.ofSeconds(30)))) {
            var pending = stream.send(writeRequest(1));

            stream.close();

            assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(OxiaStatusException.class)
                    .satisfies(
                            error -> {
                                var oxiaError = (OxiaStatusException) error.getCause();
                                assertThat(oxiaError.getStatusCode())
                                        .isEqualTo(OxiaStatusCode.RESOURCE_UNAVAILABLE);
                            });
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void timeoutFailsWritesAndNextWriteOpensNewStream() throws Exception {
        var opens = new AtomicInteger();
        Server server =
                writeServer(
                        new OxiaClientGrpc.OxiaClientImplBase() {
                            @Override
                            public StreamObserver<WriteRequest> writeStream(
                                    StreamObserver<WriteResponse> responseObserver) {
                                opens.incrementAndGet();
                                return new StreamObserver<>() {
                                    @Override
                                    public void onNext(WriteRequest value) {}

                                    @Override
                                    public void onError(Throwable t) {}

                                    @Override
                                    public void onCompleted() {}
                                };
                            }
                        });
        var address = "localhost:" + server.getPort();
        var executor = Executors.newSingleThreadScheduledExecutor();
        var config =
                ((OxiaClientBuilderImpl)
                                OxiaClientBuilder.create(address)
                                        .requestTimeout(Duration.ofMillis(50))
                                        .connectionBackoff(Duration.ofMillis(10), Duration.ofMillis(50)))
                        .getClientConfig();

        try (var provider = new GrpcRpcProvider(config, executor, shard -> address)) {
            var firstStream = provider.getWriteStream(1);
            assertThat(provider.getWriteStream(1)).isSameAs(firstStream);
            var timedOut = firstStream.send(writeRequest(1));

            assertThatThrownBy(() -> timedOut.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(OxiaStatusException.class)
                    .satisfies(
                            error -> {
                                var oxiaError = (OxiaStatusException) error.getCause();
                                assertThat(oxiaError.getStatusCode()).isEqualTo(OxiaStatusCode.TIMEOUT);
                            });

            // The stream stays open, and the next write opens a new sub-stream
            assertThat(provider.getWriteStream(1)).isSameAs(firstStream);
            firstStream.send(writeRequest(2));
            await().untilAsserted(() -> assertThat(opens).hasValue(2));
        } finally {
            executor.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void failsWriteWhenSendFailsAndOpensNewStreamForNextWrite() throws Exception {
        var rpcProvider = mock(RpcProvider.class);
        var openAttempts = new AtomicInteger();
        var requests = new ConcurrentLinkedQueue<WriteRequest>();
        when(rpcProvider.writeStream(anyLong(), any()))
                .thenAnswer(
                        invocation -> {
                            var responseObserver = invocation.<StreamObserver<WriteResponse>>getArgument(1);
                            var attempt = openAttempts.incrementAndGet();
                            return new StreamObserver<InputStream>() {
                                @Override
                                public void onNext(InputStream value) {
                                    requests.add(OxiaClientGrpc.getWriteStreamMethod().parseRequest(value));
                                    if (attempt == 1) {
                                        throw Status.UNAVAILABLE.withDescription("failed send").asRuntimeException();
                                    }
                                    responseObserver.onNext(new WriteResponse());
                                }

                                @Override
                                public void onError(Throwable t) {}

                                @Override
                                public void onCompleted() {}
                            };
                        });

        var executor = Executors.newSingleThreadScheduledExecutor();
        try (var stream =
                new ManagedWriteStream(
                        1, rpcProvider, new TimeoutSweeper(executor, Duration.ofSeconds(30)))) {
            assertThatThrownBy(() -> stream.send(writeRequest(1)).get(5, TimeUnit.SECONDS))
                    .cause()
                    .isInstanceOfSatisfying(
                            OxiaStatusException.class,
                            cause ->
                                    assertThat(cause.getStatusCode()).isEqualTo(OxiaStatusCode.RESOURCE_UNAVAILABLE));

            stream.send(writeRequest(2)).get(5, TimeUnit.SECONDS);
            assertThat(openAttempts).hasValue(2);
            assertThat(keys(requests)).containsExactly("key-1", "key-2");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void ignoresResponsesFromInactiveSubStream() throws Exception {
        var rpcProvider = mock(RpcProvider.class);
        var responseObservers = new ConcurrentLinkedQueue<StreamObserver<WriteResponse>>();
        when(rpcProvider.writeStream(anyLong(), any()))
                .thenAnswer(
                        invocation -> {
                            responseObservers.add(invocation.getArgument(1));
                            return new StreamObserver<InputStream>() {
                                @Override
                                public void onNext(InputStream value) {}

                                @Override
                                public void onError(Throwable t) {}

                                @Override
                                public void onCompleted() {}
                            };
                        });

        var executor = Executors.newSingleThreadScheduledExecutor();
        try (var stream =
                new ManagedWriteStream(
                        1, rpcProvider, new TimeoutSweeper(executor, Duration.ofSeconds(30)))) {
            var first = stream.send(writeRequest(1));
            var inactiveObserver = responseObservers.peek();
            inactiveObserver.onError(notLeaderError());
            assertThatThrownBy(() -> first.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(OxiaStatusException.class);

            var second = stream.send(writeRequest(2));
            assertThat(responseObservers).hasSize(2);
            inactiveObserver.onNext(new WriteResponse());
            assertThat(second).isNotDone();

            responseObservers.stream().skip(1).findFirst().orElseThrow().onNext(new WriteResponse());
            second.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void failsInflightWritesWhenStreamFails() throws Exception {
        var rpcProvider = mock(RpcProvider.class);
        var responseObservers = new LinkedBlockingQueue<StreamObserver<WriteResponse>>();
        var requests = new ConcurrentLinkedQueue<WriteRequest>();
        when(rpcProvider.writeStream(anyLong(), any()))
                .thenAnswer(
                        invocation -> {
                            responseObservers.add(invocation.getArgument(1));
                            return new StreamObserver<InputStream>() {
                                @Override
                                public void onNext(InputStream value) {
                                    requests.add(OxiaClientGrpc.getWriteStreamMethod().parseRequest(value));
                                }

                                @Override
                                public void onError(Throwable t) {}

                                @Override
                                public void onCompleted() {}
                            };
                        });

        var executor = Executors.newSingleThreadScheduledExecutor();
        try (var stream =
                new ManagedWriteStream(
                        1, rpcProvider, new TimeoutSweeper(executor, Duration.ofSeconds(30)))) {
            var first = stream.send(writeRequest(1));
            var second = stream.send(writeRequest(2));
            await().untilAsserted(() -> assertThat(keys(requests)).containsExactly("key-1", "key-2"));

            // The leader may have appended both writes before it failed: sending them again could
            // apply them twice
            var error = notLeaderError();
            responseObservers.poll(5, TimeUnit.SECONDS).onError(error);

            for (var future : java.util.List.of(first, second)) {
                assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                        .cause()
                        .isInstanceOfSatisfying(
                                OxiaStatusException.class,
                                cause -> {
                                    assertThat(cause.getStatusCode()).isEqualTo(OxiaStatusCode.RESOURCE_UNAVAILABLE);
                                    assertThat(cause).hasCause(error);
                                });
            }
            assertThat(responseObservers.poll(200, TimeUnit.MILLISECONDS)).isNull();
            assertThat(keys(requests)).containsExactly("key-1", "key-2");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void failsInflightWritesWhenServerCompletesStream() throws Exception {
        var rpcProvider = mock(RpcProvider.class);
        var responseObservers = new LinkedBlockingQueue<StreamObserver<WriteResponse>>();
        when(rpcProvider.writeStream(anyLong(), any()))
                .thenAnswer(
                        invocation -> {
                            responseObservers.add(invocation.getArgument(1));
                            return new StreamObserver<InputStream>() {
                                @Override
                                public void onNext(InputStream value) {}

                                @Override
                                public void onError(Throwable t) {}

                                @Override
                                public void onCompleted() {}
                            };
                        });

        var executor = Executors.newSingleThreadScheduledExecutor();
        try (var stream =
                new ManagedWriteStream(
                        1, rpcProvider, new TimeoutSweeper(executor, Duration.ofSeconds(30)))) {
            var future = stream.send(writeRequest(1));
            responseObservers.poll(5, TimeUnit.SECONDS).onCompleted();

            assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                    .cause()
                    .isInstanceOfSatisfying(
                            OxiaStatusException.class,
                            cause ->
                                    assertThat(cause.getStatusCode()).isEqualTo(OxiaStatusCode.RESOURCE_UNAVAILABLE));
            assertThat(responseObservers.poll(200, TimeUnit.MILLISECONDS)).isNull();
        } finally {
            executor.shutdownNow();
        }
    }

    private static OxiaClientGrpc.OxiaClientImplBase respondingWriteService(
            Queue<WriteRequest> requests) {
        return new OxiaClientGrpc.OxiaClientImplBase() {
            @Override
            public StreamObserver<WriteRequest> writeStream(
                    StreamObserver<WriteResponse> responseObserver) {
                return new StreamObserver<>() {
                    @Override
                    public void onNext(WriteRequest value) {
                        requests.add(value);
                        responseObserver.onNext(new WriteResponse());
                    }

                    @Override
                    public void onError(Throwable t) {}

                    @Override
                    public void onCompleted() {
                        responseObserver.onCompleted();
                    }
                };
            }
        };
    }

    private static Server writeServer(OxiaClientGrpc.OxiaClientImplBase service) throws Exception {
        return ServerBuilder.forPort(0).directExecutor().addService(service).build().start();
    }

    private static WriteRequest writeRequest(int id) {
        var request = new WriteRequest();
        request.setShard(1);
        request.addPut().setKey("key-" + id).setValue(("value-" + id).getBytes(StandardCharsets.UTF_8));
        return request;
    }

    private static java.util.List<String> keys(Queue<WriteRequest> requests) {
        return requests.stream().map(request -> request.getPutAt(0).getKey()).toList();
    }

    // A value whose serialization waits until the test lets it proceed
    private static final class BlockingValue extends UnpooledHeapByteBuf {
        final CountDownLatch serializing = new CountDownLatch(1);
        final CountDownLatch proceed = new CountDownLatch(1);

        BlockingValue(byte[] value) {
            super(UnpooledByteBufAllocator.DEFAULT, value, value.length);
        }

        @Override
        public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
            serializing.countDown();
            try {
                proceed.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return super.getBytes(index, dst, dstIndex, length);
        }
    }

    private static io.oxia.client.ClientConfig clientConfig(String address) {
        return ((OxiaClientBuilderImpl)
                        OxiaClientBuilder.create(address)
                                .connectionBackoff(Duration.ofMillis(10), Duration.ofMillis(50)))
                .getClientConfig();
    }

    private static Throwable notLeaderError() {
        var grpcStatus =
                com.google.rpc.Status.newBuilder()
                        .setCode(Status.Code.ABORTED.value())
                        .addDetails(
                                Any.pack(
                                        ErrorInfo.newBuilder()
                                                .setDomain("oxia.io")
                                                .setReason("NODE_IS_NOT_LEADER")
                                                .build()))
                        .build();
        return StatusProto.toStatusRuntimeException(grpcStatus);
    }
}
