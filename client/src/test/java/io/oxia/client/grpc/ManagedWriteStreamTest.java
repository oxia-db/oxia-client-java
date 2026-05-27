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
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
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
                var stream = new ManagedWriteStream(1, provider, executor, config.requestTimeout())) {
            var first = writeRequest(1);
            var second = writeRequest(2);

            var firstFuture = stream.send(() -> first);
            var secondFuture = stream.send(() -> second);

            firstFuture.get(5, TimeUnit.SECONDS);
            secondFuture.get(5, TimeUnit.SECONDS);

            await().untilAsserted(() -> assertThat(keys(requests)).containsExactly("key-1", "key-2"));
        } finally {
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
                var stream = new ManagedWriteStream(1, provider, executor, config.requestTimeout())) {
            var pending = stream.send(() -> writeRequest(1));

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
            assertThat(stream.send(() -> writeRequest(2))).isCompletedExceptionally();
        } finally {
            executor.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void timeoutClosesCurrentStreamAndNextLookupCreatesNewStream() throws Exception {
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
        var config =
                ((OxiaClientBuilderImpl)
                                OxiaClientBuilder.create(address)
                                        .requestTimeout(Duration.ofMillis(50))
                                        .connectionBackoff(Duration.ofMillis(10), Duration.ofMillis(50)))
                        .getClientConfig();

        try (var provider = new GrpcRpcProvider(config, executor, shard -> address)) {
            var firstStream = provider.getWriteStream(1);
            var timedOut = firstStream.send(() -> writeRequest(1));

            assertThatThrownBy(() -> timedOut.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(OxiaStatusException.class)
                    .satisfies(
                            error -> {
                                var oxiaError = (OxiaStatusException) error.getCause();
                                assertThat(oxiaError.getStatusCode()).isEqualTo(OxiaStatusCode.TIMEOUT);
                            });

            await().untilAsserted(() -> assertThat(firstStream.isClosed()).isTrue());
            assertThat(provider.getWriteStream(1)).isNotSameAs(firstStream);
        } finally {
            executor.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void replaysInflightWritesAfterRetryableError() throws Exception {
        var leaderRequests = new ConcurrentLinkedQueue<WriteRequest>();
        Server leaderServer = writeServer(respondingWriteService(leaderRequests));
        var leaderAddress = "localhost:" + leaderServer.getPort();

        var staleRequests = new ConcurrentLinkedQueue<WriteRequest>();
        var staleRequestCount = new AtomicInteger();
        Server staleServer =
                writeServer(
                        new OxiaClientGrpc.OxiaClientImplBase() {
                            @Override
                            public StreamObserver<WriteRequest> writeStream(
                                    StreamObserver<WriteResponse> responseObserver) {
                                return new StreamObserver<>() {
                                    @Override
                                    public void onNext(WriteRequest value) {
                                        staleRequests.add(value);
                                        if (staleRequestCount.incrementAndGet() == 2) {
                                            responseObserver.onError(retryableErrorWithLeaderHint(leaderAddress));
                                        }
                                    }

                                    @Override
                                    public void onError(Throwable t) {}

                                    @Override
                                    public void onCompleted() {}
                                };
                            }
                        });
        var staleAddress = "localhost:" + staleServer.getPort();
        var executor = Executors.newSingleThreadScheduledExecutor();
        var config = clientConfig(staleAddress);

        try (var provider = new GrpcRpcProvider(config, executor, shard -> staleAddress);
                var stream = new ManagedWriteStream(1, provider, executor, config.requestTimeout())) {
            var firstFuture = stream.send(() -> writeRequest(1));
            var secondFuture = stream.send(() -> writeRequest(2));

            firstFuture.get(5, TimeUnit.SECONDS);
            secondFuture.get(5, TimeUnit.SECONDS);

            await()
                    .untilAsserted(() -> assertThat(keys(staleRequests)).containsExactly("key-1", "key-2"));
            await()
                    .untilAsserted(() -> assertThat(keys(leaderRequests)).containsExactly("key-1", "key-2"));
            await()
                    .untilAsserted(
                            () -> assertThat(values(leaderRequests)).containsExactly("value-1", "value-2"));
        } finally {
            executor.shutdownNow();
            staleServer.shutdownNow();
            leaderServer.shutdownNow();
        }
    }

    @Test
    void resetsObserverWhenRecoveryReplayFails() throws Exception {
        var rpcProvider = mock(RpcProvider.class);
        var openAttempts = new AtomicInteger();
        var replayFailures = new AtomicInteger();
        var requests = new ConcurrentLinkedQueue<WriteRequest>();
        var responseObservers = new ConcurrentLinkedQueue<StreamObserver<WriteResponse>>();
        when(rpcProvider.writeStream(anyLong(), nullable(OxiaStatusException.class), any()))
                .thenAnswer(
                        invocation -> {
                            var responseObserver = invocation.<StreamObserver<WriteResponse>>getArgument(2);
                            responseObservers.add(responseObserver);
                            var attempt = openAttempts.incrementAndGet();
                            return new StreamObserver<WriteRequest>() {
                                @Override
                                public void onNext(WriteRequest value) {
                                    requests.add(value);
                                    if (attempt == 2) {
                                        replayFailures.incrementAndGet();
                                        throw Status.UNAVAILABLE.withDescription("failed replay").asRuntimeException();
                                    }
                                    if (attempt == 3) {
                                        responseObserver.onNext(new WriteResponse());
                                    }
                                }

                                @Override
                                public void onError(Throwable t) {}

                                @Override
                                public void onCompleted() {}
                            };
                        });

        var executor = Executors.newSingleThreadScheduledExecutor();
        try (var stream = new ManagedWriteStream(1, rpcProvider, executor, Duration.ofSeconds(30))) {
            var future = stream.send(() -> writeRequest(1));
            await()
                    .untilAsserted(
                            () -> {
                                assertThat(openAttempts).hasValue(1);
                                assertThat(requests).hasSize(1);
                            });

            responseObservers
                    .peek()
                    .onError(Status.UNAVAILABLE.withDescription("stale leader").asRuntimeException());

            future.get(5, TimeUnit.SECONDS);

            await()
                    .untilAsserted(
                            () -> {
                                assertThat(openAttempts).hasValue(3);
                                assertThat(replayFailures).hasValue(1);
                                assertThat(keys(requests)).containsExactly("key-1", "key-1", "key-1");
                            });
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void ignoresResponsesFromInactiveSubStream() throws Exception {
        var rpcProvider = mock(RpcProvider.class);
        var responseObservers = new ConcurrentLinkedQueue<StreamObserver<WriteResponse>>();
        var requestCount = new AtomicInteger();
        when(rpcProvider.writeStream(anyLong(), nullable(OxiaStatusException.class), any()))
                .thenAnswer(
                        invocation -> {
                            var responseObserver = invocation.<StreamObserver<WriteResponse>>getArgument(2);
                            responseObservers.add(responseObserver);
                            return new StreamObserver<WriteRequest>() {
                                @Override
                                public void onNext(WriteRequest value) {
                                    requestCount.incrementAndGet();
                                }

                                @Override
                                public void onError(Throwable t) {}

                                @Override
                                public void onCompleted() {}
                            };
                        });

        var executor = Executors.newSingleThreadScheduledExecutor();
        try (var stream = new ManagedWriteStream(1, rpcProvider, executor, Duration.ofSeconds(30))) {
            var future = stream.send(() -> writeRequest(1));
            assertThat(requestCount).hasValue(1);
            assertThat(responseObservers).hasSize(1);

            var inactiveObserver = responseObservers.peek();
            inactiveObserver.onError(
                    Status.UNAVAILABLE.withDescription("stale leader").asRuntimeException());

            await().untilAsserted(() -> assertThat(responseObservers).hasSize(2));
            inactiveObserver.onNext(new WriteResponse());
            assertThat(future).isNotDone();

            responseObservers.stream().skip(1).findFirst().orElseThrow().onNext(new WriteResponse());
            future.get(5, TimeUnit.SECONDS);
            assertThat(requestCount).hasValue(2);
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

    private static java.util.List<String> values(Queue<WriteRequest> requests) {
        return requests.stream()
                .map(request -> new String(request.getPutAt(0).getValue(), StandardCharsets.UTF_8))
                .toList();
    }

    private static io.oxia.client.ClientConfig clientConfig(String address) {
        return ((OxiaClientBuilderImpl)
                        OxiaClientBuilder.create(address)
                                .connectionBackoff(Duration.ofMillis(10), Duration.ofMillis(50)))
                .getClientConfig();
    }

    private static Throwable retryableErrorWithLeaderHint(String leaderAddress) {
        var grpcStatus =
                com.google.rpc.Status.newBuilder()
                        .setCode(Status.Code.ABORTED.value())
                        .setMessage("oxia: node is not leader")
                        .addDetails(
                                Any.pack(
                                        ErrorInfo.newBuilder()
                                                .setDomain("oxia.io")
                                                .setReason("NODE_IS_NOT_LEADER")
                                                .putMetadata("shard", "1")
                                                .putMetadata("leader", leaderAddress)
                                                .build()))
                        .build();
        return StatusProto.toStatusRuntimeException(grpcStatus);
    }
}
