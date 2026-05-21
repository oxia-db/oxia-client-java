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
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
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
                var stream = new ManagedWriteStream(1, provider, executor)) {
            var first = writeRequest(1);
            var second = writeRequest(2);

            var firstFuture = stream.sendWithRecovery(first);
            var secondFuture = stream.sendWithRecovery(second);

            firstFuture.get(5, TimeUnit.SECONDS);
            secondFuture.get(5, TimeUnit.SECONDS);

            await().untilAsserted(() -> assertThat(keys(requests)).containsExactly("key-1", "key-2"));
        } finally {
            executor.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void closeCancelsPendingWritesAndRejectsNewWrites() throws Exception {
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
                var stream = new ManagedWriteStream(1, provider, executor)) {
            CompletableFuture<WriteResponse> pending = stream.sendWithRecovery(writeRequest(1));

            stream.close();

            assertThat(pending).isCompletedExceptionally();
            assertThatThrownBy(pending::get)
                    .isInstanceOf(java.util.concurrent.CancellationException.class);
            assertThat(stream.sendWithRecovery(writeRequest(2))).isCompletedExceptionally();
        } finally {
            executor.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void replaysInflightWritesAfterRetryableError() throws Exception {
        var staleRequests = new ConcurrentLinkedQueue<WriteRequest>();
        var recoveredRequests = new ConcurrentLinkedQueue<WriteRequest>();
        var streamOpenCount = new AtomicInteger();
        var staleRequestCount = new AtomicInteger();
        Server staleServer =
                writeServer(
                        new OxiaClientGrpc.OxiaClientImplBase() {
                            @Override
                            public StreamObserver<WriteRequest> writeStream(
                                    StreamObserver<WriteResponse> responseObserver) {
                                var streamAttempt = streamOpenCount.incrementAndGet();
                                return new StreamObserver<>() {
                                    @Override
                                    public void onNext(WriteRequest value) {
                                        if (streamAttempt == 1) {
                                            staleRequests.add(value);
                                            if (staleRequestCount.incrementAndGet() == 2) {
                                                responseObserver.onError(
                                                        retryableErrorWithLeaderHint("localhost:0"));
                                            }
                                            return;
                                        }
                                        recoveredRequests.add(value);
                                        responseObserver.onNext(new WriteResponse());
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
                var stream = new ManagedWriteStream(1, provider, executor)) {
            var firstFuture = stream.sendWithRecovery(writeRequest(1));
            var secondFuture = stream.sendWithRecovery(writeRequest(2));

            firstFuture.get(5, TimeUnit.SECONDS);
            secondFuture.get(5, TimeUnit.SECONDS);

            await()
                    .untilAsserted(() -> assertThat(keys(staleRequests)).containsExactly("key-1", "key-2"));
            await()
                    .untilAsserted(() -> assertThat(keys(recoveredRequests)).containsExactly("key-1", "key-2"));
        } finally {
            executor.shutdownNow();
            staleServer.shutdownNow();
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
        request.addPut().setKey("key-" + id);
        return request;
    }

    private static java.util.List<String> keys(Queue<WriteRequest> requests) {
        return requests.stream().map(request -> request.getPutAt(0).getKey()).toList();
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
