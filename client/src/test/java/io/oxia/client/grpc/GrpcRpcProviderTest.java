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
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.client.shard.NoShardAvailableException;
import io.oxia.proto.CloseSessionRequest;
import io.oxia.proto.CloseSessionResponse;
import io.oxia.proto.CreateSessionRequest;
import io.oxia.proto.CreateSessionResponse;
import io.oxia.proto.GetSequenceUpdatesRequest;
import io.oxia.proto.GetSequenceUpdatesResponse;
import io.oxia.proto.KeepAliveResponse;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.SessionHeartbeat;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import org.junit.jupiter.api.Test;

class GrpcRpcProviderTest {
    @Test
    void keepAliveRetriesWithLeaderHint() throws Exception {
        var leaderServerRequests = new AtomicReference<SessionHeartbeat>();
        var leaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void keepAlive(
                            SessionHeartbeat request, StreamObserver<KeepAliveResponse> responseObserver) {
                        leaderServerRequests.set(request);
                        responseObserver.onNext(new KeepAliveResponse());
                        responseObserver.onCompleted();
                    }
                };
        Server leaderServer =
                ServerBuilder.forPort(0).directExecutor().addService(leaderService).build().start();
        var leaderAddress = "localhost:" + leaderServer.getPort();
        var firstServerRequests = new AtomicReference<SessionHeartbeat>();
        var staleLeaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void keepAlive(
                            SessionHeartbeat request, StreamObserver<KeepAliveResponse> responseObserver) {
                        firstServerRequests.set(request);
                        responseObserver.onError(retryableErrorWithLeaderHint(leaderAddress));
                    }
                };
        Server staleLeaderServer =
                ServerBuilder.forPort(0).directExecutor().addService(staleLeaderService).build().start();
        var staleLeaderAddress = "localhost:" + staleLeaderServer.getPort();
        var executor = Executors.newSingleThreadScheduledExecutor();
        var config =
                ((OxiaClientBuilderImpl)
                                OxiaClientBuilder.create(staleLeaderAddress)
                                        .connectionBackoff(Duration.ofMillis(10), Duration.ofMillis(50)))
                        .getClientConfig();

        try (var provider = new GrpcRpcProvider(config, executor, shardId -> staleLeaderAddress)) {
            var request = new SessionHeartbeat();
            request.setShard(1);

            provider.keepAlive(request, Duration.ofSeconds(5)).get(5, TimeUnit.SECONDS);

            assertThat(firstServerRequests.get()).isNotNull();
            assertThat(leaderServerRequests.get()).isNotNull();
        } finally {
            executor.shutdownNow();
            staleLeaderServer.shutdownNow();
            leaderServer.shutdownNow();
        }
    }

    @Test
    void closeSessionRetriesWithLeaderHint() throws Exception {
        var leaderServerRequests = new AtomicReference<CloseSessionRequest>();
        var leaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void closeSession(
                            CloseSessionRequest request, StreamObserver<CloseSessionResponse> responseObserver) {
                        leaderServerRequests.set(request);
                        responseObserver.onNext(new CloseSessionResponse());
                        responseObserver.onCompleted();
                    }
                };
        Server leaderServer =
                ServerBuilder.forPort(0).directExecutor().addService(leaderService).build().start();
        var leaderAddress = "localhost:" + leaderServer.getPort();
        var firstServerRequests = new AtomicReference<CloseSessionRequest>();
        var staleLeaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void closeSession(
                            CloseSessionRequest request, StreamObserver<CloseSessionResponse> responseObserver) {
                        firstServerRequests.set(request);
                        responseObserver.onError(retryableErrorWithLeaderHint(leaderAddress));
                    }
                };
        Server staleLeaderServer =
                ServerBuilder.forPort(0).directExecutor().addService(staleLeaderService).build().start();
        var staleLeaderAddress = "localhost:" + staleLeaderServer.getPort();
        var executor = Executors.newSingleThreadScheduledExecutor();
        var config =
                ((OxiaClientBuilderImpl)
                                OxiaClientBuilder.create(staleLeaderAddress)
                                        .connectionBackoff(Duration.ofMillis(10), Duration.ofMillis(50)))
                        .getClientConfig();

        try (var provider = new GrpcRpcProvider(config, executor, shardId -> staleLeaderAddress)) {
            var request = new CloseSessionRequest();
            request.setShard(1);

            provider.closeSession(request).get(5, TimeUnit.SECONDS);

            assertThat(firstServerRequests.get()).isNotNull();
            assertThat(leaderServerRequests.get()).isNotNull();
        } finally {
            executor.shutdownNow();
            staleLeaderServer.shutdownNow();
            leaderServer.shutdownNow();
        }
    }

    @Test
    void createSessionRetriesWithLeaderHint() throws Exception {
        var leaderServerRequests = new AtomicReference<CreateSessionRequest>();
        var leaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void createSession(
                            CreateSessionRequest request,
                            io.grpc.stub.StreamObserver<CreateSessionResponse> responseObserver) {
                        leaderServerRequests.set(request);
                        responseObserver.onNext(new CreateSessionResponse().setSessionId(1));
                        responseObserver.onCompleted();
                    }
                };
        Server leaderServer =
                ServerBuilder.forPort(0).directExecutor().addService(leaderService).build().start();
        var leaderAddress = "localhost:" + leaderServer.getPort();
        var firstServerRequests = new AtomicReference<CreateSessionRequest>();
        var staleLeaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void createSession(
                            CreateSessionRequest request,
                            io.grpc.stub.StreamObserver<CreateSessionResponse> responseObserver) {
                        firstServerRequests.set(request);
                        responseObserver.onError(retryableErrorWithLeaderHint(leaderAddress));
                    }
                };
        Server staleLeaderServer =
                ServerBuilder.forPort(0).directExecutor().addService(staleLeaderService).build().start();
        var staleLeaderAddress = "localhost:" + staleLeaderServer.getPort();
        var executor = Executors.newSingleThreadScheduledExecutor();
        var config =
                ((OxiaClientBuilderImpl)
                                OxiaClientBuilder.create(staleLeaderAddress)
                                        .connectionBackoff(Duration.ofMillis(10), Duration.ofMillis(50)))
                        .getClientConfig();

        try (var provider = new GrpcRpcProvider(config, executor, shardId -> staleLeaderAddress)) {
            var request = new CreateSessionRequest();
            request.setShard(1);

            var response = provider.createSession(request).get(5, TimeUnit.SECONDS);

            assertThat(response.getSessionId()).isEqualTo(1);
            assertThat(firstServerRequests.get()).isNotNull();
            assertThat(leaderServerRequests.get()).isNotNull();
        } finally {
            executor.shutdownNow();
            staleLeaderServer.shutdownNow();
            leaderServer.shutdownNow();
        }
    }

    @Test
    void getSequenceUpdatesReportsLookupFailure() throws Exception {
        var executor = Executors.newSingleThreadScheduledExecutor();
        var config =
                ((OxiaClientBuilderImpl) OxiaClientBuilder.create("localhost:0")).getClientConfig();

        try (var provider =
                new GrpcRpcProvider(
                        config,
                        executor,
                        shardId -> {
                            throw new NoShardAvailableException(shardId);
                        })) {
            var request = new GetSequenceUpdatesRequest();
            request.setShard(1).setKey("key");
            var error = new AtomicReference<Throwable>();

            provider.getSequenceUpdates(
                    request,
                    new CancelableStreamObserver<>() {
                        @Override
                        protected void onNextValue(@NonNull GetSequenceUpdatesResponse value) {}

                        @Override
                        protected void onErrorValue(@NonNull Throwable throwable) {
                            error.set(throwable);
                        }

                        @Override
                        protected void onCompletedValue() {}
                    });

            await()
                    .untilAsserted(
                            () -> assertThat(error.get()).isInstanceOf(NoShardAvailableException.class));
        } finally {
            executor.shutdownNow();
        }
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
