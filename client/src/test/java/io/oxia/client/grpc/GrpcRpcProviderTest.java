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
import io.grpc.Context;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.proto.CloseSessionRequest;
import io.oxia.proto.CloseSessionResponse;
import io.oxia.proto.CreateSessionRequest;
import io.oxia.proto.CreateSessionResponse;
import io.oxia.proto.GetSequenceUpdatesRequest;
import io.oxia.proto.GetSequenceUpdatesResponse;
import io.oxia.proto.KeepAliveResponse;
import io.oxia.proto.ListRequest;
import io.oxia.proto.ListResponse;
import io.oxia.proto.NotificationBatch;
import io.oxia.proto.NotificationsRequest;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.RangeScanRequest;
import io.oxia.proto.RangeScanResponse;
import io.oxia.proto.ReadRequest;
import io.oxia.proto.ReadResponse;
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
    void getNotificationsRetriesWithLeaderHint() throws Exception {
        var leaderServerRequests = new AtomicReference<NotificationsRequest>();
        var leaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void getNotifications(
                            NotificationsRequest request, StreamObserver<NotificationBatch> responseObserver) {
                        leaderServerRequests.set(request);
                        responseObserver.onNext(new NotificationBatch().setOffset(1));
                        responseObserver.onCompleted();
                    }
                };
        Server leaderServer =
                ServerBuilder.forPort(0).directExecutor().addService(leaderService).build().start();
        var leaderAddress = "localhost:" + leaderServer.getPort();
        var firstServerRequests = new AtomicReference<NotificationsRequest>();
        var staleLeaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void getNotifications(
                            NotificationsRequest request, StreamObserver<NotificationBatch> responseObserver) {
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
            var request = new NotificationsRequest();
            request.setShard(1);
            var notification = new AtomicReference<NotificationBatch>();

            provider.getNotifications(
                    request,
                    new StreamObserver<>() {
                        @Override
                        public void onNext(NotificationBatch value) {
                            notification.set(value);
                        }

                        @Override
                        public void onError(Throwable t) {}

                        @Override
                        public void onCompleted() {}
                    });

            await()
                    .untilAsserted(
                            () -> {
                                assertThat(notification.get()).isNotNull();
                                assertThat(notification.get().getOffset()).isEqualTo(1);
                                assertThat(firstServerRequests.get()).isNotNull();
                                assertThat(leaderServerRequests.get()).isNotNull();
                            });
        } finally {
            executor.shutdownNow();
            staleLeaderServer.shutdownNow();
            leaderServer.shutdownNow();
        }
    }

    @Test
    void readRetriesWithLeaderHint() throws Exception {
        var leaderServerRequests = new AtomicReference<ReadRequest>();
        var leaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
                        leaderServerRequests.set(request);
                        responseObserver.onNext(new ReadResponse());
                        responseObserver.onCompleted();
                    }
                };
        Server leaderServer =
                ServerBuilder.forPort(0).directExecutor().addService(leaderService).build().start();
        var leaderAddress = "localhost:" + leaderServer.getPort();
        var firstServerRequests = new AtomicReference<ReadRequest>();
        var staleLeaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
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
            var request = new ReadRequest();
            request.setShard(1);
            var response = new AtomicReference<ReadResponse>();

            provider.read(request, capturingStreamObserver(response));

            await()
                    .untilAsserted(
                            () -> {
                                assertThat(response.get()).isNotNull();
                                assertThat(firstServerRequests.get()).isNotNull();
                                assertThat(leaderServerRequests.get()).isNotNull();
                            });
        } finally {
            executor.shutdownNow();
            staleLeaderServer.shutdownNow();
            leaderServer.shutdownNow();
        }
    }

    @Test
    void listRetriesWithLeaderHint() throws Exception {
        var leaderServerRequests = new AtomicReference<ListRequest>();
        var leaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void list(ListRequest request, StreamObserver<ListResponse> responseObserver) {
                        leaderServerRequests.set(request);
                        responseObserver.onNext(new ListResponse().addAllKeys(java.util.List.of("key")));
                        responseObserver.onCompleted();
                    }
                };
        Server leaderServer =
                ServerBuilder.forPort(0).directExecutor().addService(leaderService).build().start();
        var leaderAddress = "localhost:" + leaderServer.getPort();
        var firstServerRequests = new AtomicReference<ListRequest>();
        var staleLeaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void list(ListRequest request, StreamObserver<ListResponse> responseObserver) {
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
            var request = new ListRequest();
            request.setShard(1);
            var response = new AtomicReference<ListResponse>();

            provider.list(request, capturingCancelableObserver(response));

            await()
                    .untilAsserted(
                            () -> {
                                assertThat(response.get()).isNotNull();
                                assertThat(response.get().getKeyAt(0)).isEqualTo("key");
                                assertThat(firstServerRequests.get()).isNotNull();
                                assertThat(leaderServerRequests.get()).isNotNull();
                            });
        } finally {
            executor.shutdownNow();
            staleLeaderServer.shutdownNow();
            leaderServer.shutdownNow();
        }
    }

    @Test
    void rangeScanRetriesWithLeaderHint() throws Exception {
        var leaderServerRequests = new AtomicReference<RangeScanRequest>();
        var leaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void rangeScan(
                            RangeScanRequest request, StreamObserver<RangeScanResponse> responseObserver) {
                        leaderServerRequests.set(request);
                        responseObserver.onNext(new RangeScanResponse());
                        responseObserver.onCompleted();
                    }
                };
        Server leaderServer =
                ServerBuilder.forPort(0).directExecutor().addService(leaderService).build().start();
        var leaderAddress = "localhost:" + leaderServer.getPort();
        var firstServerRequests = new AtomicReference<RangeScanRequest>();
        var staleLeaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void rangeScan(
                            RangeScanRequest request, StreamObserver<RangeScanResponse> responseObserver) {
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
            var request = new RangeScanRequest();
            request.setShard(1);
            var response = new AtomicReference<RangeScanResponse>();

            provider.rangeScan(request, capturingCancelableObserver(response));

            await()
                    .untilAsserted(
                            () -> {
                                assertThat(response.get()).isNotNull();
                                assertThat(firstServerRequests.get()).isNotNull();
                                assertThat(leaderServerRequests.get()).isNotNull();
                            });
        } finally {
            executor.shutdownNow();
            staleLeaderServer.shutdownNow();
            leaderServer.shutdownNow();
        }
    }

    @Test
    void getSequenceUpdatesRetriesWithLeaderHint() throws Exception {
        var leaderServerRequests = new AtomicReference<GetSequenceUpdatesRequest>();
        var leaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void getSequenceUpdates(
                            GetSequenceUpdatesRequest request,
                            StreamObserver<GetSequenceUpdatesResponse> responseObserver) {
                        leaderServerRequests.set(request);
                        responseObserver.onNext(
                                new GetSequenceUpdatesResponse().setHighestSequenceKey("key-1"));
                        responseObserver.onCompleted();
                    }
                };
        Server leaderServer =
                ServerBuilder.forPort(0).directExecutor().addService(leaderService).build().start();
        var leaderAddress = "localhost:" + leaderServer.getPort();
        var firstServerRequests = new AtomicReference<GetSequenceUpdatesRequest>();
        var staleLeaderService =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void getSequenceUpdates(
                            GetSequenceUpdatesRequest request,
                            StreamObserver<GetSequenceUpdatesResponse> responseObserver) {
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
            var request = new GetSequenceUpdatesRequest();
            request.setShard(1).setKey("key");
            var response = new AtomicReference<GetSequenceUpdatesResponse>();

            provider.getSequenceUpdates(request, capturingCancelableObserver(response));

            await()
                    .untilAsserted(
                            () -> {
                                assertThat(response.get()).isNotNull();
                                assertThat(response.get().getHighestSequenceKey()).isEqualTo("key-1");
                                assertThat(firstServerRequests.get()).isNotNull();
                                assertThat(leaderServerRequests.get()).isNotNull();
                            });
        } finally {
            executor.shutdownNow();
            staleLeaderServer.shutdownNow();
            leaderServer.shutdownNow();
        }
    }

    @Test
    void sessionRequestsUseRequestTimeoutDeadline() throws Exception {
        var createSessionHasDeadline = new AtomicReference<Boolean>();
        var closeSessionHasDeadline = new AtomicReference<Boolean>();
        var service =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void createSession(
                            CreateSessionRequest request,
                            StreamObserver<CreateSessionResponse> responseObserver) {
                        createSessionHasDeadline.set(Context.current().getDeadline() != null);
                        responseObserver.onNext(new CreateSessionResponse().setSessionId(1));
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void closeSession(
                            CloseSessionRequest request, StreamObserver<CloseSessionResponse> responseObserver) {
                        closeSessionHasDeadline.set(Context.current().getDeadline() != null);
                        responseObserver.onNext(new CloseSessionResponse());
                        responseObserver.onCompleted();
                    }
                };
        Server server = ServerBuilder.forPort(0).directExecutor().addService(service).build().start();
        var address = "localhost:" + server.getPort();
        var executor = Executors.newSingleThreadScheduledExecutor();
        var config =
                ((OxiaClientBuilderImpl)
                                OxiaClientBuilder.create(address).requestTimeout(Duration.ofSeconds(5)))
                        .getClientConfig();

        try (var provider = new GrpcRpcProvider(config, executor, shardId -> address)) {
            provider.createSession(new CreateSessionRequest().setShard(1)).get(5, TimeUnit.SECONDS);
            provider.closeSession(new CloseSessionRequest().setShard(1)).get(5, TimeUnit.SECONDS);

            await()
                    .untilAsserted(
                            () -> {
                                assertThat(createSessionHasDeadline.get()).isTrue();
                                assertThat(closeSessionHasDeadline.get()).isTrue();
                            });
        } finally {
            executor.shutdownNow();
            server.shutdownNow();
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
                            throw OxiaStatusException.shardNotFound(shardId);
                        })) {
            var request = new GetSequenceUpdatesRequest();
            request.setShard(1).setKey("key");
            var error = new AtomicReference<Throwable>();

            provider.getSequenceUpdates(
                    request,
                    new CancelableStreamObserver<>() {
                        @Override
                        protected void handleNext(@NonNull GetSequenceUpdatesResponse value) {}

                        @Override
                        protected void handleError(@NonNull Throwable throwable) {
                            error.set(throwable);
                        }

                        @Override
                        protected void handleComplete() {}
                    });

            await()
                    .untilAsserted(
                            () -> {
                                assertThat(error.get()).isInstanceOf(OxiaStatusException.class);
                                var oxiaError = (OxiaStatusException) error.get();
                                assertThat(oxiaError.getStatusCode()).isEqualTo(OxiaStatusCode.SHARD_NOT_FOUND);
                                assertThat(oxiaError.getMetadata()).containsEntry("shard", "1");
                            });
        } finally {
            executor.shutdownNow();
        }
    }

    private static <T> StreamObserver<T> capturingStreamObserver(AtomicReference<T> response) {
        return new StreamObserver<>() {
            @Override
            public void onNext(T value) {
                response.set(value);
            }

            @Override
            public void onError(Throwable t) {}

            @Override
            public void onCompleted() {}
        };
    }

    private static <T> CancelableStreamObserver<T> capturingCancelableObserver(
            AtomicReference<T> response) {
        return new CancelableStreamObserver<>() {
            @Override
            protected void handleNext(@NonNull T value) {
                response.set(value);
            }

            @Override
            protected void handleError(@NonNull Throwable t) {}

            @Override
            protected void handleComplete() {}
        };
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
