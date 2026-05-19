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

import io.grpc.ClientCall;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
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
import io.oxia.proto.ShardAssignments;
import io.oxia.proto.ShardAssignmentsRequest;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.LongFunction;
import lombok.NonNull;

final class GrpcRpcProvider implements RpcProvider {
    private final ClientConfig clientConfig;
    private final ConnectionManager connectionManager;
    private final LongFunction<String> shardLeaderProvider;
    private final WriteStreamManager writeStreamManager;

    GrpcRpcProvider(
            @NonNull ClientConfig clientConfig,
            @NonNull ScheduledExecutorService executor,
            @NonNull LongFunction<String> shardLeaderProvider) {
        this.clientConfig = clientConfig;
        this.connectionManager = new ConnectionManager(clientConfig, executor);
        this.shardLeaderProvider = shardLeaderProvider;
        this.writeStreamManager =
                new WriteStreamManager(
                        clientConfig.namespace(),
                        shardId -> connectionManager.getConnection(shardLeaderProvider.apply(shardId)).stub());
    }

    @Override
    public void getShardAssignments(
            @NonNull ShardAssignmentsRequest request,
            @NonNull StreamObserver<ShardAssignments> observer) {
        try {
            connectionManager
                    .getConnection(clientConfig.serviceAddress())
                    .stub()
                    .getShardAssignments(request, withOxiaErrors(observer));
        } catch (Throwable error) {
            observer.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public void getNotifications(
            @NonNull NotificationsRequest request, @NonNull StreamObserver<NotificationBatch> observer) {
        try {
            connectionManager
                    .getConnection(shardLeaderProvider.apply(request.getShard()))
                    .stub()
                    .getNotifications(request, withOxiaErrors(observer));
        } catch (Throwable error) {
            observer.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public CompletableFuture<CreateSessionResponse> createSession(
            @NonNull CreateSessionRequest request) {
        final CompletableFuture<CreateSessionResponse> future = new CompletableFuture<>();
        try {
            connectionManager
                    .getConnection(shardLeaderProvider.apply(request.getShard()))
                    .stub()
                    .createSession(
                            request,
                            new StreamObserver<>() {
                                private CreateSessionResponse response;

                                @Override
                                public void onNext(@NonNull CreateSessionResponse response) {
                                    this.response = response;
                                }

                                @Override
                                public void onError(@NonNull Throwable error) {
                                    future.completeExceptionally(OxiaStatusException.toException(error));
                                }

                                @Override
                                public void onCompleted() {
                                    future.complete(response);
                                }
                            });
        } catch (Throwable error) {
            future.completeExceptionally(OxiaStatusException.toException(error));
        }
        return future;
    }

    @Override
    public void keepAlive(
            @NonNull SessionHeartbeat heartbeat, @NonNull StreamObserver<KeepAliveResponse> observer) {
        try {
            connectionManager
                    .getConnection(shardLeaderProvider.apply(heartbeat.getShard()))
                    .stub()
                    .keepAlive(heartbeat, withOxiaErrors(observer));
        } catch (Throwable error) {
            observer.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public CompletableFuture<CloseSessionResponse> closeSession(
            @NonNull CloseSessionRequest request) {
        final CompletableFuture<CloseSessionResponse> future = new CompletableFuture<>();
        try {
            connectionManager
                    .getConnection(shardLeaderProvider.apply(request.getShard()))
                    .stub()
                    .closeSession(
                            request,
                            new StreamObserver<>() {
                                private CloseSessionResponse response;

                                @Override
                                public void onNext(@NonNull CloseSessionResponse response) {
                                    this.response = response;
                                }

                                @Override
                                public void onError(@NonNull Throwable error) {
                                    future.completeExceptionally(OxiaStatusException.toException(error));
                                }

                                @Override
                                public void onCompleted() {
                                    future.complete(response);
                                }
                            });
        } catch (Throwable error) {
            future.completeExceptionally(OxiaStatusException.toException(error));
        }
        return future;
    }

    @Override
    public void read(@NonNull ReadRequest request, @NonNull StreamObserver<ReadResponse> observer) {
        try {
            connectionManager
                    .getConnection(shardLeaderProvider.apply(request.getShard()))
                    .stub()
                    .read(request, withOxiaErrors(observer));
        } catch (Throwable error) {
            observer.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public CompletableFuture<WriteResponse> write(@NonNull WriteRequest request) {
        try {
            return writeStreamManager
                    .getWriteStream(request.getShard())
                    .send(request)
                    .exceptionally(
                            error -> {
                                throw new CompletionException(OxiaStatusException.toException(error));
                            });
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(OxiaStatusException.toException(error));
        }
    }

    @Override
    public void list(@NonNull ListRequest request, @NonNull StreamObserver<ListResponse> observer) {
        try {
            connectionManager
                    .getConnection(shardLeaderProvider.apply(request.getShard()))
                    .stub()
                    .list(request, withOxiaErrors(observer));
        } catch (Throwable error) {
            observer.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public void rangeScan(
            @NonNull RangeScanRequest request,
            @NonNull ClientResponseObserver<RangeScanRequest, RangeScanResponse> observer) {
        try {
            connectionManager
                    .getConnection(shardLeaderProvider.apply(request.getShard()))
                    .stub()
                    .rangeScan(request, withOxiaErrors(observer));
        } catch (Throwable error) {
            observer.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public ClientCall<GetSequenceUpdatesRequest, GetSequenceUpdatesResponse> getSequenceUpdates(
            @NonNull GetSequenceUpdatesRequest request,
            @NonNull StreamObserver<GetSequenceUpdatesResponse> observer) {
        final var stub =
                connectionManager.getConnection(shardLeaderProvider.apply(request.getShard())).stub();
        final var call =
                stub.getChannel()
                        .newCall(OxiaClientGrpc.getGetSequenceUpdatesMethod(), stub.getCallOptions());
        ClientCalls.asyncServerStreamingCall(call, request, withOxiaErrors(observer));
        return call;
    }

    @Override
    public void close() throws Exception {
        connectionManager.close();
    }

    private static <T> StreamObserver<T> withOxiaErrors(StreamObserver<T> observer) {
        return new StreamObserver<>() {
            @Override
            public void onNext(@NonNull T value) {
                observer.onNext(value);
            }

            @Override
            public void onError(@NonNull Throwable error) {
                observer.onError(OxiaStatusException.toException(error));
            }

            @Override
            public void onCompleted() {
                observer.onCompleted();
            }
        };
    }

    private static <ReqT, RespT> ClientResponseObserver<ReqT, RespT> withOxiaErrors(
            ClientResponseObserver<ReqT, RespT> observer) {
        return new ClientResponseObserver<>() {
            @Override
            public void beforeStart(@NonNull ClientCallStreamObserver<ReqT> requestStream) {
                observer.beforeStart(requestStream);
            }

            @Override
            public void onNext(@NonNull RespT value) {
                observer.onNext(value);
            }

            @Override
            public void onError(@NonNull Throwable error) {
                observer.onError(OxiaStatusException.toException(error));
            }

            @Override
            public void onCompleted() {
                observer.onCompleted();
            }
        };
    }
}
