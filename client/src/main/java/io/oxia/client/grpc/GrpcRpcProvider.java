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

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.github.merlimat.slog.Logger;
import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.client.grpc.observer.ManagedClientResponseObserver;
import io.oxia.client.grpc.observer.ManagedObservers;
import io.oxia.client.grpc.observer.ManagedStreamObserver;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import lombok.NonNull;

final class GrpcRpcProvider implements RpcProvider {
    private static final Logger log = Logger.get(GrpcRpcProvider.class);

    private final ClientConfig clientConfig;
    private final ConnectionManager connectionManager;
    private final ScheduledExecutorService executor;
    private final LongFunction<String> shardLeaderProvider;
    private final ManagedWriteStream managedWriteStream;

    GrpcRpcProvider(
            @NonNull ClientConfig clientConfig,
            @NonNull ScheduledExecutorService executor,
            @NonNull LongFunction<String> shardLeaderProvider) {
        this.clientConfig = clientConfig;
        this.executor = executor;
        this.connectionManager = new ConnectionManager(clientConfig, executor);
        this.shardLeaderProvider = shardLeaderProvider;
        this.managedWriteStream =
                new ManagedWriteStream(
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
                    .getShardAssignments(request, new ManagedStreamObserver<>(observer));
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
                    .getNotifications(request, new ManagedStreamObserver<>(observer));
        } catch (Throwable error) {
            observer.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public CompletableFuture<CreateSessionResponse> createSession(
            @NonNull CreateSessionRequest request) {
        final var hint = new AtomicReference<OxiaStatusException>();
        return Failsafe.with(getRetryPolicy(hint))
                .with(executor)
                .getStageAsync(
                        () -> {
                            final var future = new CompletableFuture<CreateSessionResponse>();
                            try {
                                connectionManager
                                        .getConnection(getLeader(request.getShard(), hint))
                                        .stub()
                                        .createSession(request, ManagedObservers.toCompletableFuture(future));
                            } catch (Throwable error) {
                                future.completeExceptionally(OxiaStatusException.toException(error));
                            }
                            return future;
                        });
    }

    @Override
    public CompletableFuture<KeepAliveResponse> keepAlive(@NonNull SessionHeartbeat heartbeat) {
        final var hint = new AtomicReference<OxiaStatusException>();
        return Failsafe.with(getRetryPolicy(hint))
                .with(executor)
                .getStageAsync(
                        () -> {
                            final var future = new CompletableFuture<KeepAliveResponse>();
                            try {
                                connectionManager
                                        .getConnection(getLeader(heartbeat.getShard(), hint))
                                        .stub()
                                        .keepAlive(heartbeat, ManagedObservers.toCompletableFuture(future));
                            } catch (Throwable error) {
                                future.completeExceptionally(OxiaStatusException.toException(error));
                            }
                            return future;
                        });
    }

    @Override
    public CompletableFuture<CloseSessionResponse> closeSession(
            @NonNull CloseSessionRequest request) {
        final var hint = new AtomicReference<OxiaStatusException>();
        return Failsafe.with(getRetryPolicy(hint))
                .with(executor)
                .getStageAsync(
                        () -> {
                            final var future = new CompletableFuture<CloseSessionResponse>();
                            try {
                                connectionManager
                                        .getConnection(getLeader(request.getShard(), hint))
                                        .stub()
                                        .closeSession(request, ManagedObservers.toCompletableFuture(future));
                            } catch (Throwable error) {
                                future.completeExceptionally(OxiaStatusException.toException(error));
                            }
                            return future;
                        });
    }

    @Override
    public void read(@NonNull ReadRequest request, @NonNull StreamObserver<ReadResponse> observer) {
        try {
            connectionManager
                    .getConnection(shardLeaderProvider.apply(request.getShard()))
                    .stub()
                    .read(request, new ManagedStreamObserver<>(observer));
        } catch (Throwable error) {
            observer.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public CompletableFuture<WriteResponse> write(@NonNull WriteRequest request) {
        try {
            return managedWriteStream
                    .write(request)
                    .exceptionally(
                            error -> {
                                throw new CompletionException(OxiaStatusException.toException(error));
                            });
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(OxiaStatusException.toException(error));
        }
    }

    @Override
    public void list(
            @NonNull ListRequest request, @NonNull CancelableStreamObserver<ListResponse> observer) {
        try {
            connectionManager
                    .getConnection(shardLeaderProvider.apply(request.getShard()))
                    .stub()
                    .list(request, new ManagedClientResponseObserver<>(observer));
        } catch (Throwable error) {
            observer.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public void rangeScan(
            @NonNull RangeScanRequest request,
            @NonNull CancelableStreamObserver<RangeScanResponse> observer) {
        try {
            connectionManager
                    .getConnection(shardLeaderProvider.apply(request.getShard()))
                    .stub()
                    .rangeScan(request, new ManagedClientResponseObserver<>(observer));
        } catch (Throwable error) {
            observer.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public void getSequenceUpdates(
            @NonNull GetSequenceUpdatesRequest request,
            @NonNull CancelableStreamObserver<GetSequenceUpdatesResponse> observer) {
        try {
            connectionManager
                    .getConnection(shardLeaderProvider.apply(request.getShard()))
                    .stub()
                    .getSequenceUpdates(request, new ManagedClientResponseObserver<>(observer));
        } catch (Throwable error) {
            final var translated = OxiaStatusException.toException(error);
            executor.execute(() -> observer.onError(translated));
        }
    }

    @Override
    public void close() throws Exception {
        try {
            managedWriteStream.close();
        } finally {
            connectionManager.close();
        }
    }

    private String getLeader(long shardId, @NonNull AtomicReference<OxiaStatusException> hint) {
        final var retryHint = hint.get();
        return retryHint == null
                ? shardLeaderProvider.apply(shardId)
                : retryHint.getLeaderHint(shardId).orElseGet(() -> shardLeaderProvider.apply(shardId));
    }

    private <T> RetryPolicy<T> getRetryPolicy(@NonNull AtomicReference<OxiaStatusException> hint) {
        return RetryPolicy.<T>builder()
                .handleIf(
                        error -> {
                            final var translated = OxiaStatusException.toException(error);
                            if (translated instanceof OxiaStatusException oxiaError) {
                                hint.set(oxiaError);
                            }
                            return OxiaStatusException.isRetryable(translated);
                        })
                .withBackoff(
                        clientConfig.connectionBackoffMinDelay(), clientConfig.connectionBackoffMaxDelay())
                .onRetryScheduled(
                        event ->
                                log.warn()
                                        .exceptionMessage(OxiaStatusException.toException(event.getLastException()))
                                        .log(
                                                "Retrying RPC operation after "
                                                        + event.getDelay()
                                                        + ". attempt="
                                                        + (event.getAttemptCount() + 1)))
                .withMaxAttempts(-1)
                .build();
    }
}
