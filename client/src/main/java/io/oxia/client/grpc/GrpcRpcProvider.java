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
import dev.failsafe.Timeout;
import io.github.merlimat.slog.Logger;
import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.client.grpc.observer.ManagedObservers;
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
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import lombok.NonNull;

final class GrpcRpcProvider implements RpcProvider {
    private static final Logger log = Logger.get(GrpcRpcProvider.class);

    private final ClientConfig clientConfig;
    private final ConnectionManager connectionManager;
    private final ScheduledExecutorService asyncExecutor;
    private final LongFunction<String> shardLeaderProvider;
    private final ManagedWriteStream managedWriteStream;

    GrpcRpcProvider(
            @NonNull ClientConfig clientConfig,
            @NonNull ScheduledExecutorService asyncExecutor,
            @NonNull LongFunction<String> shardLeaderProvider) {
        this.clientConfig = clientConfig;
        this.asyncExecutor = asyncExecutor;
        this.connectionManager = new ConnectionManager(clientConfig, asyncExecutor);
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
        final var guardedObserver = ManagedObservers.toGuardedStreamObserver(observer);
        try {
            Failsafe.with(getRetryPolicy("get shard assignments", null))
                    .with(asyncExecutor)
                    .getStageAsync(
                            () -> {
                                final var barrierFuture = new CompletableFuture<Void>();
                                final var barrierObserver =
                                        ManagedObservers.toBarrierStreamObserver(guardedObserver, barrierFuture);
                                try {
                                    connectionManager
                                            .getConnection(clientConfig.serviceAddress())
                                            .stub()
                                            .getShardAssignments(request, barrierObserver);
                                } catch (Throwable error) {
                                    barrierFuture.completeExceptionally(OxiaStatusException.toException(error));
                                }
                                return barrierFuture;
                            })
                    .exceptionally(
                            error -> {
                                guardedObserver.onError(OxiaStatusException.toException(error));
                                return null;
                            });
        } catch (Throwable error) {
            guardedObserver.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public void getNotifications(
            @NonNull NotificationsRequest request, @NonNull StreamObserver<NotificationBatch> observer) {
        final var guardedObserver = ManagedObservers.toGuardedStreamObserver(observer);
        final var hint = new AtomicReference<OxiaStatusException>();
        try {
            Failsafe.with(getRetryPolicy("get notifications", hint))
                    .with(asyncExecutor)
                    .getStageAsync(
                            () -> {
                                final var barrierFuture = new CompletableFuture<Void>();
                                final var barrierObserver =
                                        ManagedObservers.toBarrierStreamObserver(guardedObserver, barrierFuture);
                                try {
                                    connectionManager
                                            .getConnection(getLeader(request.getShard(), hint))
                                            .stub()
                                            .getNotifications(request, barrierObserver);
                                } catch (Throwable error) {
                                    barrierFuture.completeExceptionally(OxiaStatusException.toException(error));
                                }
                                return barrierFuture;
                            })
                    .exceptionally(
                            error -> {
                                guardedObserver.onError(OxiaStatusException.toException(error));
                                return null;
                            });
        } catch (Throwable error) {
            guardedObserver.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public CompletableFuture<CreateSessionResponse> createSession(
            @NonNull CreateSessionRequest request) {
        final var hint = new AtomicReference<OxiaStatusException>();
        return Failsafe.with(getRetryPolicy("create session", hint))
                .with(asyncExecutor)
                .getStageAsync(
                        () -> {
                            final var future = new CompletableFuture<CreateSessionResponse>();
                            try {
                                connectionManager
                                        .getConnection(getLeader(request.getShard(), hint))
                                        .stub()
                                        .withDeadlineAfter(
                                                clientConfig.requestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                                        .createSession(request, ManagedObservers.toCompletableFuture(future));
                            } catch (Throwable error) {
                                future.completeExceptionally(OxiaStatusException.toException(error));
                            }
                            return future;
                        });
    }

    @Override
    public CompletableFuture<KeepAliveResponse> keepAlive(
            @NonNull SessionHeartbeat heartbeat, @NonNull Duration timeout) {
        final var hint = new AtomicReference<OxiaStatusException>();
        return Failsafe.with(Timeout.of(timeout), getRetryPolicy("keep alive", hint))
                .with(asyncExecutor)
                .getStageAsync(
                        () -> {
                            final var future = new CompletableFuture<KeepAliveResponse>();
                            try {
                                connectionManager
                                        .getConnection(getLeader(heartbeat.getShard(), hint))
                                        .stub()
                                        .withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS)
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
        return Failsafe.with(getRetryPolicy("close session", hint))
                .with(asyncExecutor)
                .getStageAsync(
                        () -> {
                            final var future = new CompletableFuture<CloseSessionResponse>();
                            try {
                                connectionManager
                                        .getConnection(getLeader(request.getShard(), hint))
                                        .stub()
                                        .withDeadlineAfter(
                                                clientConfig.requestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                                        .closeSession(request, ManagedObservers.toCompletableFuture(future));
                            } catch (Throwable error) {
                                future.completeExceptionally(OxiaStatusException.toException(error));
                            }
                            return future;
                        });
    }

    @Override
    public void read(@NonNull ReadRequest request, @NonNull StreamObserver<ReadResponse> observer) {
        final var guardedObserver = ManagedObservers.toGuardedStreamObserver(observer);
        final var hint = new AtomicReference<OxiaStatusException>();
        try {
            Failsafe.with(getRetryPolicy("read", hint))
                    .with(asyncExecutor)
                    .getStageAsync(
                            () -> {
                                final var barrierFuture = new CompletableFuture<Void>();
                                final var barrierObserver =
                                        ManagedObservers.toBarrierStreamObserver(guardedObserver, barrierFuture);
                                try {
                                    connectionManager
                                            .getConnection(getLeader(request.getShard(), hint))
                                            .stub()
                                            .read(request, barrierObserver);
                                } catch (Throwable error) {
                                    barrierFuture.completeExceptionally(OxiaStatusException.toException(error));
                                }
                                return barrierFuture;
                            })
                    .exceptionally(
                            error -> {
                                guardedObserver.onError(OxiaStatusException.toException(error));
                                return null;
                            });
        } catch (Throwable error) {
            guardedObserver.onError(OxiaStatusException.toException(error));
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
        final var hint = new AtomicReference<OxiaStatusException>();
        try {
            Failsafe.with(getRetryPolicy("list", hint))
                    .with(asyncExecutor)
                    .getStageAsync(
                            () -> {
                                final var barrierFuture = new CompletableFuture<Void>();
                                final var barrierObserver =
                                        ManagedObservers.toBarrierClientResponseObserver(observer, barrierFuture);
                                try {
                                    connectionManager
                                            .getConnection(getLeader(request.getShard(), hint))
                                            .stub()
                                            .list(request, barrierObserver);
                                } catch (Throwable error) {
                                    barrierFuture.completeExceptionally(OxiaStatusException.toException(error));
                                }
                                return barrierFuture;
                            })
                    .exceptionally(
                            error -> {
                                observer.onError(OxiaStatusException.toException(error));
                                return null;
                            });
        } catch (Throwable error) {
            observer.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public void rangeScan(
            @NonNull RangeScanRequest request,
            @NonNull CancelableStreamObserver<RangeScanResponse> observer) {
        final var hint = new AtomicReference<OxiaStatusException>();
        try {
            Failsafe.with(getRetryPolicy("range scan", hint))
                    .with(asyncExecutor)
                    .getStageAsync(
                            () -> {
                                final var barrierFuture = new CompletableFuture<Void>();
                                final var barrierObserver =
                                        ManagedObservers.toBarrierClientResponseObserver(observer, barrierFuture);
                                try {
                                    connectionManager
                                            .getConnection(getLeader(request.getShard(), hint))
                                            .stub()
                                            .rangeScan(request, barrierObserver);
                                } catch (Throwable error) {
                                    barrierFuture.completeExceptionally(OxiaStatusException.toException(error));
                                }
                                return barrierFuture;
                            })
                    .exceptionally(
                            error -> {
                                observer.onError(OxiaStatusException.toException(error));
                                return null;
                            });
        } catch (Throwable error) {
            observer.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public void getSequenceUpdates(
            @NonNull GetSequenceUpdatesRequest request,
            @NonNull CancelableStreamObserver<GetSequenceUpdatesResponse> observer) {
        final var hint = new AtomicReference<OxiaStatusException>();
        try {
            Failsafe.with(getRetryPolicy("get sequence updates", hint))
                    .with(asyncExecutor)
                    .getStageAsync(
                            () -> {
                                final var barrierFuture = new CompletableFuture<Void>();
                                final var barrierObserver =
                                        ManagedObservers.toBarrierClientResponseObserver(observer, barrierFuture);
                                try {
                                    connectionManager
                                            .getConnection(getLeader(request.getShard(), hint))
                                            .stub()
                                            .getSequenceUpdates(request, barrierObserver);
                                } catch (Throwable error) {
                                    barrierFuture.completeExceptionally(OxiaStatusException.toException(error));
                                }
                                return barrierFuture;
                            })
                    .exceptionally(
                            error -> {
                                observer.onError(OxiaStatusException.toException(error));
                                return null;
                            });
        } catch (Throwable error) {
            observer.onError(OxiaStatusException.toException(error));
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

    private <T> RetryPolicy<T> getRetryPolicy(
            @NonNull String operation, AtomicReference<OxiaStatusException> hint) {
        return RetryPolicy.<T>builder()
                .handleIf(
                        error -> {
                            final var translated = OxiaStatusException.toException(error);
                            if (hint != null && translated instanceof OxiaStatusException oxiaError) {
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
                                                "Retrying "
                                                        + operation
                                                        + " after "
                                                        + event.getDelay()
                                                        + ". attempt="
                                                        + (event.getAttemptCount() + 1)))
                .withMaxAttempts(-1)
                .build();
    }
}
