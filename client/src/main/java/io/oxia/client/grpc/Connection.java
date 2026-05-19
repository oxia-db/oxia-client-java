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

import static io.oxia.client.constants.Constants.MAXIMUM_FRAME_SIZE;
import static io.oxia.client.grpc.ConnectionUtils.getAddress;
import static io.oxia.client.grpc.ConnectionUtils.getChannelCredential;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.github.merlimat.slog.Logger;
import io.grpc.CallCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
import io.oxia.proto.OxiaClientGrpc;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.NonNull;

class Connection implements AutoCloseable, StreamObserver<HealthCheckResponse> {
    private static final Logger log = Logger.get(Connection.class);

    private final ManagedChannel channel;
    private final @NonNull OxiaClientGrpc.OxiaClientStub asyncStub;
    private final @NonNull HealthGrpc.HealthStub healthStub;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean healthCheckInProgress = new AtomicBoolean();
    private final @Nullable ScheduledFuture<?> healthCheckTask;
    private final @Nullable HealthCheckFailureCallback healthCheckFailureCallback;

    Connection(
            @NonNull String address,
            @NonNull ClientConfig clientConfig,
            @NonNull ScheduledExecutorService executor,
            @NonNull HealthCheckFailureCallback healthCheckFailureCallback) {
        this.channel =
                Grpc.newChannelBuilder(
                                getAddress(address), getChannelCredential(address, clientConfig.enableTls()))
                        .keepAliveTime(clientConfig.connectionKeepAliveTime().toMillis(), MILLISECONDS)
                        .keepAliveTimeout(clientConfig.connectionKeepAliveTimeout().toMillis(), MILLISECONDS)
                        .keepAliveWithoutCalls(true)
                        .disableRetry()
                        .directExecutor()
                        .build();
        CallCredentials credentials = null;
        var authentication = clientConfig.authentication();
        if (authentication != null) {
            credentials =
                    new CallCredentials() {
                        @Override
                        public void applyRequestMetadata(
                                RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
                            Metadata metadata = new Metadata();
                            authentication
                                    .generateCredentials()
                                    .forEach(
                                            (key, value) ->
                                                    metadata.put(
                                                            Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value));
                            applier.apply(metadata);
                        }
                    };
        }
        var baseHealthStub = HealthGrpc.newStub(channel);
        this.healthStub =
                credentials != null ? baseHealthStub.withCallCredentials(credentials) : baseHealthStub;
        var asyncStub =
                OxiaClientGrpc.newStub(channel)
                        .withMaxInboundMessageSize(MAXIMUM_FRAME_SIZE)
                        .withMaxOutboundMessageSize(MAXIMUM_FRAME_SIZE);
        this.asyncStub = credentials != null ? asyncStub.withCallCredentials(credentials) : asyncStub;
        this.healthCheckFailureCallback = healthCheckFailureCallback;
        this.healthCheckTask =
                executor.scheduleWithFixedDelay(
                        () -> {
                            if (closed.get()) {
                                return;
                            }
                            if (!healthCheckInProgress.compareAndSet(false, true)) {
                                return;
                            }
                            try {
                                this.healthStub
                                        .withDeadlineAfter(
                                                clientConfig.connectionKeepAliveTimeout().toMillis(), MILLISECONDS)
                                        .check(HealthCheckRequest.newBuilder().build(), this);
                            } catch (RuntimeException e) {
                                onError(e);
                            }
                        },
                        clientConfig.connectionKeepAliveTime().toMillis(),
                        clientConfig.connectionKeepAliveTime().toMillis(),
                        TimeUnit.MILLISECONDS);
    }

    @NonNull
    OxiaClientGrpc.OxiaClientStub stub() {
        return asyncStub;
    }

    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (healthCheckTask != null) {
            healthCheckTask.cancel(true);
        }
        channel.shutdown();
        try {
            if (!channel.awaitTermination(100, MILLISECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            log.warn("Interrupted while closing GRPC channel");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void onNext(HealthCheckResponse response) {
        if (response.getStatus() == ServingStatus.SERVING) {
            return;
        }
        try {
            if (closed.get()) {
                return;
            }
            log.warn().attr("status", response.getStatus()).log("Connection health check failed");
            if (healthCheckFailureCallback != null) {
                healthCheckFailureCallback.onFailure(this);
            }
        } finally {
            healthCheckInProgress.set(false);
        }
    }

    @Override
    public void onError(Throwable error) {
        try {
            if (closed.get()) {
                return;
            }
            if (Status.fromThrowable(error).getCode() == Status.Code.UNIMPLEMENTED) {
                log.debug("Connection health ping is unsupported");
                if (healthCheckTask != null) {
                    healthCheckTask.cancel(false);
                }
                return;
            }
            log.warn().exception(error).log("Connection health check failed");
            if (healthCheckFailureCallback != null) {
                healthCheckFailureCallback.onFailure(this);
            }
        } finally {
            healthCheckInProgress.set(false);
        }
    }

    @Override
    public void onCompleted() {
        healthCheckInProgress.set(false);
    }
}
