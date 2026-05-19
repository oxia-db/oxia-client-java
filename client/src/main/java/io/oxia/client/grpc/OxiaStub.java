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
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.github.merlimat.slog.Logger;
import io.grpc.CallCredentials;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.TlsChannelCredentials;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
import io.oxia.client.api.Authentication;
import io.oxia.proto.CloseSessionRequest;
import io.oxia.proto.CloseSessionResponse;
import io.oxia.proto.OxiaClientGrpc;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.NonNull;

public class OxiaStub implements AutoCloseable, StreamObserver<HealthCheckResponse> {
    public static String TLS_SCHEMA = "tls://";

    private static final Logger log = Logger.get(OxiaStub.class);

    private final ManagedChannel channel;
    private final @NonNull OxiaClientGrpc.OxiaClientStub asyncStub;
    private final @NonNull HealthGrpc.HealthStub healthStub;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean healthCheckInProgress = new AtomicBoolean();
    private final @Nullable ScheduledFuture<?> healthCheckTask;
    private final @Nullable HealthCheckFailureCallback healthCheckFailureCallback;

    static String getAddress(String address) {
        if (address.startsWith(TLS_SCHEMA)) {
            return address.substring(TLS_SCHEMA.length());
        }
        return address;
    }

    static ChannelCredentials getChannelCredential(String address, boolean tlsEnabled) {
        return tlsEnabled || address.startsWith(TLS_SCHEMA)
                ? TlsChannelCredentials.newBuilder().build()
                : InsecureChannelCredentials.create();
    }

    static ManagedChannel newChannel(String address, ClientConfig clientConfig) {
        return Grpc.newChannelBuilder(
                        getAddress(address), getChannelCredential(address, clientConfig.enableTls()))
                .keepAliveTime(clientConfig.connectionKeepAliveTime().toMillis(), MILLISECONDS)
                .keepAliveTimeout(clientConfig.connectionKeepAliveTimeout().toMillis(), MILLISECONDS)
                .keepAliveWithoutCalls(true)
                .disableRetry()
                .directExecutor()
                .build();
    }

    OxiaStub(
            @NonNull String address,
            @NonNull ClientConfig clientConfig,
            @NonNull ScheduledExecutorService executor,
            @NonNull HealthCheckFailureCallback healthCheckFailureCallback) {
        this.channel = newChannel(address, clientConfig);
        this.healthStub = newHealthStub(channel, clientConfig.authentication());
        this.asyncStub = newAsyncStub(channel, clientConfig.authentication());
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
                                healthStub
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

    public OxiaStub(ManagedChannel channel, @Nullable final Authentication authentication) {
        this.channel = channel;
        this.healthStub = newHealthStub(channel, authentication);
        this.asyncStub = newAsyncStub(channel, authentication);
        this.healthCheckFailureCallback = null;
        this.healthCheckTask = null;
    }

    private static HealthGrpc.HealthStub newHealthStub(
            ManagedChannel channel, @Nullable final Authentication authentication) {
        var stub = HealthGrpc.newStub(channel);
        var credentials = newCallCredentials(authentication);
        return credentials != null ? stub.withCallCredentials(credentials) : stub;
    }

    private static OxiaClientGrpc.OxiaClientStub newAsyncStub(
            ManagedChannel channel, @Nullable final Authentication authentication) {
        var stub =
                OxiaClientGrpc.newStub(channel)
                        .withMaxInboundMessageSize(MAXIMUM_FRAME_SIZE)
                        .withMaxOutboundMessageSize(MAXIMUM_FRAME_SIZE);
        var credentials = newCallCredentials(authentication);
        return credentials != null ? stub.withCallCredentials(credentials) : stub;
    }

    private static @Nullable CallCredentials newCallCredentials(
            @Nullable final Authentication authentication) {
        if (authentication == null) {
            return null;
        }
        return new CallCredentials() {
            @Override
            public void applyRequestMetadata(
                    RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
                Metadata credentials = new Metadata();
                authentication
                        .generateCredentials()
                        .forEach(
                                (key, value) ->
                                        credentials.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value));
                applier.apply(credentials);
            }
        };
    }

    public OxiaClientGrpc.OxiaClientStub async() {
        return asyncStub;
    }

    public CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request) {
        final CompletableFuture<CloseSessionResponse> f = new CompletableFuture<>();
        final var defer =
                new StreamObserver<CloseSessionResponse>() {
                    private CloseSessionResponse response;

                    @Override
                    public void onNext(CloseSessionResponse response) {
                        this.response = response;
                    }

                    @Override
                    public void onError(Throwable t) {
                        f.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        f.complete(response);
                    }
                };
        try {
            asyncStub.closeSession(request, defer);
        } catch (Throwable ex) {
            f.completeExceptionally(ex);
        }
        return f;
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
