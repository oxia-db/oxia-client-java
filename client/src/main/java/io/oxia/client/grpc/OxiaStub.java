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
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
import io.oxia.client.api.Authentication;
import io.oxia.proto.CloseSessionRequest;
import io.oxia.proto.CloseSessionResponse;
import io.oxia.proto.OxiaClientGrpc;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import lombok.NonNull;

public class OxiaStub implements AutoCloseable {
    public static String TLS_SCHEMA = "tls://";
    private final ManagedChannel channel;
    private final @NonNull OxiaClientGrpc.OxiaClientStub asyncStub;
    private final HealthGrpc.HealthBlockingStub healthStub;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean disconnected = new AtomicBoolean(false);
    private final AtomicBoolean healthCheckStarted = new AtomicBoolean(false);
    private final @Nullable Consumer<OxiaStub> connectionFailureCallback;
    private volatile @Nullable Thread healthCheckThread;
    private final @Nullable Duration healthCheckInterval;
    private final @Nullable Duration healthCheckTimeout;
    private final Logger log;

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

    public OxiaStub(String address, ClientConfig clientConfig) {
        this(address, clientConfig, null);
    }

    OxiaStub(
            String address,
            ClientConfig clientConfig,
            @Nullable Consumer<OxiaStub> connectionFailureCallback) {
        this(
                Grpc.newChannelBuilder(
                                getAddress(address), getChannelCredential(address, clientConfig.enableTls()))
                        .keepAliveTime(clientConfig.connectionKeepAliveTime().toMillis(), MILLISECONDS)
                        .keepAliveTimeout(clientConfig.connectionKeepAliveTimeout().toMillis(), MILLISECONDS)
                        .keepAliveWithoutCalls(true)
                        .disableRetry()
                        .directExecutor()
                        .build(),
                clientConfig.authentication(),
                connectionFailureCallback,
                clientConfig.connectionKeepAliveTime(),
                clientConfig.connectionKeepAliveTimeout(),
                address);
    }

    public OxiaStub(ManagedChannel channel) {
        this(channel, null);
    }

    public OxiaStub(ManagedChannel channel, @Nullable final Authentication authentication) {
        this(channel, authentication, null, null, null, "managed-channel");
    }

    private OxiaStub(
            ManagedChannel channel,
            @Nullable final Authentication authentication,
            @Nullable Consumer<OxiaStub> connectionFailureCallback,
            @Nullable Duration healthCheckInterval,
            @Nullable Duration healthCheckTimeout,
            String logAddress) {
        this.channel = channel;
        this.connectionFailureCallback = connectionFailureCallback;
        this.healthCheckInterval = healthCheckInterval;
        this.healthCheckTimeout = healthCheckTimeout;
        this.log = Logger.get(OxiaStub.class).with().attr("address", logAddress).build();

        final var callCredentials = authentication != null ? getCallCredentials(authentication) : null;
        OxiaClientGrpc.OxiaClientStub oxiaStub =
                OxiaClientGrpc.newStub(channel)
                        .withMaxInboundMessageSize(MAXIMUM_FRAME_SIZE)
                        .withMaxOutboundMessageSize(MAXIMUM_FRAME_SIZE);
        HealthGrpc.HealthBlockingStub healthStub = HealthGrpc.newBlockingStub(channel);
        if (callCredentials != null) {
            oxiaStub = oxiaStub.withCallCredentials(callCredentials);
            healthStub = healthStub.withCallCredentials(callCredentials);
        }
        this.asyncStub = oxiaStub;
        this.healthStub = healthStub;
        this.healthCheckThread = null;
    }

    private static CallCredentials getCallCredentials(@NonNull Authentication authentication) {
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

    void startHealthCheck() {
        if (connectionFailureCallback == null
                || healthCheckInterval == null
                || healthCheckTimeout == null
                || !healthCheckStarted.compareAndSet(false, true)) {
            return;
        }

        final var thread = new Thread(this::healthCheckLoop, "oxia-grpc-health-check");
        thread.setDaemon(true);
        healthCheckThread = thread;
        thread.start();
    }

    private void healthCheckLoop() {
        while (!closed.get()) {
            if (!healthCheck()) {
                return;
            }

            try {
                Thread.sleep(healthCheckInterval.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean healthCheck() {
        try {
            final var response =
                    healthStub
                            .withDeadlineAfter(healthCheckTimeout.toMillis(), MILLISECONDS)
                            .check(HealthCheckRequest.newBuilder().build());
            if (response.getStatus() == HealthCheckResponse.ServingStatus.SERVING) {
                return true;
            }
            log.warn()
                    .attr("status", response.getStatus())
                    .log("Connection health check returned unhealthy status");
            notifyConnectionFailure();
            return false;
        } catch (Throwable t) {
            if (closed.get()) {
                return false;
            }
            if (Status.fromThrowable(t).getCode() == Status.Code.UNIMPLEMENTED) {
                log.debug("Connection health check is unsupported");
                return false;
            }
            log.warn().exceptionMessage(t).log("Connection health check failed");
            notifyConnectionFailure();
            return false;
        }
    }

    private void notifyConnectionFailure() {
        if (connectionFailureCallback != null && disconnected.compareAndSet(false, true)) {
            connectionFailureCallback.accept(this);
        }
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
        final var thread = healthCheckThread;
        if (thread != null && Thread.currentThread() != thread) {
            thread.interrupt();
        }
        channel.shutdown();
        try {
            if (!channel.awaitTermination(100, MILLISECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
        if (thread != null && Thread.currentThread() != thread) {
            try {
                thread.join(MILLISECONDS.toMillis(100));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
