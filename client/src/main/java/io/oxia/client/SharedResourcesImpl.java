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
package io.oxia.client;

import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.Authentication;
import io.oxia.client.api.SharedResources;
import io.oxia.client.grpc.ConnectionManager;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.shard.ShardManager;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;

/**
 * Default {@link SharedResources} implementation.
 *
 * <p>Owns the resources that can be shared by many client instances in the same JVM:
 *
 * <ul>
 *   <li>a single background {@link ScheduledExecutorService};
 *   <li>a single {@link ConnectionManager} (gRPC channel pool) — namespaces are applied per-RPC, so
 *       connections are namespace-agnostic and shared across all clients;
 *   <li>one shard-assignment stream ({@link ShardManager}) per {@code (serviceAddress, namespace)}.
 * </ul>
 *
 * <p>Transport-level settings (TLS, authentication, keep-alive, connection pool size) are governed
 * by this object and configured through its {@link Builder}. When a client is built with {@link
 * io.oxia.client.api.OxiaClientBuilder#sharedResources(SharedResources)}, setting any of them on
 * that client is rejected; per-client settings such as namespace, client identifier, session and
 * request timeouts still apply.
 */
public class SharedResourcesImpl implements SharedResources {
    private static final Logger log = Logger.get(SharedResourcesImpl.class);

    private final ScheduledExecutorService executor;
    private final ConnectionManager connectionManager;
    private final OpenTelemetry openTelemetry;
    private final Map<NamespaceKey, SharedNamespace> namespaces = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public static SharedResources.Builder builder() {
        return new Builder();
    }

    SharedResourcesImpl(int numWorkerThreads, @NonNull ClientConfig transportConfig) {
        this.executor =
                Executors.newScheduledThreadPool(
                        numWorkerThreads, new DefaultThreadFactory("oxia-client-shared"));
        this.connectionManager = new ConnectionManager(transportConfig, executor);
        this.openTelemetry = transportConfig.openTelemetry();
    }

    ScheduledExecutorService executor() {
        return executor;
    }

    ConnectionManager connectionManager() {
        return connectionManager;
    }

    /** The number of currently-open shared connections. Exposed to demonstrate connection sharing. */
    @VisibleForTesting
    public int getConnectionCount() {
        return connectionManager.getConnectionCount();
    }

    /** The number of distinct {@code (serviceAddress, namespace)} shard managers currently held. */
    @VisibleForTesting
    int namespaceCount() {
        return namespaces.size();
    }

    /**
     * Return the shared shard-assignment manager for the {@code (serviceAddress, namespace)} of the
     * given config, creating and starting it on first use. The returned future completes once the
     * initial shard assignments have been received.
     */
    CompletableFuture<ShardManager> getOrCreateShardManager(@NonNull ClientConfig config) {
        if (closed) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("SharedResources has been closed"));
        }
        var key = new NamespaceKey(config.serviceAddress(), config.namespace());
        var ns = namespaces.computeIfAbsent(key, k -> createNamespace(config));
        return ns.started().thenApply(v -> ns.shardManager());
    }

    private SharedNamespace createNamespace(@NonNull ClientConfig config) {
        // The shard-assignment stream is shared, so attribute its metrics to the pool's OpenTelemetry
        // rather than to whichever client happened to create the namespace first.
        var instrumentProvider = new InstrumentProvider(openTelemetry, config.namespace());
        var shardManagerRef = new AtomicReference<ShardManager>();
        var rpcProvider =
                RpcProvider.create(
                        config, executor, connectionManager, shardId -> shardManagerRef.get().leader(shardId));
        var shardManager =
                new ShardManager(executor, rpcProvider, instrumentProvider, config.namespace());
        shardManagerRef.set(shardManager);
        return new SharedNamespace(rpcProvider, shardManager, shardManager.start());
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // Close the shard managers first (stops assignment-stream retries), then their RpcProviders,
        // then the shared connection pool and finally the executor.
        for (SharedNamespace ns : namespaces.values()) {
            ns.shardManager().close();
            try {
                ns.rpcProvider().close();
            } catch (Exception e) {
                log.warn().exception(e).log("Failed to close shared shard RpcProvider");
            }
        }
        namespaces.clear();
        try {
            connectionManager.close();
        } catch (Exception e) {
            log.warn().exception(e).log("Failed to close shared connection manager");
        }
        executor.shutdownNow();
    }

    record NamespaceKey(String serviceAddress, String namespace) {}

    private record SharedNamespace(
            RpcProvider rpcProvider, ShardManager shardManager, CompletableFuture<Void> started) {}

    /** Builder for {@link SharedResourcesImpl}. */
    public static final class Builder implements SharedResources.Builder {
        private int numWorkerThreads = Runtime.getRuntime().availableProcessors();
        private boolean enableTls = OxiaClientBuilderImpl.DefaultEnableTls;
        private Authentication authentication;
        private Duration connectionKeepAliveTime = Duration.ofSeconds(10);
        private Duration connectionKeepAliveTimeout = Duration.ofSeconds(3);
        private Duration connectionBackoffMinDelay = Duration.ofMillis(100);
        private Duration connectionBackoffMaxDelay = Duration.ofSeconds(30);
        private int maxConnectionPerNode = OxiaClientBuilderImpl.DefaultMaxConnectionPerNode;
        private OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();

        @Override
        public SharedResources.Builder numWorkerThreads(int numWorkerThreads) {
            if (numWorkerThreads <= 0) {
                throw new IllegalArgumentException(
                        "numWorkerThreads must be greater than zero: " + numWorkerThreads);
            }
            this.numWorkerThreads = numWorkerThreads;
            return this;
        }

        @Override
        public SharedResources.Builder enableTls(boolean enableTls) {
            this.enableTls = enableTls;
            return this;
        }

        @Override
        public SharedResources.Builder authentication(Authentication authentication) {
            this.authentication = authentication;
            return this;
        }

        @Override
        public SharedResources.Builder connectionKeepAliveTime(@NonNull Duration keepAliveTime) {
            this.connectionKeepAliveTime = keepAliveTime;
            return this;
        }

        @Override
        public SharedResources.Builder connectionKeepAliveTimeout(@NonNull Duration keepAliveTimeout) {
            this.connectionKeepAliveTimeout = keepAliveTimeout;
            return this;
        }

        @Override
        public SharedResources.Builder connectionBackoff(
                @NonNull Duration minDelay, @NonNull Duration maxDelay) {
            this.connectionBackoffMinDelay = minDelay;
            this.connectionBackoffMaxDelay = maxDelay;
            return this;
        }

        @Override
        public SharedResources.Builder maxConnectionPerNode(int connections) {
            if (connections <= 0) {
                throw new IllegalArgumentException(
                        "maxConnectionPerNode must be greater than zero: " + connections);
            }
            this.maxConnectionPerNode = connections;
            return this;
        }

        @Override
        public SharedResources.Builder openTelemetry(@NonNull OpenTelemetry openTelemetry) {
            this.openTelemetry = openTelemetry;
            return this;
        }

        @Override
        public SharedResources build() {
            var transportConfig =
                    new ClientConfig(
                            "shared",
                            OxiaClientBuilderImpl.DefaultRequestTimeout,
                            OxiaClientBuilderImpl.DefaultMaxRequestsPerBatch,
                            OxiaClientBuilderImpl.DefaultMaxBatchSize,
                            OxiaClientBuilderImpl.DefaultMaxPendingBytes,
                            OxiaClientBuilderImpl.DefaultSessionTimeout,
                            "oxia-shared-resources",
                            openTelemetry,
                            OxiaClientBuilderImpl.DefaultNamespace,
                            authentication,
                            enableTls,
                            connectionBackoffMinDelay,
                            connectionBackoffMaxDelay,
                            connectionKeepAliveTime,
                            connectionKeepAliveTimeout,
                            maxConnectionPerNode);
            return new SharedResourcesImpl(numWorkerThreads, transportConfig);
        }
    }
}
