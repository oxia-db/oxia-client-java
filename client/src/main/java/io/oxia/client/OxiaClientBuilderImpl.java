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
package io.oxia.client;

import static java.time.Duration.ZERO;

import com.google.common.base.Strings;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Authentication;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.OxiaException;
import io.oxia.client.api.exceptions.UnsupportedAuthenticationException;
import io.oxia.client.auth.AuthenticationFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class OxiaClientBuilderImpl implements OxiaClientBuilder {

    public static final Duration DefaultBatchLinger = Duration.ofMillis(5);
    public static final int DefaultMaxRequestsPerBatch = 1000;
    public static final int DefaultMaxBatchSize = 128 * 1024;
    public static final Duration DefaultRequestTimeout = Duration.ofSeconds(30);
    public static final Duration DefaultSessionTimeout = Duration.ofSeconds(15);
    public static final String DefaultNamespace = "default";
    public static final boolean DefaultEnableTls = false;
    public static final int DefaultMaxConnectionPerNode = 1;

    @NonNull protected final String serviceAddress;
    @NonNull protected Duration requestTimeout = DefaultRequestTimeout;
    @NonNull protected Duration batchLinger = DefaultBatchLinger;
    protected int maxRequestsPerBatch = DefaultMaxRequestsPerBatch;
    @NonNull protected Duration sessionTimeout = DefaultSessionTimeout;

    protected String clientIdentifier = randomClientIdentifier();
    @NonNull protected Supplier<String> clientIdentifierSupplier = () -> clientIdentifier;

    @NonNull protected String namespace = DefaultNamespace;
    @NonNull protected OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
    @Nullable protected String authPluginClassName;
    @Nullable protected String authParams;
    @Nullable protected Authentication authentication;
    protected boolean enableTls = DefaultEnableTls;

    @NonNull protected Duration connectionBackoffMinDelay = Duration.ofMillis(100);
    @NonNull protected Duration connectionBackoffMaxDelay = Duration.ofSeconds(30);

    protected Duration connectionKeepAliveTime = Duration.ofSeconds(10);
    protected Duration connectionKeepAliveTimeout = Duration.ofSeconds(5);

    protected int maxConnectionsPerNode = DefaultMaxConnectionPerNode;

    @Override
    public @NonNull OxiaClientBuilder requestTimeout(@NonNull Duration requestTimeout) {
        if (requestTimeout.isNegative() || requestTimeout.equals(ZERO)) {
            throw new IllegalArgumentException(
                    "requestTimeout must be greater than zero: " + requestTimeout);
        }
        this.requestTimeout = requestTimeout;
        return this;
    }

    @Override
    public @NonNull OxiaClientBuilder batchLinger(@NonNull Duration batchLinger) {
        if (batchLinger.isNegative() || batchLinger.equals(ZERO)) {
            throw new IllegalArgumentException("batchLinger must be greater than zero: " + batchLinger);
        }
        this.batchLinger = batchLinger;
        return this;
    }

    @Override
    public @NonNull OxiaClientBuilder maxRequestsPerBatch(int maxRequestsPerBatch) {
        if (maxRequestsPerBatch <= 0) {
            throw new IllegalArgumentException(
                    "MaxRequestsPerBatch must be greater than zero: " + maxRequestsPerBatch);
        }
        this.maxRequestsPerBatch = maxRequestsPerBatch;
        return this;
    }

    @Override
    public @NonNull OxiaClientBuilder namespace(@NonNull String namespace) {
        if (Strings.isNullOrEmpty(namespace)) {
            throw new IllegalArgumentException("namespace must not be null or empty.");
        }
        this.namespace = namespace;
        return this;
    }

    @Override
    public @NonNull OxiaClientBuilder sessionTimeout(@NonNull Duration sessionTimeout) {
        if (sessionTimeout.isNegative() || sessionTimeout.equals(ZERO)) {
            throw new IllegalArgumentException(
                    "SessionTimeout must be greater than zero: " + sessionTimeout);
        }
        this.sessionTimeout = sessionTimeout;
        return this;
    }

    @Override
    public @NonNull OxiaClientBuilder clientIdentifier(@NonNull String clientIdentifier) {
        this.clientIdentifier = clientIdentifier;
        this.clientIdentifierSupplier = () -> clientIdentifier;
        return this;
    }

    @Override
    public @NonNull OxiaClientBuilder clientIdentifier(
            @NonNull Supplier<String> clientIdentifierSupplier) {
        this.clientIdentifierSupplier = clientIdentifierSupplier;
        return this;
    }

    @Override
    public @NonNull OxiaClientBuilder openTelemetry(@NonNull OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
        return this;
    }

    @Override
    public OxiaClientBuilder authentication(Authentication authentication) {
        this.authentication = authentication;
        return this;
    }

    @Override
    public OxiaClientBuilder connectionBackoff(Duration minDelay, Duration maxDelay) {
        this.connectionBackoffMinDelay = minDelay;
        this.connectionBackoffMaxDelay = maxDelay;
        return this;
    }

    @Override
    public OxiaClientBuilder maxConnectionPerNode(int connections) {
        if (connections <= 0) {
            throw new IllegalArgumentException(
                    "maxConnectionPerNode must be greater than zero: " + connections);
        }
        this.maxConnectionsPerNode = connections;
        return this;
    }

    @Override
    public OxiaClientBuilder connectionKeepAliveTimeout(Duration connectionKeepAliveTimeout) {
        this.connectionKeepAliveTimeout = connectionKeepAliveTimeout;
        return this;
    }

    @Override
    public OxiaClientBuilder connectionKeepAliveTime(Duration keepAliveTime) {
        this.connectionKeepAliveTime = keepAliveTime;
        return this;
    }

    @Override
    public OxiaClientBuilder authentication(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException {
        this.authPluginClassName = authPluginClassName;
        this.authParams = authParamsString;
        this.authentication = AuthenticationFactory.create(authPluginClassName, authParamsString);
        return this;
    }

    @Override
    public OxiaClientBuilder enableTls(boolean enableTls) {
        this.enableTls = enableTls;
        return this;
    }

    @Override
    public OxiaClientBuilder loadConfig(String configPath) {
        // Load the configuration from the given path.
        try {
            File configFile = new File(configPath);
            return loadConfig(configFile);
        } catch (Throwable e) {
            throw new IllegalArgumentException(
                    "Failed to load configuration from file: " + configPath, e);
        }
    }

    @Override
    public OxiaClientBuilder loadConfig(File configFile) {
        // Load the configuration from the given file.
        Properties properties = new Properties();
        try (InputStream input = new FileInputStream(configFile)) {
            properties.load(input);
        } catch (IOException ex) {
            throw new IllegalArgumentException(
                    "Failed to load configuration from file: " + configFile, ex);
        }
        return loadConfig(properties);
    }

    @Override
    public OxiaClientBuilder loadConfig(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("Properties must not be null.");
        }
        // Load the configuration from the given properties using reflection.
        for (String name : properties.stringPropertyNames()) {
            try {
                var field = getClass().getDeclaredField(name);
                field.setAccessible(true);
                if (field.getType().equals(Duration.class)) {
                    field.set(this, Duration.ofMillis(Long.parseLong(properties.getProperty(name))));
                } else if (field.getType().equals(int.class)) {
                    field.set(this, Integer.parseInt(properties.getProperty(name)));
                } else if (field.getType().equals(boolean.class)) {
                    field.set(this, Boolean.parseBoolean(properties.getProperty(name)));
                } else {
                    field.set(this, properties.getProperty(name));
                }
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new IllegalArgumentException("Invalid configuration property: " + name);
            }
        }
        // Create the authentication from the configuration.
        try {
            this.authentication = AuthenticationFactory.create(authPluginClassName, authParams);
        } catch (UnsupportedAuthenticationException e) {
            throw new IllegalArgumentException("Failed to create authentication from configuration.", e);
        }
        return this;
    }

    @Override
    public @NonNull CompletableFuture<AsyncOxiaClient> asyncClient() {
        return AsyncOxiaClientImpl.newInstance(getClientConfig());
    }

    public ClientConfig getClientConfig() {
        return new ClientConfig(
                serviceAddress,
                requestTimeout,
                batchLinger,
                maxRequestsPerBatch,
                DefaultMaxBatchSize,
                sessionTimeout,
                clientIdentifierSupplier.get(),
                openTelemetry,
                namespace,
                authentication,
                enableTls,
                connectionBackoffMinDelay,
                connectionBackoffMaxDelay,
                connectionKeepAliveTime,
                connectionKeepAliveTimeout,
                maxConnectionsPerNode);
    }

    @Override
    public SyncOxiaClient syncClient() throws OxiaException {
        return new SyncOxiaClientImpl(asyncClient().join());
    }

    private static @NonNull String randomClientIdentifier() {
        return "oxia-client-java:" + UUID.randomUUID();
    }
}
