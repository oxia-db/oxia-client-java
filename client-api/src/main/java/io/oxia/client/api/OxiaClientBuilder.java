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
package io.oxia.client.api;

import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.exceptions.OxiaException;
import io.oxia.client.api.exceptions.UnsupportedAuthenticationException;
import io.oxia.client.internal.DefaultImplementation;
import java.io.File;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Builder for {@link SyncOxiaClient} and {@link AsyncOxiaClient}. */
public interface OxiaClientBuilder {

    /**
     * Create a new client builder.
     *
     * @param serviceAddress the address of the Oxia server
     * @return the client builder instance
     */
    static OxiaClientBuilder create(String serviceAddress) {
        return DefaultImplementation.getDefaultImplementation(serviceAddress);
    }

    /**
     * Create a synchronous client.
     *
     * <p>This method is blocking while the client initializes and talks to the server.
     *
     * @return the synchronous client instance
     * @throws OxiaException if failed to create the client
     */
    SyncOxiaClient syncClient() throws OxiaException;

    /**
     * Create an asynchronous client.
     *
     * @return a future to retrieve the client instance, when ready
     */
    CompletableFuture<AsyncOxiaClient> asyncClient();

    /**
     * Specify a custom timeout for all requests.
     *
     * <p>Default is <code>30 secs</code>.
     *
     * @param requestTimeout the timeout duration
     * @return the builder instance
     */
    OxiaClientBuilder requestTimeout(Duration requestTimeout);

    /**
     * Specify the maximum time to wait for a batch to be sent.
     *
     * <p>Default is <code>5 millis</code>
     *
     * <p>A larger linger time might help in creating batches with more ops in them and thus might
     * improve the overall throughput.
     *
     * <p>A shorter linger time might help in minimizing the latency.
     *
     * @param batchLinger the batch linger duration
     * @return the builder instance
     */
    OxiaClientBuilder batchLinger(Duration batchLinger);

    /**
     * Specify the maximum number of requests to include in a batch.
     *
     * <p>Default is <code>1000</code>.
     *
     * @param maxRequestsPerBatch the maximum number of requests per batch
     * @return the builder instance
     */
    OxiaClientBuilder maxRequestsPerBatch(int maxRequestsPerBatch);

    /**
     * Specify the Oxia namesace to use for this client instance.
     *
     * <p>Default is <code>"default"</code>.
     *
     * @param namespace the namespace to use
     * @return the builder instance
     */
    OxiaClientBuilder namespace(String namespace);

    /**
     * Specify the session timeout for this client instance.
     *
     * <p>Default is <code>30 secs</code>.
     *
     * @see <a href="https://oxia-db.github.io/docs/features/ephemerals">Oxia Ephemeral Records</a>
     * @param sessionTimeout the session timeout duration
     * @return the builder instance
     */
    OxiaClientBuilder sessionTimeout(Duration sessionTimeout);

    /**
     * Specify the client identifier for this client instance.
     *
     * <p>Default is a random UUID.
     *
     * @param clientIdentifier the client identifier
     * @return the builder instance
     * @see <a
     *     href="https://oxia-db.github.io/docs/features/ephemerals#session-id-and-client-identity">Oxia
     *     Client Identity</a>
     */
    OxiaClientBuilder clientIdentifier(String clientIdentifier);

    /**
     * Specify the client identifier for this client instance.
     *
     * <p>Default is a random UUID.
     *
     * @param clientIdentifier the client identifier supplier
     * @return the builder instance
     * @see <a
     *     href="https://oxia-db.github.io/docs/features/ephemerals#session-id-and-client-identity">Oxia
     *     Client Identity</a>
     */
    OxiaClientBuilder clientIdentifier(Supplier<String> clientIdentifier);

    /**
     * Specify the OpenTelemetry instance to use for this client instance.
     *
     * <p>By default, the client will use the global OpenTelemetry instance if available.
     *
     * @param openTelemetry an OpenTelemetry instance
     * @return the builder instance
     */
    OxiaClientBuilder openTelemetry(OpenTelemetry openTelemetry);

    /**
     * Configure the authentication plugin and its parameters.
     *
     * @param authentication the authentication instance
     * @return the builder instance
     */
    OxiaClientBuilder authentication(Authentication authentication);

    /**
     * Configure the connection backoff policy.
     *
     * <p>Defaults are <code>min=100millis, max=30sec</code>.
     *
     * @param minDelay the minimum delay between retries
     * @param maxDelay the maximum delay between retries
     * @return the builder instance
     */
    OxiaClientBuilder connectionBackoff(Duration minDelay, Duration maxDelay);

    /**
     * Configure the maximum number of connections to each Oxia server node.
     *
     * <p>Default is <code>1</code>.
     *
     * @param connections the maximum number of connections
     * @return the builder instance
     */
    OxiaClientBuilder maxConnectionPerNode(int connections);

    /**
     * Configure the keep alive timeout for the connection.
     *
     * <p>Default is <code>5 sec</code>.
     *
     * @param connectionKeepAliveTimeout the keep alive timeout duration
     * @return the builder instance
     */
    OxiaClientBuilder connectionKeepAliveTimeout(Duration connectionKeepAliveTimeout);

    /**
     * Configure the keep alive interval for the connection.
     *
     * <p>Default is <code>10 sec</code>.
     *
     * @param connectionKeepAlive the keep alive interval duration
     * @return the builder instance
     */
    OxiaClientBuilder connectionKeepAliveTime(Duration connectionKeepAlive);

    /**
     * Configure the authentication plugin and its parameters.
     *
     * @param authPluginClassName the class name of the authentication plugin
     * @param authParamsString the parameters of the authentication plugin
     * @return the OxiaClientBuilder instance
     * @throws UnsupportedAuthenticationException if the authentication plugin is not supported
     */
    OxiaClientBuilder authentication(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException;

    /**
     * Configures whether to enable TLS (Transport Layer Security) for the client connection.
     *
     * @param enableTls true to enable TLS, false to disable it
     * @return the builder instance for method chaining
     */
    OxiaClientBuilder enableTls(boolean enableTls);

    /**
     * Load the configuration from the specified configuration file.
     *
     * @param configPath the path of the configuration file
     * @return the OxiaClientBuilder instance
     */
    OxiaClientBuilder loadConfig(String configPath);

    /**
     * Load the configuration from the specified configuration file.
     *
     * @param configFile the configuration file
     * @return the OxiaClientBuilder instance
     */
    OxiaClientBuilder loadConfig(File configFile);

    /**
     * Load the configuration from the specified properties.
     *
     * @param properties the properties
     * @return the OxiaClientBuilder instance
     */
    OxiaClientBuilder loadConfig(Properties properties);
}
