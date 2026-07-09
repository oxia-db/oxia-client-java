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
package io.oxia.client.api;

import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.internal.DefaultImplementation;
import java.time.Duration;

/**
 * A pool of resources that can be shared by multiple Oxia client instances in the same JVM process.
 *
 * <p>Each standalone client normally owns a full footprint: a background thread pool, its own gRPC
 * connections to every data server, and its own shard-assignment stream per namespace. When many
 * clients (or many sessions) run in a single process, this footprint dominates. A {@code
 * SharedResources} lets those clients multiplex over a shared transport instead:
 *
 * <ul>
 *   <li>a single background thread pool;
 *   <li>a single pool of gRPC connections;
 *   <li>a single shard-assignment stream per {@code (serviceAddress, namespace)}.
 * </ul>
 *
 * <p>Each client keeps its own sessions, ephemeral records, notifications and batching — closing an
 * individual client only releases that per-client state. The shared resources live until this
 * object is {@link #close() closed}.
 *
 * <pre>{@code
 * try (SharedResources shared = SharedResources.builder().numWorkerThreads(4).build()) {
 *     AsyncOxiaClient c1 = OxiaClientBuilder.create("localhost:6648")
 *             .sharedResources(shared)
 *             .clientIdentifier("app-1")
 *             .asyncClient()
 *             .join();
 *     // c1..cN share the executor, connections and shard-assignment stream.
 * } // releases the shared executor, connections and shard managers
 * }</pre>
 *
 * <p>Transport-level settings (TLS, authentication, connection keep-alive and pool size) are
 * governed by this object; a client built with {@link OxiaClientBuilder#sharedResources(
 * SharedResources)} inherits them. Setting any transport-level option on such a client is a
 * configuration error, rejected with an {@link IllegalArgumentException} — configure them here
 * instead, and only share a pool between clients whose transport settings match.
 */
public interface SharedResources extends AutoCloseable {

    /**
     * Create a new builder for a {@link SharedResources} pool.
     *
     * @return the builder instance
     */
    static Builder builder() {
        return DefaultImplementation.getSharedResourcesBuilder();
    }

    /** Release the shared executor, connections and shard-assignment streams. */
    @Override
    void close();

    /** Builder for a {@link SharedResources} pool. */
    interface Builder {

        /**
         * Set the number of threads in the shared background thread pool.
         *
         * <p>Default is the number of available processors.
         *
         * @param numWorkerThreads the number of worker threads
         * @return the builder instance
         */
        Builder numWorkerThreads(int numWorkerThreads);

        /**
         * Set the number of shared batch-assembly threads (reads and writes each get this many).
         *
         * <p>These threads are shared by every client built on this pool, so the total number of
         * batching threads stays fixed no matter how many clients are created. Default is {@code 1}.
         *
         * @param batchingThreads the number of shared batching threads
         * @return the builder instance
         */
        Builder batchingThreads(int batchingThreads);

        /**
         * Configure whether to enable TLS for the shared connections.
         *
         * <p>Default is {@code false}.
         *
         * @param enableTls true to enable TLS
         * @return the builder instance
         */
        Builder enableTls(boolean enableTls);

        /**
         * Configure the authentication used by the shared connections.
         *
         * @param authentication the authentication instance
         * @return the builder instance
         */
        Builder authentication(Authentication authentication);

        /**
         * Configure the keep-alive interval for the shared connections.
         *
         * <p>Default is {@code 10 sec}.
         *
         * @param keepAliveTime the keep-alive interval
         * @return the builder instance
         */
        Builder connectionKeepAliveTime(Duration keepAliveTime);

        /**
         * Configure the keep-alive timeout for the shared connections.
         *
         * <p>Default is {@code 3 sec}.
         *
         * @param keepAliveTimeout the keep-alive timeout
         * @return the builder instance
         */
        Builder connectionKeepAliveTimeout(Duration keepAliveTimeout);

        /**
         * Configure the connection backoff policy for the shared connections.
         *
         * <p>Defaults are {@code min=100millis, max=30sec}.
         *
         * @param minDelay the minimum delay between retries
         * @param maxDelay the maximum delay between retries
         * @return the builder instance
         */
        Builder connectionBackoff(Duration minDelay, Duration maxDelay);

        /**
         * Configure the maximum number of shared connections to each Oxia server node.
         *
         * <p>Default is {@code 1}.
         *
         * @param connections the maximum number of connections
         * @return the builder instance
         */
        Builder maxConnectionPerNode(int connections);

        /**
         * Configure the OpenTelemetry instance used for shared-resource metrics.
         *
         * <p>By default, the global OpenTelemetry instance is used if available.
         *
         * @param openTelemetry an OpenTelemetry instance
         * @return the builder instance
         */
        Builder openTelemetry(OpenTelemetry openTelemetry);

        /**
         * Build the {@link SharedResources} pool.
         *
         * @return the shared resources instance
         */
        SharedResources build();
    }
}
