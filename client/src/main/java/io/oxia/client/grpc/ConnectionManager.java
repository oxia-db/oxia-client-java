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

import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import io.oxia.client.ClientConfig;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

class ConnectionManager implements AutoCloseable {
    @VisibleForTesting final Map<Key, Connection> connections = new ConcurrentHashMap<>();

    private static final Logger log = Logger.get(ConnectionManager.class);

    private final int maxConnectionPerNode;
    private final ClientConfig clientConfig;
    private final ScheduledExecutorService executor;

    ConnectionManager(ClientConfig clientConfig, ScheduledExecutorService executor) {
        this.clientConfig = clientConfig;
        this.maxConnectionPerNode = clientConfig.maxConnectionPerNode();
        this.executor = executor;
    }

    Connection getConnection(String address) {
        final var random = ThreadLocalRandom.current().nextInt();
        var modKey = random % maxConnectionPerNode;
        if (modKey < 0) {
            modKey += maxConnectionPerNode;
        }
        return connections.computeIfAbsent(
                new Key(address, modKey),
                key ->
                        new Connection(
                                key.address,
                                clientConfig,
                                executor,
                                connection -> removeConnection(key, connection)));
    }

    @Override
    public void close() throws Exception {
        for (Connection connection : connections.values()) {
            connection.close();
        }
    }

    private void removeConnection(Key key, Connection connection) {
        if (!connections.remove(key, connection)) {
            return;
        }
        try {
            connection.close();
        } catch (Exception e) {
            log.warn().exception(e).log("Failed to close unhealthy GRPC connection");
        }
    }

    record Key(String address, int random) {}
}
