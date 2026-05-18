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
import java.util.concurrent.ThreadLocalRandom;

public class OxiaStubManager implements AutoCloseable {
    private static final Logger LOG = Logger.get(OxiaStubManager.class);

    @VisibleForTesting final Map<Key, OxiaStub> stubs = new ConcurrentHashMap<>();

    private final int maxConnectionPerNode;
    private final ClientConfig clientConfig;

    public OxiaStubManager(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        this.maxConnectionPerNode = clientConfig.maxConnectionPerNode();
    }

    public OxiaStub getStub(String address) {
        final var random = ThreadLocalRandom.current().nextInt();
        var modKey = random % maxConnectionPerNode;
        if (modKey < 0) {
            modKey += maxConnectionPerNode;
        }
        final var key = new Key(address, modKey);
        final var existing = stubs.get(key);
        if (existing != null) {
            return existing;
        }

        final var stub = newStub(key);
        final var previous = stubs.putIfAbsent(key, stub);
        if (previous != null) {
            closeUnusedStub(key, stub);
            return previous;
        }

        stub.startHealthCheck();
        return stub;
    }

    private OxiaStub newStub(Key key) {
        return new OxiaStub(key.address, clientConfig, stub -> removeStub(key, stub));
    }

    private void removeStub(Key key, OxiaStub stub) {
        if (!stubs.remove(key, stub)) {
            return;
        }
        try {
            stub.close();
        } catch (Exception e) {
            LOG.warn()
                    .attr("address", key.address())
                    .exceptionMessage(e)
                    .log("Failed to close unhealthy GRPC stub");
        }
    }

    private void closeUnusedStub(Key key, OxiaStub stub) {
        try {
            stub.close();
        } catch (Exception e) {
            LOG.warn()
                    .attr("address", key.address())
                    .exceptionMessage(e)
                    .log("Failed to close unused GRPC stub");
        }
    }

    @Override
    public void close() throws Exception {
        for (OxiaStub stub : stubs.values()) {
            stub.close();
        }
    }

    record Key(String address, int random) {}
}
