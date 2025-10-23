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
import io.oxia.client.ClientConfig;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class OxiaStubManager implements AutoCloseable {
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
        return stubs.computeIfAbsent(
                new Key(address, modKey), key -> new OxiaStub(key.address, clientConfig));
    }

    @Override
    public void close() throws Exception {
        for (OxiaStub stub : stubs.values()) {
            stub.close();
        }
    }

    record Key(String address, int random) {}
}
