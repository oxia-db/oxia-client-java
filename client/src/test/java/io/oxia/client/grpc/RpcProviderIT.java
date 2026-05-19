/*
 * Copyright © 2022-2026 The Oxia Authors
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

import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class RpcProviderIT {
    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME, 4, true)
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(RpcProviderIT.class)));

    @Test
    void testMaxConnectionPerNode() throws Exception {
        int maxConnectionPerNode = 10;
        OxiaClientBuilder builder = OxiaClientBuilder.create("");
        builder.maxConnectionPerNode(maxConnectionPerNode);

        var executor = Executors.newSingleThreadScheduledExecutor();
        try (ConnectionManager connectionManager =
                new ConnectionManager(((OxiaClientBuilderImpl) builder).getClientConfig(), executor)) {
            for (int i = 0; i < 1000; i++) {
                connectionManager.getConnection(oxia.getServiceAddress());
            }

            Assertions.assertEquals(maxConnectionPerNode, connectionCount(connectionManager));
        } finally {
            executor.shutdownNow();
        }
    }

    private static int connectionCount(ConnectionManager connectionManager)
            throws ReflectiveOperationException {
        Field field = ConnectionManager.class.getDeclaredField("connections");
        field.setAccessible(true);
        return ((Map<?, ?>) field.get(connectionManager)).size();
    }
}
