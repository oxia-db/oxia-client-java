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
package io.oxia.client.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.oxia.client.SharedResourcesImpl;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SharedResources;
import io.oxia.client.api.options.PutOption;
import io.oxia.testcontainers.OxiaContainer;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class SharedResourcesIT {

    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaImages.OXIA)
                    .withShards(3)
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(SharedResourcesIT.class)));

    @Test
    void clientsShareTransportButKeepIndependentSessions() throws Exception {
        // Baseline of dedicated "oxia-client-async" threads created by any standalone clients running
        // in this JVM, so the assertion below is robust to other test classes.
        long asyncThreadsBefore = threadCount("oxia-client-async");

        try (SharedResources shared = SharedResources.builder().numWorkerThreads(2).build()) {
            AsyncOxiaClient c1 =
                    OxiaClientBuilder.create(oxia.getServiceAddress())
                            .sharedResources(shared)
                            .clientIdentifier("client-1")
                            .asyncClient()
                            .join();
            AsyncOxiaClient c2 =
                    OxiaClientBuilder.create(oxia.getServiceAddress())
                            .sharedResources(shared)
                            .clientIdentifier("client-2")
                            .asyncClient()
                            .join();

            // Both clients are functional and see each other's (non-ephemeral) writes.
            c1.put("k1", "v1".getBytes(UTF_8)).join();
            c2.put("k2", "v2".getBytes(UTF_8)).join();
            assertThat(c2.get("k1").join().value()).isEqualTo("v1".getBytes(UTF_8));
            assertThat(c1.get("k2").join().value()).isEqualTo("v2".getBytes(UTF_8));

            // The executor is shared: the pooled clients spawned no dedicated async thread pool, and
            // the shared pool's threads are present.
            assertThat(threadCount("oxia-client-async")).isEqualTo(asyncThreadsBefore);
            assertThat(threadCount("oxia-client-shared")).isGreaterThan(0);

            // Connections are shared and actually open.
            assertThat(((SharedResourcesImpl) shared).getConnectionCount()).isGreaterThan(0);

            // Sessions are independent per client: each ephemeral record is tied to its own session.
            c1.put("e1", "e1".getBytes(UTF_8), Set.of(PutOption.AsEphemeralRecord)).join();
            c2.put("e2", "e2".getBytes(UTF_8), Set.of(PutOption.AsEphemeralRecord)).join();
            assertThat(c2.get("e1").join()).isNotNull();
            assertThat(c1.get("e2").join()).isNotNull();

            // Closing one client only removes its own ephemeral keys; the shared resources and the
            // other client keep working.
            c1.close();
            await().untilAsserted(() -> assertThat(c2.get("e1").join()).isNull());
            assertThat(c2.get("e2").join()).isNotNull();

            c2.put("k3", "v3".getBytes(UTF_8)).join();
            assertThat(c2.get("k3").join().value()).isEqualTo("v3".getBytes(UTF_8));
            assertThat(((SharedResourcesImpl) shared).getConnectionCount()).isGreaterThan(0);
            assertThat(threadCount("oxia-client-shared")).isGreaterThan(0);

            c2.close();

            // A new client can still be created from the still-open pool.
            AsyncOxiaClient c3 =
                    OxiaClientBuilder.create(oxia.getServiceAddress())
                            .sharedResources(shared)
                            .clientIdentifier("client-3")
                            .asyncClient()
                            .join();
            assertThat(c3.get("k1").join().value()).isEqualTo("v1".getBytes(UTF_8));
            c3.close();
        }
    }

    private static long threadCount(String namePrefix) {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith(namePrefix))
                .count();
    }
}
