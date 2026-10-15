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
package io.oxia.client.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.options.PutOption;
import io.oxia.testcontainers.OxiaContainer;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** The client against a server before 0.15.0, which only serves the legacy service name. */
@Testcontainers
class LegacyServerIT {

    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaImages.OXIA_0_14)
                    .withShards(2)
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(LegacyServerIT.class)));

    @Test
    void testOperations() throws Exception {
        try (var client = OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient()) {
            client.put("/a", "1".getBytes(UTF_8));
            client.put("/b", "2".getBytes(UTF_8), Set.of(PutOption.AsEphemeralRecord));

            assertArrayEquals("1".getBytes(UTF_8), client.get("/a").value());
            assertArrayEquals("2".getBytes(UTF_8), client.get("/b").value());
            assertEquals(List.of("/a", "/b"), client.list("/a", "/c"));

            assertTrue(client.delete("/a"));
            assertNull(client.get("/a"));
        }
    }
}
