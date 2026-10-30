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
package io.oxia.client.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.options.PutOption;
import io.oxia.testcontainers.OxiaContainer;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class LegacyServiceIT {

    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaImages.OXIA_0_14)
                    .withShards(4)
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(LegacyServiceIT.class)));

    @Test
    void readsAndWritesThroughTheLegacyService() throws Exception {
        try (var client = OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient()) {
            client.put("a", "a".getBytes(UTF_8));
            client.put("b", "b".getBytes(UTF_8), Set.of(PutOption.AsEphemeralRecord));

            assertThat(client.get("a").value()).isEqualTo("a".getBytes(UTF_8));
            assertThat(client.get("b").version().sessionId()).isNotEmpty();
            assertThat(client.delete("a")).isTrue();
            assertThat(client.get("a")).isNull();
        }
    }
}
