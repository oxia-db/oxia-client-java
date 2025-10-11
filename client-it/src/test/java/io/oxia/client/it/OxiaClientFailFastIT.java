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

import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.shard.NamespaceNotFoundException;
import io.oxia.testcontainers.OxiaContainer;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@Slf4j
public class OxiaClientFailFastIT {

    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME)
                    .withShards(4)
                    .withLogConsumer(new Slf4jLogConsumer(log));

    @Test
    public void testWrongNamespace() {
        try {
            OxiaClientBuilder.create(oxia.getServiceAddress())
                    .namespace("my-ns-does-not-exist")
                    .maxConnectionPerNode(ThreadLocalRandom.current().nextInt(10) + 1)
                    .asyncClient()
                    .join();
            Assertions.fail("Unexpected behaviour!");
        } catch (CompletionException exception) {
            Assertions.assertNotNull(exception.getCause());
            Assertions.assertInstanceOf(NamespaceNotFoundException.class, exception.getCause());
        }
    }
}
