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

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Testcontainers
@Slf4j
public class ClientReconnectIT {

    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME, 4, true)
                    .withLogConsumer(new Slf4jLogConsumer(log));

    @Test
    public void testReconnection() {
        final AsyncOxiaClient client =
                OxiaClientBuilder.create(oxia.getServiceAddress()).asyncClient().join();
        final String key = "1";
        final byte[] value = "1".getBytes(StandardCharsets.UTF_8);

        final long startTime = System.currentTimeMillis();
        final long elapse = 3000L;
        while (true) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            try {
                client.put(key, value).get(1, TimeUnit.SECONDS);
            } catch (Throwable ex) {
                Assertions.fail("unexpected behaviour", ex);
            }

            try {
                final GetResult getResult = client.get("1").get(1, TimeUnit.SECONDS);
                Assertions.assertArrayEquals(getResult.value(), value);
            } catch (Throwable ex) {
                Assertions.fail("unexpected behaviour", ex);
            }

            if (System.currentTimeMillis() - startTime >= elapse) {
                oxia.stop();

                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                oxia.start();

                Awaitility.await()
                        .atMost(15, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> {
                                    try {
                                        client.put(key, value).get(1, TimeUnit.SECONDS);
                                    } catch (Throwable ex) {
                                        Assertions.fail("unexpected behaviour", ex);
                                    }

                                    try {
                                        final GetResult getResult = client.get("1").get(1, TimeUnit.SECONDS);
                                        Assertions.assertArrayEquals(getResult.value(), value);
                                    } catch (Throwable ex) {
                                        Assertions.fail("unexpected behaviour", ex);
                                    }
                                });
                break;
            }
        }
    }
}
