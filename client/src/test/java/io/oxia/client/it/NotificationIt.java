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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Notification;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class NotificationIt {
    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME)
                    .withImagePullPolicy(PullPolicy.alwaysPull())
                    .withShards(10)
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(NotificationIt.class)));

    private static AsyncOxiaClient client;

    private static final Queue<Notification> notifications = new LinkedBlockingQueue<>();

    @BeforeAll
    static void beforeAll() {
        Resource resource =
                Resource.getDefault()
                        .merge(
                                Resource.create(
                                        Attributes.of(AttributeKey.stringKey("service.name"), "logical-service-name")));

        InMemoryMetricReader metricReader = InMemoryMetricReader.create();
        SdkMeterProvider sdkMeterProvider =
                SdkMeterProvider.builder().registerMetricReader(metricReader).setResource(resource).build();

        OpenTelemetry openTelemetry =
                OpenTelemetrySdk.builder().setMeterProvider(sdkMeterProvider).build();

        client =
                OxiaClientBuilder.create(oxia.getServiceAddress())
                        .maxConnectionPerNode(ThreadLocalRandom.current().nextInt(10) + 1)
                        .openTelemetry(openTelemetry)
                        .asyncClient()
                        .join();
        client.notifications(notifications::add);
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void testDeleteRange() {
        for (int i = 0; i < 10; i++) {
            client.put(i + "", (i + "").getBytes(StandardCharsets.UTF_8)).join();
        }

        Awaitility.await().untilAsserted(() -> Assertions.assertEquals(10, notifications.size()));

        notifications.clear();

        client.deleteRange("0", "100").join();

        Awaitility.await()
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(10, notifications.size());
                            for (Notification notification : notifications) {
                                Assertions.assertInstanceOf(Notification.KeyRangeDelete.class, notification);
                                Notification.KeyRangeDelete delete = (Notification.KeyRangeDelete) notification;
                                Assertions.assertEquals("0", delete.startKeyInclusive());
                                Assertions.assertEquals("100", delete.endKeyExclusive());
                            }
                        });
    }
}
