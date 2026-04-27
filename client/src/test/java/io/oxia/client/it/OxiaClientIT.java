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

import static io.oxia.client.api.options.PutOption.IfRecordDoesNotExist;
import static io.oxia.client.api.options.PutOption.IfVersionIdEquals;
import static io.oxia.client.constants.Constants.MAXIMUM_FRAME_SIZE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.grpc.StatusRuntimeException;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification.KeyCreated;
import io.oxia.client.api.Notification.KeyDeleted;
import io.oxia.client.api.Notification.KeyModified;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.DeleteRangeOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.GetSequenceUpdatesOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import io.oxia.testcontainers.OxiaContainer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class OxiaClientIT {
    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME)
                    .withShards(10)
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(OxiaClientIT.class)));

    private static AsyncOxiaClient client;

    private static final Queue<Notification> notifications = new LinkedBlockingQueue<>();

    private static InMemoryMetricReader metricReader;

    @BeforeAll
    static void beforeAll() {
        Resource resource =
                Resource.getDefault()
                        .merge(
                                Resource.create(
                                        Attributes.of(AttributeKey.stringKey("service.name"), "logical-service-name")));

        metricReader = InMemoryMetricReader.create();
        SdkMeterProvider sdkMeterProvider =
                SdkMeterProvider.builder().registerMetricReader(metricReader).setResource(resource).build();

        OpenTelemetry openTelemetry =
                OpenTelemetrySdk.builder().setMeterProvider(sdkMeterProvider).build();

        client =
                OxiaClientBuilder.create(oxia.getServiceAddress())
                        .openTelemetry(openTelemetry)
                        .maxConnectionPerNode(10)
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
    void test() throws Exception {
        CompletableFuture<PutResult> a =
                client.put("a", "a".getBytes(UTF_8), Set.of(IfRecordDoesNotExist));
        CompletableFuture<PutResult> b =
                client.put("b", "b".getBytes(UTF_8), Set.of(IfRecordDoesNotExist));
        CompletableFuture<PutResult> c = client.put("c", "c".getBytes(UTF_8));
        CompletableFuture<PutResult> d = client.put("d", "d".getBytes(UTF_8));
        allOf(a, b, c, d).join();

        assertThatThrownBy(
                        () -> client.put("a", "a".getBytes(UTF_8), Set.of(IfRecordDoesNotExist)).join())
                .hasCauseInstanceOf(KeyAlreadyExistsException.class);
        GetResult getResult = client.get("a").join();
        assertThat(getResult.value()).isEqualTo("a".getBytes(UTF_8));
        long aVersion = getResult.version().versionId();

        long finalAVersion = aVersion;
        await()
                .untilAsserted(
                        () -> assertThat(notifications).contains(new KeyCreated("a", finalAVersion)));

        client.put("a", "a2".getBytes(UTF_8), Set.of(IfVersionIdEquals(aVersion))).join();
        getResult = client.get("a").join();
        assertThat(getResult.value()).isEqualTo("a2".getBytes(UTF_8));
        aVersion = getResult.version().versionId();

        long finalA2Version = aVersion;
        await()
                .untilAsserted(
                        () -> assertThat(notifications).contains(new KeyModified("a", finalA2Version)));

        long bVersion = client.get("b").join().version().versionId();
        assertThatThrownBy(
                        () ->
                                client
                                        .put("b", "b2".getBytes(UTF_8), Set.of(IfVersionIdEquals(bVersion + 1L)))
                                        .join())
                .hasCauseInstanceOf(UnexpectedVersionIdException.class);

        long cVersion = client.get("c").join().version().versionId();
        assertThatThrownBy(
                        () -> client.delete("c", Set.of(DeleteOption.IfVersionIdEquals(cVersion + 1L))).join())
                .hasCauseInstanceOf(UnexpectedVersionIdException.class);

        List<String> listResult = client.list("a", "e").join();
        assertThat(listResult).containsOnly("a", "b", "c", "d");

        client.delete("a", Set.of(DeleteOption.IfVersionIdEquals(aVersion))).join();
        getResult = client.get("a").join();
        assertThat(getResult).isNull();

        await().untilAsserted(() -> assertThat(notifications).contains(new KeyDeleted("a")));

        client.delete("b").join();
        getResult = client.get("b").join();
        assertThat(getResult).isNull();

        client.deleteRange("c", "d").join();

        listResult = client.list("a", "e").join();
        assertThat(listResult).containsExactly("d");

        assertThat(client.get("z").join()).isNull();

        String clientId = getClass().getSimpleName();
        try (AsyncOxiaClient otherClient =
                OxiaClientBuilder.create(oxia.getServiceAddress())
                        .clientIdentifier(clientId)
                        .asyncClient()
                        .join()) {
            otherClient.put("f", "f".getBytes(), Set.of(PutOption.AsEphemeralRecord)).join();
            getResult = client.get("f").join();
            Object sessionId = getResult.version().sessionId().get();
            assertThat(sessionId).isNotNull();
            assertThat(getResult.version().clientIdentifier().get()).isEqualTo(clientId);

            PutResult putResult =
                    otherClient.put("g", "g".getBytes(), Set.of(PutOption.AsEphemeralRecord)).join();
            assertThat(putResult.version().clientIdentifier().get()).isEqualTo(clientId);
            assertThat(putResult.version().sessionId().get()).isNotNull();

            otherClient.put("h", "h".getBytes()).join();
        }

        await().untilAsserted(() -> assertThat(client.get("f").join()).isNull());
        assertThat(client.get("g").join()).isNull();
        assertThat(client.get("h").join()).isNotNull();

        metricReader.forceFlush();
        var metrics = metricReader.collectAllMetrics();
        var metricsByName = metrics.stream().collect(Collectors.toMap(MetricData::getName, identity()));

        assertThat(
                        metricsByName.get("oxia.client.ops").getHistogramData().getPoints().stream()
                                .map(HistogramPointData::getCount)
                                .reduce(0L, Long::sum))
                .isEqualTo(124);
    }

    @Test
    void testGetFloorCeiling() throws Exception {
        try (SyncOxiaClient syncClient =
                OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient()) {
            syncClient.put("a", "0".getBytes());
            syncClient.put("c", "2".getBytes());
            syncClient.put("d", "3".getBytes());
            syncClient.put("e", "4".getBytes());
            syncClient.put("g", "6".getBytes());

            GetResult getResult = syncClient.get("a");
            assertThat(getResult.key()).isEqualTo("a");
            assertThat(getResult.value()).isEqualTo("0".getBytes());

            getResult = syncClient.get("a", Collections.singleton(GetOption.ComparisonEqual));
            assertThat(getResult.key()).isEqualTo("a");
            assertThat(getResult.value()).isEqualTo("0".getBytes());

            getResult = syncClient.get("a", Collections.singleton(GetOption.ComparisonFloor));
            assertThat(getResult.key()).isEqualTo("a");
            assertThat(getResult.value()).isEqualTo("0".getBytes());

            getResult = syncClient.get("a", Collections.singleton(GetOption.ComparisonCeiling));
            assertThat(getResult.key()).isEqualTo("a");
            assertThat(getResult.value()).isEqualTo("0".getBytes());

            getResult = syncClient.get("a", Collections.singleton(GetOption.ComparisonLower));
            assertThat(getResult).isNull();

            getResult = syncClient.get("a", Collections.singleton(GetOption.ComparisonHigher));
            assertThat(getResult.key()).isEqualTo("c");
            assertThat(getResult.value()).isEqualTo("2".getBytes());

            getResult = syncClient.get("b");
            assertThat(getResult).isNull();

            getResult = syncClient.get("b", Collections.singleton(GetOption.ComparisonEqual));
            assertThat(getResult).isNull();

            getResult = syncClient.get("b", Collections.singleton(GetOption.ComparisonFloor));
            assertThat(getResult.key()).isEqualTo("a");
            assertThat(getResult.value()).isEqualTo("0".getBytes());

            getResult = syncClient.get("b", Collections.singleton(GetOption.ComparisonCeiling));
            assertThat(getResult.key()).isEqualTo("c");
            assertThat(getResult.value()).isEqualTo("2".getBytes());

            getResult = syncClient.get("b", Collections.singleton(GetOption.ComparisonLower));
            assertThat(getResult.key()).isEqualTo("a");
            assertThat(getResult.value()).isEqualTo("0".getBytes());

            getResult = syncClient.get("b", Collections.singleton(GetOption.ComparisonHigher));
            assertThat(getResult.key()).isEqualTo("c");
            assertThat(getResult.value()).isEqualTo("2".getBytes());

            getResult = syncClient.get("c");
            assertThat(getResult.key()).isEqualTo("c");
            assertThat(getResult.value()).isEqualTo("2".getBytes());

            getResult = syncClient.get("c", Collections.singleton(GetOption.ComparisonEqual));
            assertThat(getResult.key()).isEqualTo("c");
            assertThat(getResult.value()).isEqualTo("2".getBytes());

            getResult = syncClient.get("c", Collections.singleton(GetOption.ComparisonFloor));
            assertThat(getResult.key()).isEqualTo("c");
            assertThat(getResult.value()).isEqualTo("2".getBytes());

            getResult = syncClient.get("c", Collections.singleton(GetOption.ComparisonCeiling));
            assertThat(getResult.key()).isEqualTo("c");
            assertThat(getResult.value()).isEqualTo("2".getBytes());

            getResult = syncClient.get("c", Collections.singleton(GetOption.ComparisonLower));
            assertThat(getResult.key()).isEqualTo("a");
            assertThat(getResult.value()).isEqualTo("0".getBytes());

            getResult = syncClient.get("c", Collections.singleton(GetOption.ComparisonHigher));
            assertThat(getResult.key()).isEqualTo("d");
            assertThat(getResult.value()).isEqualTo("3".getBytes());

            getResult = syncClient.get("d");
            assertThat(getResult.key()).isEqualTo("d");
            assertThat(getResult.value()).isEqualTo("3".getBytes());

            getResult = syncClient.get("d", Collections.singleton(GetOption.ComparisonEqual));
            assertThat(getResult.key()).isEqualTo("d");
            assertThat(getResult.value()).isEqualTo("3".getBytes());

            getResult = syncClient.get("d", Collections.singleton(GetOption.ComparisonFloor));
            assertThat(getResult.key()).isEqualTo("d");
            assertThat(getResult.value()).isEqualTo("3".getBytes());

            getResult = syncClient.get("d", Collections.singleton(GetOption.ComparisonCeiling));
            assertThat(getResult.key()).isEqualTo("d");
            assertThat(getResult.value()).isEqualTo("3".getBytes());

            getResult = syncClient.get("d", Collections.singleton(GetOption.ComparisonLower));
            assertThat(getResult.key()).isEqualTo("c");
            assertThat(getResult.value()).isEqualTo("2".getBytes());

            getResult = syncClient.get("d", Collections.singleton(GetOption.ComparisonHigher));
            assertThat(getResult.key()).isEqualTo("e");
            assertThat(getResult.value()).isEqualTo("4".getBytes());

            getResult = syncClient.get("e");
            assertThat(getResult.key()).isEqualTo("e");
            assertThat(getResult.value()).isEqualTo("4".getBytes());

            getResult = syncClient.get("e", Collections.singleton(GetOption.ComparisonEqual));
            assertThat(getResult.key()).isEqualTo("e");
            assertThat(getResult.value()).isEqualTo("4".getBytes());

            getResult = syncClient.get("e", Collections.singleton(GetOption.ComparisonFloor));
            assertThat(getResult.key()).isEqualTo("e");
            assertThat(getResult.value()).isEqualTo("4".getBytes());

            getResult = syncClient.get("e", Collections.singleton(GetOption.ComparisonCeiling));
            assertThat(getResult.key()).isEqualTo("e");
            assertThat(getResult.value()).isEqualTo("4".getBytes());

            getResult = syncClient.get("e", Collections.singleton(GetOption.ComparisonLower));
            assertThat(getResult.key()).isEqualTo("d");
            assertThat(getResult.value()).isEqualTo("3".getBytes());

            getResult = syncClient.get("e", Collections.singleton(GetOption.ComparisonHigher));
            assertThat(getResult.key()).isEqualTo("g");
            assertThat(getResult.value()).isEqualTo("6".getBytes());

            getResult = syncClient.get("f");
            assertThat(getResult).isNull();

            getResult = syncClient.get("f", Collections.singleton(GetOption.ComparisonEqual));
            assertThat(getResult).isNull();

            getResult = syncClient.get("f", Collections.singleton(GetOption.ComparisonFloor));
            assertThat(getResult.key()).isEqualTo("e");
            assertThat(getResult.value()).isEqualTo("4".getBytes());

            getResult = syncClient.get("f", Collections.singleton(GetOption.ComparisonCeiling));
            assertThat(getResult.key()).isEqualTo("g");
            assertThat(getResult.value()).isEqualTo("6".getBytes());

            getResult = syncClient.get("f", Collections.singleton(GetOption.ComparisonLower));
            assertThat(getResult.key()).isEqualTo("e");
            assertThat(getResult.value()).isEqualTo("4".getBytes());

            getResult = syncClient.get("f", Collections.singleton(GetOption.ComparisonHigher));
            assertThat(getResult.key()).isEqualTo("g");
            assertThat(getResult.value()).isEqualTo("6".getBytes());
        }
    }

    @Test
    void testPartitionKey() throws Exception {
        try (SyncOxiaClient syncClient =
                OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient()) {
            syncClient.put("pk_a", "0".getBytes(), Set.of(PutOption.PartitionKey("x")));

            GetResult getResult = syncClient.get("pk_a");
            assertThat(getResult).isNull();

            getResult = syncClient.get("pk_a", Set.of(GetOption.PartitionKey("x")));
            assertThat(getResult.key()).isEqualTo("pk_a");
            assertThat(getResult.value()).isEqualTo("0".getBytes());

            Set<PutOption> partitionKey = Set.of(PutOption.PartitionKey("x"));
            syncClient.put("pk_a", "0".getBytes(), partitionKey);
            syncClient.put("pk_b", "1".getBytes(), partitionKey);
            syncClient.put("pk_c", "2".getBytes(), partitionKey);
            syncClient.put("pk_d", "3".getBytes(), partitionKey);
            syncClient.put("pk_e", "4".getBytes(), partitionKey);
            syncClient.put("pk_f", "5".getBytes(), partitionKey);
            syncClient.put("pk_g", "6".getBytes(), partitionKey);

            List<String> keys = syncClient.list("pk_a", "pk_d");
            assertThat(keys).containsExactly("pk_a", "pk_b", "pk_c");

            keys = syncClient.list("pk_a", "pk_d", Set.of(ListOption.PartitionKey("x")));
            assertThat(keys).containsExactly("pk_a", "pk_b", "pk_c");

            keys =
                    syncClient.list("pk_a", "pk_d", Set.of(ListOption.PartitionKey("wrong-partition-key")));
            assertThat(keys).isEmpty();

            boolean deleted =
                    syncClient.delete("pk_g", Set.of(DeleteOption.PartitionKey("wrong-partition-key")));
            assertThat(deleted).isFalse();

            deleted = syncClient.delete("pk_g", Set.of(DeleteOption.PartitionKey("x")));
            assertThat(deleted).isTrue();

            getResult = syncClient.get("pk_a", Set.of(GetOption.ComparisonHigher));
            assertThat(getResult.key()).isEqualTo("pk_b");
            assertThat(getResult.value()).isEqualTo("1".getBytes());

            getResult =
                    syncClient.get("pk_a", Set.of(GetOption.ComparisonHigher, GetOption.PartitionKey("x")));
            assertThat(getResult.key()).isEqualTo("pk_b");
            assertThat(getResult.value()).isEqualTo("1".getBytes());

            getResult =
                    syncClient.get(
                            "pk_a",
                            Set.of(
                                    GetOption.ComparisonHigher,
                                    GetOption.PartitionKey("another-wrong-partition-key")));
            assertThat(getResult.key()).isNotEqualTo("pk_b");
            assertThat(getResult.value()).isNotEqualTo("1".getBytes());

            syncClient.deleteRange(
                    "pk_c", "pk_e", Set.of(DeleteRangeOption.PartitionKey("wrong-partition-key")));

            keys = syncClient.list("pk_c", "pk_f");
            assertThat(keys).containsExactly("pk_c", "pk_d", "pk_e");

            syncClient.deleteRange("pk_c", "pk_e", Set.of(DeleteRangeOption.PartitionKey("x")));

            keys = syncClient.list("pk_c", "pk_f");
            assertThat(keys).containsExactly("pk_e");
        }
    }

    @Test
    void testSequentialKeys() throws Exception {
        try (SyncOxiaClient syncClient =
                OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient()) {
            assertThatThrownBy(
                            () ->
                                    syncClient.put(
                                            "sk_a", "0".getBytes(), Set.of(PutOption.SequenceKeysDeltas(List.of(1L)))))
                    .isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(
                            () ->
                                    syncClient.put(
                                            "sk_a",
                                            "0".getBytes(),
                                            Set.of(
                                                    PutOption.SequenceKeysDeltas(List.of(0L)), PutOption.PartitionKey("x"))))
                    .isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(
                            () ->
                                    syncClient.put(
                                            "sk_a",
                                            "0".getBytes(),
                                            Set.of(
                                                    PutOption.SequenceKeysDeltas(List.of(1L, -1L)),
                                                    PutOption.PartitionKey("x"))))
                    .isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(
                            () ->
                                    syncClient.put(
                                            "sk_a",
                                            "0".getBytes(),
                                            Set.of(
                                                    PutOption.SequenceKeysDeltas(List.of(1L)),
                                                    PutOption.PartitionKey("x"),
                                                    PutOption.IfVersionIdEquals(1L))))
                    .isInstanceOf(IllegalArgumentException.class);

            PutResult putResult =
                    syncClient.put(
                            "sk_a",
                            "0".getBytes(),
                            Set.of(PutOption.PartitionKey("x"), PutOption.SequenceKeysDeltas(List.of(1L))));
            assertThat(putResult.key()).isEqualTo(String.format("sk_a-%020d", 1));

            putResult =
                    syncClient.put(
                            "sk_a",
                            "1".getBytes(),
                            Set.of(PutOption.PartitionKey("x"), PutOption.SequenceKeysDeltas(List.of(3L))));
            assertThat(putResult.key()).isEqualTo(String.format("sk_a-%020d", 4));

            putResult =
                    syncClient.put(
                            "sk_a",
                            "2".getBytes(),
                            Set.of(PutOption.PartitionKey("x"), PutOption.SequenceKeysDeltas(List.of(1L, 6L))));
            assertThat(putResult.key()).isEqualTo(String.format("sk_a-%020d-%020d", 5, 6));

            GetResult getResult = syncClient.get("sk_a", Set.of(GetOption.PartitionKey("x")));
            assertThat(getResult).isNull();

            getResult =
                    syncClient.get(String.format("sk_a-%020d", 1), Set.of(GetOption.PartitionKey("x")));
            assertThat(getResult.value()).isEqualTo("0".getBytes());

            getResult =
                    syncClient.get(String.format("sk_a-%020d", 4), Set.of(GetOption.PartitionKey("x")));
            assertThat(getResult.value()).isEqualTo("1".getBytes());

            getResult =
                    syncClient.get(
                            String.format("sk_a-%020d-%020d", 5, 6), Set.of(GetOption.PartitionKey("x")));
            assertThat(getResult.value()).isEqualTo("2".getBytes());
        }
    }

    @Test
    void testRangeScanWithPartitionKey() throws Exception {
        try (SyncOxiaClient syncClient =
                OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient()) {
            syncClient.put("range-scan-pkey-a", "0".getBytes(), Set.of(PutOption.PartitionKey("x")));
            syncClient.put("range-scan-pkey-b", "1".getBytes(), Set.of(PutOption.PartitionKey("x")));
            syncClient.put("range-scan-pkey-c", "2".getBytes(), Set.of(PutOption.PartitionKey("x")));
            syncClient.put("range-scan-pkey-d", "3".getBytes(), Set.of(PutOption.PartitionKey("x")));
            syncClient.put("range-scan-pkey-e", "4".getBytes(), Set.of(PutOption.PartitionKey("x")));
            syncClient.put("range-scan-pkey-f", "5".getBytes(), Set.of(PutOption.PartitionKey("x")));
            syncClient.put("range-scan-pkey-g", "6".getBytes(), Set.of(PutOption.PartitionKey("x")));

            Iterable<GetResult> iterable =
                    syncClient.rangeScan(
                            "range-scan-pkey-a", "range-scan-pkey-d", Set.of(RangeScanOption.PartitionKey("x")));

            List<String> keys =
                    StreamSupport.stream(iterable.spliterator(), false).map(GetResult::key).toList();
            assertThat(keys)
                    .containsExactly("range-scan-pkey-a", "range-scan-pkey-b", "range-scan-pkey-c");
        }
    }

    @Test
    void testRangeScan() throws Exception {
        try (SyncOxiaClient syncClient =
                OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient()) {
            syncClient.put("range-scan-a", "0".getBytes());
            syncClient.put("range-scan-b", "1".getBytes());
            syncClient.put("range-scan-c", "2".getBytes());
            syncClient.put("range-scan-d", "3".getBytes());
            syncClient.put("range-scan-e", "4".getBytes());
            syncClient.put("range-scan-f", "5".getBytes());
            syncClient.put("range-scan-g", "6".getBytes());

            Iterable<GetResult> iterable = syncClient.rangeScan("range-scan-a", "range-scan-d");

            List<String> keys =
                    StreamSupport.stream(iterable.spliterator(), false).map(GetResult::key).sorted().toList();
            assertThat(keys).containsExactly("range-scan-a", "range-scan-b", "range-scan-c");

            List<String> sameKeys =
                    StreamSupport.stream(iterable.spliterator(), false).map(GetResult::key).sorted().toList();
            assertThat(sameKeys).isEqualTo(keys);
        }
    }

    @Test
    void testSequenceBatching() {
        int testNum = 50;

        List<CompletableFuture<PutResult>> resultList = new ArrayList<>();
        for (int i = 1; i <= testNum; i++) {
            byte[] value = ("message-" + i).getBytes();
            resultList.add(
                    client.put(
                            "idx",
                            value,
                            Set.of(PutOption.PartitionKey("ids"), PutOption.SequenceKeysDeltas(List.of(1L)))));
        }

        CompletableFuture.allOf(resultList.toArray(new CompletableFuture[0])).join();

        for (int i = 0; i < testNum; i++) {
            PutResult result = resultList.get(i).join();
            GetResult getResult = client.get(result.key(), Set.of(GetOption.PartitionKey("ids"))).join();

            assertThat(result.key()).isEqualTo(String.format("idx-%020d", i + 1));
            assertThat(new String(getResult.value())).isEqualTo("message-" + (i + 1));
        }
    }

    @Test
    void testSecondaryIndex() throws Exception {
        try (SyncOxiaClient syncClient =
                OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient()) {
            syncClient.put("si-a", "0".getBytes(), Set.of(PutOption.SecondaryIndex("val", "0")));
            syncClient.put("si-b", "1".getBytes(), Set.of(PutOption.SecondaryIndex("val", "1")));
            syncClient.put("si-c", "2".getBytes(), Set.of(PutOption.SecondaryIndex("val", "2")));
            syncClient.put("si-d", "3".getBytes(), Set.of(PutOption.SecondaryIndex("val", "3")));
            syncClient.put("si-e", "4".getBytes(), Set.of(PutOption.SecondaryIndex("val", "4")));
            syncClient.put("si-f", "5".getBytes(), Set.of(PutOption.SecondaryIndex("val", "5")));
            syncClient.put("si-g", "6".getBytes(), Set.of(PutOption.SecondaryIndex("val", "6")));

            List<String> keys = syncClient.list("1", "4", Set.of(ListOption.UseIndex("val")));
            assertThat(keys).containsExactly("si-b", "si-c", "si-d");

            Iterable<GetResult> iterable =
                    syncClient.rangeScan("2", "5", Set.of(RangeScanOption.UseIndex("val")));
            keys =
                    StreamSupport.stream(iterable.spliterator(), false).map(GetResult::key).sorted().toList();
            assertThat(keys).containsExactly("si-c", "si-d", "si-e");

            syncClient.delete("si-b");

            keys = syncClient.list("0", "3", Set.of(ListOption.UseIndex("val")));
            assertThat(keys).containsExactly("si-a", "si-c");

            iterable = syncClient.rangeScan("0", "3", Set.of(RangeScanOption.UseIndex("val")));
            keys =
                    StreamSupport.stream(iterable.spliterator(), false).map(GetResult::key).sorted().toList();
            assertThat(keys).containsExactly("si-a", "si-c");
        }
    }

    @Test
    void testGetIncludeValue() throws Exception {
        try (SyncOxiaClient syncClient =
                OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient()) {
            String key = "stream";

            List<String> keys = new ArrayList<>();
            PutResult putResult =
                    syncClient.put(
                            key,
                            UUID.randomUUID().toString().getBytes(),
                            Set.of(PutOption.PartitionKey(key), PutOption.SequenceKeysDeltas(List.of(1L))));
            keys.add(putResult.key());
            putResult =
                    syncClient.put(
                            key,
                            UUID.randomUUID().toString().getBytes(),
                            Set.of(PutOption.PartitionKey(key), PutOption.SequenceKeysDeltas(List.of(1L))));
            keys.add(putResult.key());

            for (String subKey : keys) {
                GetResult result =
                        syncClient.get(subKey, Set.of(GetOption.PartitionKey(key), GetOption.IncludeValue));
                Assertions.assertNotNull(result.value());

                result =
                        syncClient.get(subKey, Set.of(GetOption.PartitionKey(key), GetOption.ExcludeValue));
                Assertions.assertEquals(0, result.value().length);
            }

            GetResult result =
                    syncClient.get(
                            keys.get(0),
                            Set.of(
                                    GetOption.PartitionKey(key), GetOption.ExcludeValue, GetOption.ComparisonHigher));
            Assertions.assertEquals(0, result.value().length);
            Assertions.assertEquals(keys.get(1), result.key());

            result =
                    syncClient.get(
                            keys.get(1),
                            Set.of(
                                    GetOption.PartitionKey(key), GetOption.ExcludeValue, GetOption.ComparisonLower));
            Assertions.assertEquals(0, result.value().length);
            Assertions.assertEquals(keys.get(0), result.key());
        }
    }

    @Test
    void testSecondaryIndexGet() throws Exception {
        try (SyncOxiaClient syncClient =
                OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient()) {
            String prefix = "si-get-" + UUID.randomUUID();

            for (int i = 1; i < 10; i++) {
                String primaryKey = String.format("%s-%c", prefix, 'a' + i);
                String value = String.format("%03d", i);

                syncClient.put(
                        primaryKey, value.getBytes(), Set.of(PutOption.SecondaryIndex("val-idx", value)));
            }

            GetResult getResult = syncClient.get("000", Set.of(GetOption.UseIndex("val-idx")));
            assertThat(getResult).isNull();

            getResult = syncClient.get("001", Set.of(GetOption.UseIndex("val-idx")));
            assertThat(getResult).isNotNull();
            assertThat(getResult.key()).isEqualTo(prefix + "-b");
            assertThat(getResult.value()).isEqualTo("001".getBytes());

            getResult = syncClient.get("005", Set.of(GetOption.UseIndex("val-idx")));
            assertThat(getResult).isNotNull();
            assertThat(getResult.key()).isEqualTo(prefix + "-f");
            assertThat(getResult.value()).isEqualTo("005".getBytes());

            getResult = syncClient.get("009", Set.of(GetOption.UseIndex("val-idx")));
            assertThat(getResult).isNotNull();
            assertThat(getResult.key()).isEqualTo(prefix + "-j");
            assertThat(getResult.value()).isEqualTo("009".getBytes());

            getResult = syncClient.get("999", Set.of(GetOption.UseIndex("val-idx")));
            assertThat(getResult).isNull();

            getResult =
                    syncClient.get("000", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonLower));
            assertThat(getResult).isNull();

            getResult =
                    syncClient.get("001", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonLower));
            assertThat(getResult).isNull();

            getResult =
                    syncClient.get("005", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonLower));
            assertThat(getResult).isNotNull();
            assertThat(getResult.key()).isEqualTo(prefix + "-e");
            assertThat(getResult.value()).isEqualTo("004".getBytes());

            getResult =
                    syncClient.get("009", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonLower));
            assertThat(getResult).isNotNull();
            assertThat(getResult.key()).isEqualTo(prefix + "-i");
            assertThat(getResult.value()).isEqualTo("008".getBytes());

            getResult =
                    syncClient.get("999", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonLower));
            assertThat(getResult).isNotNull();
            assertThat(getResult.key()).isEqualTo(prefix + "-j");
            assertThat(getResult.value()).isEqualTo("009".getBytes());

            getResult =
                    syncClient.get("000", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonHigher));
            assertThat(getResult).isNotNull();
            assertThat(getResult.key()).isEqualTo(prefix + "-b");
            assertThat(getResult.value()).isEqualTo("001".getBytes());

            getResult =
                    syncClient.get("001", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonHigher));
            assertThat(getResult).isNotNull();
            assertThat(getResult.key()).isEqualTo(prefix + "-c");
            assertThat(getResult.value()).isEqualTo("002".getBytes());

            getResult =
                    syncClient.get("005", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonHigher));
            assertThat(getResult).isNotNull();
            assertThat(getResult.key()).isEqualTo(prefix + "-g");
            assertThat(getResult.value()).isEqualTo("006".getBytes());

            getResult =
                    syncClient.get("009", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonHigher));
            assertThat(getResult).isNull();

            getResult =
                    syncClient.get("999", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonHigher));
            assertThat(getResult).isNull();

            getResult =
                    syncClient.get("000", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonCeiling));
            assertThat(getResult).isNotNull();
            assertThat(getResult.key()).isEqualTo(prefix + "-b");
            assertThat(getResult.value()).isEqualTo("001".getBytes());

            getResult =
                    syncClient.get("001", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonCeiling));
            assertThat(getResult).isNotNull();
            assertThat(getResult.key()).isEqualTo(prefix + "-b");
            assertThat(getResult.value()).isEqualTo("001".getBytes());

            getResult =
                    syncClient.get("005", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonCeiling));
            assertThat(getResult).isNotNull();
            assertThat(getResult.key()).isEqualTo(prefix + "-f");
            assertThat(getResult.value()).isEqualTo("005".getBytes());

            getResult =
                    syncClient.get("009", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonCeiling));
            assertThat(getResult).isNotNull();
            assertThat(getResult.key()).isEqualTo(prefix + "-j");
            assertThat(getResult.value()).isEqualTo("009".getBytes());

            getResult =
                    syncClient.get("999", Set.of(GetOption.UseIndex("val-idx"), GetOption.ComparisonCeiling));
            assertThat(getResult).isNull();
        }
    }

    @Test
    void testGetSequenceUpdates() throws Exception {
        try (SyncOxiaClient syncClient =
                OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient()) {
            String key = "su-" + UUID.randomUUID();

            assertThatThrownBy(() -> syncClient.getSequenceUpdates(key, update -> {}, Set.of()))
                    .isInstanceOf(IllegalArgumentException.class);

            ArrayBlockingQueue<String> updates1 = new ArrayBlockingQueue<>(100);
            AutoCloseable closer1 =
                    syncClient.getSequenceUpdates(
                            key, updates1::add, Set.of(GetSequenceUpdatesOption.PartitionKey("x")));
            assertThat(closer1).isNotNull();

            assertThat(updates1.poll(1, TimeUnit.SECONDS)).isNull();

            PutResult putResult =
                    syncClient.put(
                            key,
                            "0".getBytes(),
                            Set.of(PutOption.PartitionKey("x"), PutOption.SequenceKeysDeltas(List.of(1L))));
            assertThat(putResult.key()).isEqualTo(String.format(key + "-%020d", 1));

            assertThat(updates1.poll(1, TimeUnit.SECONDS)).isEqualTo(putResult.key());
            closer1.close();

            PutResult putResult2 =
                    syncClient.put(
                            key,
                            "0".getBytes(),
                            Set.of(PutOption.PartitionKey("x"), PutOption.SequenceKeysDeltas(List.of(1L))));

            assertThat(updates1.poll(1, TimeUnit.SECONDS)).isNull();

            ArrayBlockingQueue<String> updates2 = new ArrayBlockingQueue<>(100);
            AutoCloseable closer2 =
                    syncClient.getSequenceUpdates(
                            key, updates2::add, Set.of(GetSequenceUpdatesOption.PartitionKey("x")));
            try {
                assertThat(updates2.poll(1, TimeUnit.SECONDS)).isEqualTo(putResult2.key());

                PutResult putResult3 =
                        syncClient.put(
                                key,
                                "0".getBytes(),
                                Set.of(PutOption.PartitionKey("x"), PutOption.SequenceKeysDeltas(List.of(1L))));
                PutResult putResult4 =
                        syncClient.put(
                                key,
                                "0".getBytes(),
                                Set.of(PutOption.PartitionKey("x"), PutOption.SequenceKeysDeltas(List.of(1L))));

                assertThat(updates2.poll(1, TimeUnit.SECONDS)).isEqualTo(putResult3.key());
                assertThat(updates2.poll(1, TimeUnit.SECONDS)).isEqualTo(putResult4.key());
                assertThat(updates2.poll(1, TimeUnit.SECONDS)).isNull();
            } finally {
                closer2.close();
            }
        }
    }

    @Test
    void testMaximumSize() throws Exception {
        try (SyncOxiaClient syncClient =
                OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient()) {
            String key = "key-" + UUID.randomUUID();
            byte[] payload = new byte[50 * 1024 * 1024];
            syncClient.put(key, payload);
            syncClient.get(key);

            key = "key-" + UUID.randomUUID();
            payload = new byte[MAXIMUM_FRAME_SIZE];
            String finalKey = key;
            byte[] finalPayload = payload;
            assertThatThrownBy(() -> syncClient.put(finalKey, finalPayload))
                    .isInstanceOf(StatusRuntimeException.class);
        }
    }
}
