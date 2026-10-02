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

import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.options.PutOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Both children of a split shard inherit the sessions of the parent, with the ephemeral records of
 * each session in their hash range. The records must live as long as the client that wrote them,
 * and go away when it closes. Splitting a shard needs a coordinator, so the cluster runs a
 * coordinator and a data server.
 */
@Testcontainers
class ShardSplitSessionIT {
    private static final int PUBLIC_PORT = 6648;
    private static final int METRICS_PORT = 8080;

    // Shard 0 is split into shard 1, with the hashes up to the split point, and shard 2
    private static final long SPLIT_POINT = (1L << 31) - 1;
    private static final Duration SESSION_TIMEOUT = Duration.ofSeconds(10);
    private static final byte[] VALUE = "value".getBytes(UTF_8);

    private static final Network network = Network.newNetwork();

    @Container
    private static final GenericContainer<?> dataServer =
            new GenericContainer<>(OxiaImages.OXIA)
                    .withNetwork(network)
                    .withNetworkAliases("dataserver")
                    .withCommand("oxia", "server")
                    .withExposedPorts(PUBLIC_PORT, METRICS_PORT)
                    .waitingFor(Wait.forHttp("/metrics").forPort(METRICS_PORT).forStatusCode(200))
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("dataserver")));

    @Container
    private static final GenericContainer<?> coordinator =
            new GenericContainer<>(OxiaImages.OXIA)
                    .withNetwork(network)
                    .withCommand("oxia", "coordinator", "--metadata", "memory")
                    .withExposedPorts(METRICS_PORT)
                    .waitingFor(Wait.forHttp("/metrics").forPort(METRICS_PORT).forStatusCode(200))
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("coordinator")));

    @BeforeAll
    static void createNamespace() throws Exception {
        // The clients connect to the public address, the coordinator to the internal one
        admin(
                "dataserver",
                "create",
                "dataserver",
                "--public",
                serviceAddress(),
                "--internal",
                "dataserver:6649");
        admin("namespace", "create", "default", "--initial-shards", "1", "--replication-factor", "1");
    }

    @Test
    void keepsTheEphemeralRecordsOfASplitShardUntilTheClientCloses() throws Exception {
        var keys = new ArrayList<String>();
        try (var client =
                OxiaClientBuilder.create(serviceAddress()).sessionTimeout(SESSION_TIMEOUT).syncClient()) {
            long sessionId = 0;
            for (int i = 0; i < 20; i++) {
                var key = String.format("ephemeral-%04d", i);
                sessionId = putEphemeral(client, key);
                keys.add(key);
            }
            assertThat(keys)
                    .as("the records must be in both children")
                    .anyMatch(key -> hash(key) <= SPLIT_POINT)
                    .anyMatch(key -> hash(key) > SPLIT_POINT);

            assertThat(
                            admin(
                                    "shard",
                                    "split",
                                    "--namespace",
                                    "default",
                                    "--shard",
                                    "0",
                                    "--split-point",
                                    Long.toString(SPLIT_POINT)))
                    .contains("\"Error\":null");
            await().atMost(Duration.ofSeconds(30)).until(ShardSplitSessionIT::splitCompleted);

            // The children start to count the timeout of the sessions they inherit when they are
            // elected, before the split completes
            Thread.sleep(SESSION_TIMEOUT.multipliedBy(2).toMillis());

            for (var key : keys) {
                var result = client.get(key);
                assertThat(result)
                        .as("ephemeral record %s lost while its client is alive", key)
                        .isNotNull();
                assertThat(result.version().sessionId()).hasValue(sessionId);
            }

            // The new ephemeral records of the client on the children belong to the session they
            // inherited
            for (var key : List.of(keyIn(0, SPLIT_POINT), keyIn(SPLIT_POINT + 1, (1L << 32) - 1))) {
                assertThat(putEphemeral(client, key)).isEqualTo(sessionId);
                keys.add(key);
            }
        }

        // Closing the client closed the session on the children, which deleted its records without
        // waiting for it to expire
        try (var reader = OxiaClientBuilder.create(serviceAddress()).syncClient()) {
            await()
                    .atMost(SESSION_TIMEOUT.dividedBy(2))
                    .untilAsserted(
                            () -> assertThat(keys).allSatisfy(key -> assertThat(reader.get(key)).isNull()));
        }
    }

    // Returns the id of the session of the record
    private static long putEphemeral(SyncOxiaClient client, String key) throws Exception {
        return client
                .put(key, VALUE, Set.of(PutOption.AsEphemeralRecord))
                .version()
                .sessionId()
                .orElseThrow();
    }

    // The split is complete once the namespace has only the children of the split shard
    private static boolean splitCompleted() throws Exception {
        var status = admin("namespace", "get", "default", "-o", "json");
        return !status.contains("\"0\": {") && !status.contains("\"split\"");
    }

    private static long hash(String key) {
        return LongHashFunction.xx3().hashBytes(key.getBytes(UTF_8)) & 0xFFFFFFFFL;
    }

    private static String keyIn(long minHash, long maxHash) {
        for (int i = 0; ; i++) {
            var key = "post-split-" + i;
            if (minHash <= hash(key) && hash(key) <= maxHash) {
                return key;
            }
        }
    }

    private static String serviceAddress() {
        return dataServer.getHost() + ":" + dataServer.getMappedPort(PUBLIC_PORT);
    }

    private static String admin(String... args) throws Exception {
        var command = Stream.concat(Stream.of("oxia", "admin"), Stream.of(args)).toArray(String[]::new);
        var result = coordinator.execInContainer(command);
        assertThat(result.getExitCode()).as(result.getStderr()).isZero();
        return result.getStdout();
    }
}
