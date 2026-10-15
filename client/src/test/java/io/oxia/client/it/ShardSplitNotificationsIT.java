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
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification.KeyCreated;
import io.oxia.client.api.Notification.KeyDeleted;
import io.oxia.client.api.Notification.KeyModified;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.Version;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * A subscription to the notifications created before a split gets the notification of every write
 * once across it: the ones of the split shard that it hadn't read when the split completed, which
 * the children serve from the last batch it got from the split shard, and the ones of the writes to
 * each child afterwards. Splitting a shard needs a coordinator, so the cluster runs a coordinator
 * and a data server.
 */
@Testcontainers
class ShardSplitNotificationsIT {
    private static final Logger log = LoggerFactory.getLogger(ShardSplitNotificationsIT.class);

    private static final int PUBLIC_PORT = 6648;
    private static final int METRICS_PORT = 8080;

    // Shard 0 is split into shard 1, with the hashes up to the split point, and shard 2
    private static final long SPLIT_POINT = (1L << 31) - 1;
    private static final long MAX_HASH = (1L << 32) - 1;

    // The notifications of the writes that the subscription doesn't read fill its queue and, with
    // their long keys, the flow-control window of its stream (1 MiB): the rest of them are still on
    // the split shard when the split completes
    private static final int UNREAD_WRITES = 3000;
    private static final String PADDING = "x".repeat(1000);

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

    // The notifications of the acknowledged writes
    private final Queue<Notification> expected = new ConcurrentLinkedQueue<>();
    private final Map<Notification, Integer> received = new HashMap<>();
    // The subscription stops reading while the queue is full
    private final BlockingQueue<Notification> notifications = new ArrayBlockingQueue<>(100);
    private volatile boolean closing;
    private AsyncOxiaClient writer;

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
    void deliversEveryNotificationOnceAcrossASplit() throws Exception {
        // The writes go through a client of their own: the subscription that doesn't read blocks the
        // connection of its client
        writer = OxiaClientBuilder.create(serviceAddress()).asyncClient().get(30, SECONDS);
        var subscriber = OxiaClientBuilder.create(serviceAddress()).asyncClient().get(30, SECONDS);
        try {
            subscriber.notifications(this::enqueue);
            awaitSubscription();

            // The subscription reads the notifications of these writes from the split shard
            for (int i = 0; i < 20; i++) {
                assertThat(put(String.format("before-%03d", i))).isTrue();
            }
            receive(Duration.ofSeconds(30), Duration.ZERO);
            assertThat(missing()).as("notifications before the split").isEmpty();

            // The subscription stops reading: the rest of the notifications of these writes are still
            // on the split shard when the split completes, with those of the writes during the split
            var unread = new ArrayList<CompletableFuture<Void>>();
            for (int i = 0; i < UNREAD_WRITES; i++) {
                var key = String.format("unread-%04d-%s", i, PADDING);
                unread.add(writer.put(key, VALUE).thenAccept(result -> written(key, result.version())));
            }
            CompletableFuture.allOf(unread.toArray(CompletableFuture[]::new)).get(60, SECONDS);
            var stop = new AtomicBoolean();
            var during =
                    new Thread(
                            () -> {
                                for (int i = 0; !stop.get(); i++) {
                                    var key = String.format("during-%05d", i);
                                    if (put(key) && i % 3 == 0 && put(key) && i % 2 == 0) {
                                        delete(key);
                                    }
                                    try {
                                        Thread.sleep(2);
                                    } catch (InterruptedException e) {
                                        return;
                                    }
                                }
                            });
            during.start();
            split();
            stop.set(true);
            during.join();

            // Writes to both children, of new records and of records of the split shard
            for (int i = 0; i < 30; i++) {
                for (var key :
                        List.of(
                                keyIn(0, SPLIT_POINT, "after-" + i),
                                keyIn(SPLIT_POINT + 1, MAX_HASH, "after-" + i))) {
                    untilAcknowledged(() -> put(key));
                    if (i % 3 == 0) {
                        untilAcknowledged(() -> put(key));
                    }
                    if (i % 4 == 0) {
                        untilAcknowledged(() -> delete(key));
                    }
                }
            }
            for (int i = 0; i < 20; i += 2) {
                var modified = String.format("before-%03d", i);
                var deleted = String.format("before-%03d", i + 1);
                untilAcknowledged(() -> put(modified));
                untilAcknowledged(() -> delete(deleted));
            }

            receive(Duration.ofSeconds(60), Duration.ofSeconds(2));

            var missing = missing();
            var duplicated =
                    received.entrySet().stream()
                            .filter(entry -> entry.getValue() > 1)
                            .map(entry -> describe(entry.getKey()) + ": " + entry.getValue() + " times")
                            .sorted()
                            .toList();
            log.info(
                    "{} notifications expected, {} received, {} missing, {} duplicated",
                    expected.size(),
                    received.size(),
                    missing.size(),
                    duplicated.size());
            assertThat(missing).as("notifications not received").isEmpty();
            assertThat(duplicated).as("notifications received more than once").isEmpty();
        } finally {
            // Unblocks the subscription, which the client waits for when it closes
            closing = true;
            notifications.clear();
            subscriber.close();
            writer.close();
        }
    }

    private void enqueue(Notification notification) {
        if (closing) {
            return;
        }
        try {
            notifications.put(notification);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // There is no signal that the subscription is established: it is once it gets a notification
    private void awaitSubscription() {
        // The writes fail until the shard has a leader: the client doesn't retry them
        await()
                .atMost(Duration.ofSeconds(30))
                .ignoreExceptions()
                .until(
                        () -> {
                            writer.put("warm-up", VALUE).get(10, SECONDS);
                            return notifications.poll(1, SECONDS) != null;
                        });
        notifications.clear();
    }

    // Writes the record and records its notification, if the write is acknowledged
    private boolean put(String key) {
        try {
            written(key, writer.put(key, VALUE).get(10, SECONDS).version());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void written(String key, Version version) {
        expected.add(
                version.modificationsCount() == 0
                        ? new KeyCreated(key, version.versionId())
                        : new KeyModified(key, version.versionId()));
    }

    // The writer may not know the children yet: the write is retried until it's acknowledged
    private static void untilAcknowledged(Callable<Boolean> write) {
        await().atMost(Duration.ofSeconds(30)).until(write);
    }

    // Deletes the record and records its notification, if the delete is acknowledged
    private boolean delete(String key) {
        try {
            if (writer.delete(key).get(10, SECONDS)) {
                expected.add(new KeyDeleted(key));
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // Reads the notifications until it got all the expected ones, and then for as long as quiet, to
    // catch duplicates
    private void receive(Duration timeout, Duration quiet) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        long quietDeadline = Long.MAX_VALUE;
        while (true) {
            if (quietDeadline == Long.MAX_VALUE && expected.stream().allMatch(received::containsKey)) {
                if (quiet.isZero()) {
                    return;
                }
                quietDeadline = System.nanoTime() + quiet.toNanos();
            }
            var notification =
                    notifications.poll(Math.min(deadline, quietDeadline) - System.nanoTime(), NANOSECONDS);
            if (notification == null) {
                return;
            }
            received.merge(notification, 1, Integer::sum);
        }
    }

    private List<String> missing() {
        return expected.stream()
                .filter(notification -> !received.containsKey(notification))
                .map(ShardSplitNotificationsIT::describe)
                .sorted()
                .toList();
    }

    private static String describe(Notification notification) {
        return notification.toString().replace("-" + PADDING, "");
    }

    private static void split() throws Exception {
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
        await().atMost(Duration.ofSeconds(60)).until(ShardSplitNotificationsIT::splitCompleted);
    }

    // The split is complete once the namespace has only the children of the split shard
    private static boolean splitCompleted() throws Exception {
        var status = admin("namespace", "get", "default", "-o", "json");
        return !status.contains("\"0\": {") && !status.contains("\"split\"");
    }

    private static long hash(String key) {
        return LongHashFunction.xx3().hashBytes(key.getBytes(UTF_8)) & 0xFFFFFFFFL;
    }

    private static String keyIn(long minHash, long maxHash, String prefix) {
        for (int i = 0; ; i++) {
            var key = prefix + "-" + i;
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
