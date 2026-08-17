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
package io.oxia.client.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class WatchdogTest {
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void cleanup() {
        executor.shutdownNow();
    }

    @Test
    void firesActionWhenIdlePastInterval() {
        var actions = new AtomicInteger();
        var watchdog = new Watchdog(executor, Duration.ofMillis(200));
        watchdog.start(actions::incrementAndGet);

        await().untilAsserted(() -> assertThat(actions.get()).isGreaterThanOrEqualTo(1));
    }

    @Test
    void doesNotFireWhileActivityContinues() throws Exception {
        var actions = new AtomicInteger();
        var watchdog = new Watchdog(executor, Duration.ofMillis(200));
        watchdog.start(actions::incrementAndGet);

        for (int i = 0; i < 10; i++) {
            Thread.sleep(50);
            watchdog.pet();
        }

        assertThat(actions.get()).isZero();
    }

    @Test
    void closeStopsTheWatchdog() throws Exception {
        var actions = new AtomicInteger();
        var watchdog = new Watchdog(executor, Duration.ofMillis(100));
        watchdog.start(actions::incrementAndGet);
        Thread.sleep(500);
        assertThat(actions.get()).isGreaterThanOrEqualTo(1);

        watchdog.close();
        var fired = actions.get();
        Thread.sleep(500);
        assertThat(actions.get()).isEqualTo(fired);
    }
}
