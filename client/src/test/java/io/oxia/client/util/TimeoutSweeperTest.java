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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class TimeoutSweeperTest {

    private static final long TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10);

    // The mocked executor never runs the periodic sweep: the tests sweep by hand, at chosen times
    private final ScheduledExecutorService executor =
            mock(ScheduledExecutorService.class, RETURNS_MOCKS);
    private final TimeoutSweeper sweeper =
            new TimeoutSweeper(executor, Duration.ofNanos(TIMEOUT_NANOS));

    @Test
    void failsPendingFutureAtTheDeadline() {
        var future = sweeper.add(new CompletableFuture<String>());

        // The timeout starts at the first sweep after the future was added
        sweeper.sweep(0);
        sweeper.sweep(TIMEOUT_NANOS - 1);
        assertThat(future).isNotDone();

        sweeper.sweep(TIMEOUT_NANOS);
        assertThatThrownBy(future::join).hasCauseInstanceOf(TimeoutException.class);
        assertThat(sweeper.pendingCount()).isZero();
    }

    @Test
    void dropsCompletedFutures() {
        sweeper.add(CompletableFuture.completedFuture("already done"));
        var completed = sweeper.add(new CompletableFuture<String>());
        var pending = sweeper.add(new CompletableFuture<String>());
        assertThat(sweeper.pendingCount()).isEqualTo(2);

        sweeper.sweep(0);
        completed.complete("done");
        sweeper.sweep(1);
        assertThat(sweeper.pendingCount()).isEqualTo(1);

        sweeper.sweep(TIMEOUT_NANOS);
        assertThat(completed).isCompletedWithValue("done");
        assertThatThrownBy(pending::join).hasCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void addingThreadDropsCompletedFutures() {
        List<CompletableFuture<String>> pending = new ArrayList<>();
        for (int i = 0; i < 10 * TimeoutSweeper.MIN_COMPACTION_SIZE; i++) {
            var future = sweeper.add(new CompletableFuture<String>());
            if (i % 100 == 0) {
                pending.add(future);
            } else {
                future.complete("done");
            }
        }
        // Bounded by the pending futures, even without a sweep
        assertThat(sweeper.pendingCount()).isLessThan(TimeoutSweeper.MIN_COMPACTION_SIZE);

        sweeper.sweep(0);
        assertThat(sweeper.pendingCount()).isEqualTo(pending.size());
        sweeper.sweep(TIMEOUT_NANOS);
        assertThat(pending)
                .allSatisfy(f -> assertThatThrownBy(f::join).hasCauseInstanceOf(TimeoutException.class));
    }

    @Test
    void sweepIntervalIsATenthOfTheTimeoutUpTo100Millis() {
        new TimeoutSweeper(executor, Duration.ofMillis(500));

        verify(executor)
                .scheduleWithFixedDelay(
                        any(),
                        eq(TimeUnit.MILLISECONDS.toNanos(50)),
                        eq(TimeUnit.MILLISECONDS.toNanos(50)),
                        eq(TimeUnit.NANOSECONDS));
        // The 10 seconds timeout of the sweeper under test
        verify(executor)
                .scheduleWithFixedDelay(
                        any(),
                        eq(TimeUnit.MILLISECONDS.toNanos(100)),
                        eq(TimeUnit.MILLISECONDS.toNanos(100)),
                        eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void closeKeepsTheDeadlineOfPendingFutures() {
        var future = sweeper.add(new CompletableFuture<String>());
        // Leave 200 ms before the deadline
        sweeper.sweep(System.nanoTime() - TIMEOUT_NANOS + TimeUnit.MILLISECONDS.toNanos(200));

        sweeper.close();
        assertThat(future).isNotDone();
        await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(
                        () -> assertThatThrownBy(future::join).hasCauseInstanceOf(TimeoutException.class));
    }

    @Test
    void futureAddedAfterCloseStillTimesOut() {
        var closedSweeper = new TimeoutSweeper(executor, Duration.ofMillis(200));
        closedSweeper.close();

        var future = closedSweeper.add(new CompletableFuture<String>());
        assertThat(future).isNotDone();
        await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(
                        () -> assertThatThrownBy(future::join).hasCauseInstanceOf(TimeoutException.class));
    }

    @Test
    void periodicSweepFailsPendingFutureNoEarlierThanTheTimeout() {
        var realExecutor = Executors.newSingleThreadScheduledExecutor();
        try (var realSweeper = new TimeoutSweeper(realExecutor, Duration.ofMillis(200))) {
            var failedAt = new AtomicLong();
            long start = System.nanoTime();
            var future = realSweeper.add(new CompletableFuture<String>());
            future.whenComplete((v, e) -> failedAt.set(System.nanoTime()));

            await().atMost(Duration.ofSeconds(5)).until(() -> failedAt.get() != 0);
            assertThatThrownBy(future::join).hasCauseInstanceOf(TimeoutException.class);
            assertThat(failedAt.get() - start).isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(200));
        } finally {
            realExecutor.shutdownNow();
        }
    }
}
