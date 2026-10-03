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

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.NonNull;

/**
 * Fails the futures added to it with a {@link TimeoutException} when they are still pending after
 * the timeout, like {@link CompletableFuture#orTimeout}, but without its cost: {@code orTimeout}
 * schedules and then cancels a task on the single JVM-wide delayer for every future, taking its
 * lock twice. Here a future is appended to a list picked by the calling thread, so that threads
 * rarely contend, and a periodic sweep on the given executor fails the futures that expired.
 *
 * <p>The adding threads drop the completed futures from their list whenever it doubles in size, so
 * the memory is bounded by the pending futures rather than by the rate at which they are added.
 *
 * <p>The timeout is approximate: a future fails between the timeout and the timeout plus two sweep
 * intervals after it was added, never earlier. The sweep interval is a tenth of the timeout, and at
 * most 100 ms.
 */
public final class TimeoutSweeper implements AutoCloseable {

    private static final long MAX_SWEEP_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    @VisibleForTesting static final int MIN_COMPACTION_SIZE = 1024;

    private record Generation(long deadlineNanos, List<CompletableFuture<?>> futures) {}

    /** The futures added since the last sweep by the threads that map to this stripe. */
    private static final class Stripe {
        private List<CompletableFuture<?>> futures = new ArrayList<>();
        private int compactionSize = MIN_COMPACTION_SIZE;

        synchronized void add(CompletableFuture<?> future) {
            futures.add(future);
            if (futures.size() >= compactionSize) {
                futures.removeIf(CompletableFuture::isDone);
                compactionSize = Math.max(futures.size() * 2, MIN_COMPACTION_SIZE);
            }
        }

        synchronized List<CompletableFuture<?>> takeAll() {
            if (futures.isEmpty()) {
                return List.of();
            }
            final List<CompletableFuture<?>> taken = futures;
            futures = new ArrayList<>();
            compactionSize = MIN_COMPACTION_SIZE;
            return taken;
        }

        synchronized int size() {
            return futures.size();
        }
    }

    private final long timeoutNanos;
    private final Stripe[] stripes;
    private final ScheduledFuture<?> sweepTask;
    // The futures taken by each sweep, oldest first. Guarded by this.
    private final ArrayDeque<Generation> generations = new ArrayDeque<>();
    private volatile boolean closed;

    public TimeoutSweeper(@NonNull ScheduledExecutorService executor, @NonNull Duration timeout) {
        this.timeoutNanos = timeout.toNanos();
        final int cpus = Runtime.getRuntime().availableProcessors();
        this.stripes = new Stripe[1 << (32 - Integer.numberOfLeadingZeros(cpus - 1))];
        for (int i = 0; i < stripes.length; i++) {
            stripes[i] = new Stripe();
        }
        final long intervalNanos = Math.min(Math.max(timeoutNanos / 10, 1), MAX_SWEEP_INTERVAL_NANOS);
        this.sweepTask =
                executor.scheduleWithFixedDelay(
                        () -> sweep(System.nanoTime()), intervalNanos, intervalNanos, TimeUnit.NANOSECONDS);
    }

    /** Fails the future with a {@link TimeoutException} if it is still pending after the timeout. */
    public <T> CompletableFuture<T> add(@NonNull CompletableFuture<T> future) {
        if (!future.isDone()) {
            stripes[(int) Thread.currentThread().getId() & (stripes.length - 1)].add(future);
            if (closed) {
                // The sweep no longer runs, and close() may have taken the stripes before this add
                handOverToOrTimeout();
            }
        }
        return future;
    }

    @VisibleForTesting
    void sweep(long nowNanos) {
        List<CompletableFuture<?>> expired = new ArrayList<>();
        synchronized (this) {
            if (closed) {
                return;
            }
            final Iterator<Generation> it = generations.iterator();
            while (it.hasNext()) {
                final Generation generation = it.next();
                if (nowNanos - generation.deadlineNanos() >= 0) {
                    expired.addAll(generation.futures());
                    it.remove();
                } else {
                    generation.futures().removeIf(CompletableFuture::isDone);
                    if (generation.futures().isEmpty()) {
                        it.remove();
                    }
                }
            }
            // The futures added since the last sweep start their timeout now: later than they were
            // added, so that they never time out early
            List<CompletableFuture<?>> futures = null;
            for (Stripe stripe : stripes) {
                for (CompletableFuture<?> future : stripe.takeAll()) {
                    if (!future.isDone()) {
                        if (futures == null) {
                            futures = new ArrayList<>();
                        }
                        futures.add(future);
                    }
                }
            }
            if (futures != null) {
                generations.addLast(new Generation(nowNanos + timeoutNanos, futures));
            }
        }
        // Outside the lock: completing a future runs its callbacks
        for (CompletableFuture<?> future : expired) {
            if (!future.isDone()) {
                future.completeExceptionally(new TimeoutException());
            }
        }
    }

    @VisibleForTesting
    synchronized int pendingCount() {
        int count = 0;
        for (Stripe stripe : stripes) {
            count += stripe.size();
        }
        for (Generation generation : generations) {
            count += generation.futures().size();
        }
        return count;
    }

    /**
     * Stops the sweep. The futures still pending keep their deadline through {@link
     * CompletableFuture#orTimeout}, as the owner usually shuts down the executor next.
     */
    @Override
    public void close() {
        sweepTask.cancel(false);
        synchronized (this) {
            closed = true;
            final long now = System.nanoTime();
            for (Generation generation : generations) {
                final long remainingNanos = Math.max(generation.deadlineNanos() - now, 0);
                generation.futures().forEach(f -> f.orTimeout(remainingNanos, TimeUnit.NANOSECONDS));
            }
            generations.clear();
        }
        handOverToOrTimeout();
    }

    private void handOverToOrTimeout() {
        for (Stripe stripe : stripes) {
            stripe.takeAll().forEach(f -> f.orTimeout(timeoutNanos, TimeUnit.NANOSECONDS));
        }
    }
}
