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

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;

/**
 * Fires a callback when no activity has been observed for a configurable interval.
 *
 * <p>Callers {@link #pet()} on every received message and register the callback to run when the
 * stream has been silent too long (for example to recreate a long-lived stream whose server-side
 * handler died without delivering a terminal event). The watchdog keeps running after the callback
 * fires, so a stream that repeatedly goes silent is refreshed again.
 */
public final class Watchdog {
    private final ScheduledExecutorService executor;
    private final long intervalNanos;
    private final AtomicBoolean started = new AtomicBoolean();

    private volatile long lastActivityNanos;
    private volatile ScheduledFuture<?> task;

    public Watchdog(@NonNull ScheduledExecutorService executor, @NonNull Duration interval) {
        this.executor = executor;
        this.intervalNanos = interval.toNanos();
        this.lastActivityNanos = System.nanoTime();
    }

    /** Records that a message was received, postponing the callback. */
    public void pet() {
        lastActivityNanos = System.nanoTime();
    }

    /** Starts the watchdog. The first invocation schedules the task; later calls are no-ops. */
    public void start(@NonNull Runnable action) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        task =
                executor.scheduleWithFixedDelay(
                        () -> checkForStaleActivity(action),
                        intervalNanos / 2,
                        intervalNanos / 2,
                        TimeUnit.NANOSECONDS);
    }

    /** Stops the watchdog. */
    public void close() {
        final var current = task;
        if (current != null) {
            current.cancel(false);
        }
    }

    private void checkForStaleActivity(@NonNull Runnable action) {
        final long lastActivity = lastActivityNanos;
        if (System.nanoTime() - lastActivity < intervalNanos) {
            return;
        }
        // Reset first so a callback that does not deliver messages cannot be triggered repeatedly
        // from the same stale activity timestamp.
        lastActivityNanos = System.nanoTime();
        action.run();
    }
}
