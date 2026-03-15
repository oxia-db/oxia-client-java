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
package io.oxia.client.util;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Monitors threads for blocking operations, similar to Vert.x's BlockedThreadChecker.
 *
 * <p>When the gRPC channel is configured with a direct executor, callbacks run on Netty I/O
 * threads. If a callback blocks for too long, it stalls all gRPC I/O on that channel. This checker
 * periodically inspects registered threads and logs a warning (with stack trace) when a thread has
 * been executing a task longer than the configured threshold.
 *
 * <p>Controlled via JVM system properties:
 *
 * <ul>
 *   <li>{@code -Doxia.client.blockedThreadChecker.enabled=true} — enable the checker (default:
 *       disabled)
 *   <li>{@code -Doxia.client.blockedThreadChecker.intervalMs=1000} — check interval in
 *       milliseconds (default: 1000)
 *   <li>{@code -Doxia.client.blockedThreadChecker.warnThresholdMs=500} — warning threshold in
 *       milliseconds (default: 500)
 * </ul>
 */
@Slf4j
public class BlockedThreadChecker implements AutoCloseable {

    static final String PROP_ENABLED = "oxia.client.blockedThreadChecker.enabled";
    static final String PROP_INTERVAL_MS = "oxia.client.blockedThreadChecker.intervalMs";
    static final String PROP_WARN_THRESHOLD_MS = "oxia.client.blockedThreadChecker.warnThresholdMs";

    static final long DEFAULT_CHECK_INTERVAL_MS = 1000;
    static final long DEFAULT_WARN_THRESHOLD_MS = 500;

    private final ScheduledExecutorService scheduler;
    private final Map<Thread, TaskExecution> trackedThreads = new ConcurrentHashMap<>();
    private final long warnThresholdNanos;
    private volatile boolean closed;

    /** Returns {@code true} if the checker is enabled via system property. */
    public static boolean isEnabled() {
        return Boolean.getBoolean(PROP_ENABLED);
    }

    /**
     * Creates a checker configured from system properties, or {@code null} if not enabled. Call
     * sites should use {@link #createIfEnabled()} and null-check.
     */
    public static BlockedThreadChecker createIfEnabled() {
        if (!isEnabled()) {
            return null;
        }
        long intervalMs = getLongProperty(PROP_INTERVAL_MS, DEFAULT_CHECK_INTERVAL_MS);
        long warnMs = getLongProperty(PROP_WARN_THRESHOLD_MS, DEFAULT_WARN_THRESHOLD_MS);
        log.info(
                "Blocked thread checker enabled (interval={}ms, warnThreshold={}ms)",
                intervalMs,
                warnMs);
        return new BlockedThreadChecker(Duration.ofMillis(intervalMs), Duration.ofMillis(warnMs));
    }

    BlockedThreadChecker(Duration checkInterval, Duration warnThreshold) {
        this.warnThresholdNanos = warnThreshold.toNanos();
        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "oxia-blocked-thread-checker");
                            t.setDaemon(true);
                            return t;
                        });
        this.scheduler.scheduleAtFixedRate(
                this::checkAll,
                checkInterval.toMillis(),
                checkInterval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Creates an executor that wraps direct (caller-thread) execution with blocked-thread
     * monitoring. Tasks executed through this executor will be tracked, and a warning is logged if
     * any task exceeds the configured threshold.
     */
    public Executor checkedDirectExecutor() {
        return new CheckedDirectExecutor(this);
    }

    void taskStarted(Thread thread) {
        trackedThreads.put(thread, new TaskExecution(System.nanoTime()));
    }

    void taskFinished(Thread thread) {
        trackedThreads.remove(thread);
    }

    private void checkAll() {
        if (closed) {
            return;
        }
        long now = System.nanoTime();
        trackedThreads.forEach(
                (thread, execution) -> {
                    long elapsed = now - execution.startNanos();
                    if (elapsed > warnThresholdNanos && execution.shouldWarn(now)) {
                        log.warn(
                                "Thread {} has been blocked for {} ms (threshold: {} ms)",
                                thread.getName(),
                                elapsed / 1_000_000,
                                warnThresholdNanos / 1_000_000,
                                new BlockedThreadException(thread));
                    }
                });
    }

    @Override
    public void close() {
        closed = true;
        scheduler.shutdownNow();
        trackedThreads.clear();
    }

    private static long getLongProperty(String key, long defaultValue) {
        String value = System.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            log.warn("Invalid value for system property {}: '{}', using default {}", key, value, defaultValue);
            return defaultValue;
        }
    }

    private static class TaskExecution {
        private static final long WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);

        private final long startNanos;
        private final AtomicLong lastWarnedNanos = new AtomicLong(0);

        TaskExecution(long startNanos) {
            this.startNanos = startNanos;
        }

        long startNanos() {
            return startNanos;
        }

        /**
         * Returns true if enough time has passed since the last warning. Warns on first detection,
         * then at most once every 5 seconds to avoid log spam.
         */
        boolean shouldWarn(long nowNanos) {
            long lastWarned = lastWarnedNanos.get();
            if (lastWarned == 0 || (nowNanos - lastWarned) >= WARN_INTERVAL_NANOS) {
                return lastWarnedNanos.compareAndSet(lastWarned, nowNanos);
            }
            return false;
        }
    }

    /**
     * Exception used to capture the stack trace of a blocked thread. Not thrown — only used for
     * diagnostic logging.
     */
    public static class BlockedThreadException extends Exception {
        BlockedThreadException(Thread thread) {
            super("Thread " + thread.getName() + " blocked");
            setStackTrace(thread.getStackTrace());
        }
    }

    /**
     * An executor that runs tasks on the calling thread (like gRPC's directExecutor) but tracks
     * execution time via the {@link BlockedThreadChecker}.
     */
    private static class CheckedDirectExecutor implements Executor {
        private final BlockedThreadChecker checker;

        CheckedDirectExecutor(BlockedThreadChecker checker) {
            this.checker = checker;
        }

        @Override
        public void execute(Runnable command) {
            Thread current = Thread.currentThread();
            checker.taskStarted(current);
            try {
                command.run();
            } finally {
                checker.taskFinished(current);
            }
        }
    }
}
