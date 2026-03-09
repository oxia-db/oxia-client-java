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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Monitors threads for blocking operations, similar to Vert.x's BlockedThreadChecker.
 *
 * <p>When the gRPC channel is configured with a direct executor, callbacks run on Netty I/O
 * threads. If a callback blocks for too long, it stalls all gRPC I/O on that channel. This checker
 * periodically inspects registered threads and logs a warning (with stack trace) when a thread has
 * been executing a task longer than the configured threshold.
 */
@Slf4j
public class BlockedThreadChecker implements AutoCloseable {

    static final Duration DEFAULT_CHECK_INTERVAL = Duration.ofSeconds(1);
    static final Duration DEFAULT_WARN_THRESHOLD = Duration.ofMillis(500);

    private final Timer timer;
    private final Map<Thread, TaskExecution> trackedThreads = new ConcurrentHashMap<>();
    private final long warnThresholdNanos;
    private volatile boolean closed;

    public BlockedThreadChecker() {
        this(DEFAULT_CHECK_INTERVAL, DEFAULT_WARN_THRESHOLD);
    }

    public BlockedThreadChecker(Duration checkInterval, Duration warnThreshold) {
        this.warnThresholdNanos = warnThreshold.toNanos();
        this.timer = new Timer("oxia-blocked-thread-checker", true);
        this.timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        checkAll();
                    }
                },
                checkInterval.toMillis(),
                checkInterval.toMillis());
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
                    if (elapsed > warnThresholdNanos && execution.tryWarn()) {
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
        timer.cancel();
        trackedThreads.clear();
    }

    private static class TaskExecution {
        private final long startNanos;
        private final AtomicLong warnedAt = new AtomicLong(0);

        TaskExecution(long startNanos) {
            this.startNanos = startNanos;
        }

        long startNanos() {
            return startNanos;
        }

        /** Returns true only on the first warn for this execution, to avoid log spam. */
        boolean tryWarn() {
            return warnedAt.compareAndSet(0, System.nanoTime());
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
