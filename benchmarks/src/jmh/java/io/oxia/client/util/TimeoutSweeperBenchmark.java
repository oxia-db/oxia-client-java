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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares {@link TimeoutSweeper} against the {@link CompletableFuture#orTimeout} it replaced, for
 * an operation that completes well before its timeout. Run with {@code ./gradlew :benchmarks:jmh};
 * the threads contend on the JVM-wide delayer with {@code orTimeout}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(8)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
public class TimeoutSweeperBenchmark {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final Object RESULT = new Object();

    private ScheduledExecutorService executor;
    private TimeoutSweeper sweeper;

    @Setup
    public void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
        sweeper = new TimeoutSweeper(executor, TIMEOUT);
    }

    @TearDown
    public void tearDown() {
        sweeper.close();
        executor.shutdownNow();
    }

    @Benchmark
    public CompletableFuture<Object> orTimeout() {
        var future =
                new CompletableFuture<Object>().orTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        future.complete(RESULT);
        return future;
    }

    @Benchmark
    public CompletableFuture<Object> timeoutSweeper() {
        var future = sweeper.add(new CompletableFuture<>());
        future.complete(RESULT);
        return future;
    }
}
