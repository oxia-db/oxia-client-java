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
package io.oxia.client.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
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
 * Compares recording into the OpenTelemetry SDK backed {@link Counter} and {@link LatencyHistogram}
 * against a plain {@link LongAdder}. All the threads record into the same series, as the client
 * does for a given operation type. The nested classes run the same benchmarks with a different
 * number of threads. Run with {@code ./gradlew :benchmarks:jmh}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
public abstract class MetricsBenchmark {

    /** The instruments also add oxia.namespace, and the histogram oxia.response.status. */
    private static final Attributes ATTRIBUTES =
            Attributes.builder()
                    .put("oxia.op", "put")
                    .put("oxia.batch.type", "write")
                    .put("oxia.client.id", "client-0")
                    .put("oxia.shard", "3")
                    .build();

    @State(Scope.Benchmark)
    public static class Instruments {
        SdkMeterProvider meterProvider;
        LongAdder longAdder;
        Counter counter;
        LatencyHistogram histogram;

        @Setup
        public void setup() {
            // The SDK hands out noop instruments unless a reader is registered
            meterProvider =
                    SdkMeterProvider.builder().registerMetricReader(InMemoryMetricReader.create()).build();
            var instrumentProvider =
                    new InstrumentProvider(
                            OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build(), "default");

            longAdder = new LongAdder();
            counter = instrumentProvider.newCounter("counter", Unit.Bytes, "counter", ATTRIBUTES);
            histogram = instrumentProvider.newLatencyHistogram("histogram", "histogram", ATTRIBUTES);
        }

        @TearDown
        public void tearDown() {
            meterProvider.close();
        }
    }

    /** Latencies spread across the histogram buckets, from 100us to ~400ms. */
    @State(Scope.Thread)
    public static class Values {
        private static final int MASK = 1023;

        private final long[] values = new long[MASK + 1];
        private int idx;

        @Setup
        public void setup() {
            var random = ThreadLocalRandom.current();
            for (int i = 0; i < values.length; i++) {
                values[i] = TimeUnit.MICROSECONDS.toNanos(100L << random.nextInt(12));
            }
        }

        long next() {
            return values[idx++ & MASK];
        }
    }

    @Benchmark
    public void longAdder(Instruments instruments, Values values) {
        instruments.longAdder.add(values.next());
    }

    @Benchmark
    public void counter(Instruments instruments, Values values) {
        instruments.counter.add(values.next());
    }

    @Benchmark
    public void latencyHistogram(Instruments instruments, Values values) {
        instruments.histogram.recordSuccess(values.next());
    }

    @Threads(1)
    public static class Threads01 extends MetricsBenchmark {}

    @Threads(4)
    public static class Threads04 extends MetricsBenchmark {}

    @Threads(8)
    public static class Threads08 extends MetricsBenchmark {}

    @Threads(16)
    public static class Threads16 extends MetricsBenchmark {}
}
