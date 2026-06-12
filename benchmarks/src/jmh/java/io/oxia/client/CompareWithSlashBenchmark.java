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
package io.oxia.client;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares {@link CompareWithSlash} against the substring-based implementation it replaced. The
 * sort benchmarks model the multi-shard list() result merge. Run with {@code ./gradlew
 * :benchmarks:jmh}; the GC profiler reports the per-operation allocations.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
public class CompareWithSlashBenchmark {

    /** The substring-based implementation that predates the index-based rewrite. */
    private static final Comparator<String> LEGACY =
            (a, b) -> {
                while (a.length() > 0 && b.length() > 0) {
                    var idxA = a.indexOf('/');
                    var idxB = b.indexOf('/');
                    if (idxA < 0 && idxB < 0) {
                        return Integer.compare(a.compareTo(b), 0);
                    } else if (idxA < 0) {
                        return -1;
                    } else if (idxB < 0) {
                        return +1;
                    }

                    var spanA = a.substring(0, idxA);
                    var spanB = b.substring(0, idxB);

                    var spanRes = Integer.compare(spanA.compareTo(spanB), 0);
                    if (spanRes != 0) {
                        return spanRes;
                    }

                    a = a.substring(idxA + 1);
                    b = b.substring(idxB + 1);
                }

                if (a.length() < b.length()) {
                    return -1;
                } else if (a.length() > 0) {
                    return +1;
                } else {
                    return 0;
                }
            };

    @Param({"1000", "100000"})
    int numKeys;

    private String[] keys;

    @Setup
    public void setup() {
        var random = new Random(0xacc7);
        keys = new String[numKeys];
        for (int i = 0; i < numKeys; i++) {
            // Multi-segment keys sharing prefixes, as a sorted list() merge would see them
            keys[i] =
                    "/registry/tenant-%02d/namespace-%02d/key-%08d"
                            .formatted(random.nextInt(10), random.nextInt(100), random.nextInt(1_000_000));
        }
    }

    @Benchmark
    public int pairwiseCompare() {
        int acc = 0;
        for (int i = 1; i < keys.length; i++) {
            acc += CompareWithSlash.INSTANCE.compare(keys[i - 1], keys[i]);
        }
        return acc;
    }

    @Benchmark
    public int pairwiseCompareLegacy() {
        int acc = 0;
        for (int i = 1; i < keys.length; i++) {
            acc += LEGACY.compare(keys[i - 1], keys[i]);
        }
        return acc;
    }

    @Benchmark
    public String[] sort() {
        String[] copy = keys.clone();
        Arrays.sort(copy, CompareWithSlash.INSTANCE);
        return copy;
    }

    @Benchmark
    public String[] sortLegacy() {
        String[] copy = keys.clone();
        Arrays.sort(copy, LEGACY);
        return copy;
    }
}
