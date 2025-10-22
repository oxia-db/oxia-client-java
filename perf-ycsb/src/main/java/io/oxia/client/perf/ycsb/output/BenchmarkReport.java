/*
 * Copyright © 2022-2025 The Oxia Authors
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
package io.oxia.client.perf.ycsb.output;

import io.oxia.client.perf.ycsb.WorkerOptions;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

@Deprecated
public record BenchmarkReport(
        LongAdder writeTotal,
        LongAdder writeFailed,
        Recorder writeLatency,
        LongAdder readTotal,
        LongAdder readFailed,
        Recorder readLatency) {
    public static BenchmarkReport createDefault() {
        return new BenchmarkReport(
                new LongAdder(),
                new LongAdder(),
                new Recorder(TimeUnit.SECONDS.toMicros(120_000), 5),
                new LongAdder(),
                new LongAdder(),
                new Recorder(TimeUnit.SECONDS.toMicros(120_000), 5));
    }


    public Function<Long, BenchmarkReportSnapshot> snapshotFunc(WorkerOptions options, boolean interval) {
        // recycler objects
        final AtomicReference<Histogram> writeHistogramRef = new AtomicReference<>();
        final AtomicReference<Histogram> readHistogramRef = new AtomicReference<>();

        // lambda
        return (taskStartTime) -> {

            double elapsed = (System.nanoTime() - taskStartTime) / 1e9;
            // write section
            final long totalWrite = writeTotal().sumThenReset();
            final double writeOps = Doubles.format2Scale(totalWrite / elapsed);
            final long totalWriteFailed = writeFailed().sumThenReset();
            final double writeFailedOps = Doubles.format2Scale(totalWriteFailed / elapsed);

            // read section
            final long totalRead = readTotal().sumThenReset();
            final double readOps = Doubles.format2Scale(totalRead / elapsed);
            final long totalReadFailed = readFailed().sumThenReset();
            final double readFailedOps = Doubles.format2Scale(totalReadFailed / elapsed);

            writeHistogramRef.setRelease(writeLatency().getIntervalHistogram(writeHistogramRef.get()));
            final HistogramSnapshot writeLatencySnapshot =
                    HistogramSnapshot.fromHistogram(writeHistogramRef.get());

            readHistogramRef.setRelease(readLatency().getIntervalHistogram(readHistogramRef.get()));
            final HistogramSnapshot readLatencySnapshot =
                    HistogramSnapshot.fromHistogram(readHistogramRef.get());

            writeHistogramRef.get().reset();
            readHistogramRef.get().reset();

            /*
                Interval snapshot don't need total values.
             */
            if (interval) {
                return new BenchmarkReportSnapshot(
                        null,
                        System.currentTimeMillis(),
                        0,
                        writeOps,
                        0,
                        writeFailedOps,
                        writeLatencySnapshot,
                        0,
                        readOps,
                        0,
                        readFailedOps,
                        readLatencySnapshot);
            } else {
                return new BenchmarkReportSnapshot(
                        options,
                        System.currentTimeMillis(),
                        totalWrite,
                        writeOps,
                        totalWriteFailed,
                        writeFailedOps,
                        writeLatencySnapshot,
                        totalRead,
                        readOps,
                        totalReadFailed,
                        readFailedOps,
                        readLatencySnapshot);
            }
        };
    }
}
