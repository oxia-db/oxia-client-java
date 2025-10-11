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

import lombok.Data;
import org.HdrHistogram.Histogram;

@Data
public final class HistogramSnapshot {
    private final double p50;
    private final double p95;
    private final double p99;
    private final double p999;
    private final double max;

    public HistogramSnapshot(double p50, double p95, double p99, double p999, double max) {
        this.p50 = p50;
        this.p95 = p95;
        this.p99 = p99;
        this.p999 = p999;
        this.max = max;
    }

    public static HistogramSnapshot fromHistogram(Histogram histogram) {
        return new HistogramSnapshot(
                histogram.getValueAtPercentile(50) / 1000.0,
                histogram.getValueAtPercentile(95) / 1000.0,
                histogram.getValueAtPercentile(99) / 1000.0,
                histogram.getValueAtPercentile(999) / 1000.0,
                histogram.getMaxValue() / 1000.0);
    }
}
