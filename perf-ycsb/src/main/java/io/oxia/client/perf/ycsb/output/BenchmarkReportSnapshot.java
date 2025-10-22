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

import com.fasterxml.jackson.annotation.JsonInclude;
import io.oxia.client.perf.ycsb.WorkerOptions;
import lombok.Data;

@Data
public final class BenchmarkReportSnapshot {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final WorkerOptions definition;

    private final long timestamp;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final long totalWrite;

    private final double writeOps;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final long totalFailedWrite;

    private final double writeFps;
    private final HistogramSnapshot writeLatencyMs;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final long totalRead;

    private final double readOps;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final long totalFailedRead;

    private final double readFps;
    private final HistogramSnapshot readLatencyMs;

    public BenchmarkReportSnapshot(
            /* definitions section */
            @JsonInclude(JsonInclude.Include.NON_NULL) WorkerOptions definition,
            /* metadata section */
            long timestamp,
            /* ops write section */
            @JsonInclude(JsonInclude.Include.NON_DEFAULT) long totalWrite,
            double writeOps,
            @JsonInclude(JsonInclude.Include.NON_DEFAULT) long totalFailedWrite,
            double writeFps,
            HistogramSnapshot writeLatencyMs,
            /* ops read section */
            @JsonInclude(JsonInclude.Include.NON_DEFAULT) long totalRead,
            double readOps,
            @JsonInclude(JsonInclude.Include.NON_DEFAULT) long totalFailedRead,
            double readFps,
            HistogramSnapshot readLatencyMs) {
        this.definition = definition;
        this.timestamp = timestamp;
        this.totalWrite = totalWrite;
        this.writeOps = writeOps;
        this.totalFailedWrite = totalFailedWrite;
        this.writeFps = writeFps;
        this.writeLatencyMs = writeLatencyMs;
        this.totalRead = totalRead;
        this.readOps = readOps;
        this.totalFailedRead = totalFailedRead;
        this.readFps = readFps;
        this.readLatencyMs = readLatencyMs;
    }
}
