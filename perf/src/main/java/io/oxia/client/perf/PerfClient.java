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
package io.oxia.client.perf;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.util.concurrent.RateLimiter;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import io.github.merlimat.slog.Logger;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

public class PerfClient {

  private static final Logger log = Logger.get(PerfClient.class);

  private static final List<String> keys = new ArrayList<>();
  private static final LongAdder writeOps = new LongAdder();
  private static final LongAdder readOps = new LongAdder();
  private static final LongAdder writeFailed = new LongAdder();
  private static final LongAdder readFailed = new LongAdder();
  private static final Recorder writeLatency = new Recorder(TimeUnit.SECONDS.toMicros(120_000), 5);
  private static final Recorder readLatency = new Recorder(TimeUnit.SECONDS.toMicros(120_000), 5);
  private static final PerfArguments arguments = new PerfArguments();
  private static volatile boolean closing = false;

  public static void main(String[] args) throws Exception {
    JCommander jc = new JCommander(arguments);
    jc.setProgramName("oxia-java perf");

    try {
      jc.parse(args);
    } catch (ParameterException e) {
      System.out.println(e.getMessage());
      jc.usage();
      System.exit(1);
    }

    if (arguments.help) {
      jc.usage();
      System.exit(1);
    }

    AutoConfiguredOpenTelemetrySdk sdk = AutoConfiguredOpenTelemetrySdk.builder().build();

    AsyncOxiaClient client =
        OxiaClientBuilder.create(arguments.serviceAddr)
            .batchLinger(Duration.ofMillis(arguments.batchLingerMs))
            .maxRequestsPerBatch(arguments.maxRequestsPerBatch)
            .requestTimeout(Duration.ofMillis(arguments.requestTimeoutMs))
            .namespace(arguments.namespace)
            .openTelemetry(sdk.getOpenTelemetrySdk())
            .asyncClient()
            .get();

    for (int i = 0; i < arguments.keysCardinality; i++) {
      keys.add("key-" + i);
    }

    ExecutorService executor = Executors.newCachedThreadPool();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  closing = true;
                  executor.shutdownNow();
                  Runtime.getRuntime().halt(0);
                }));

    if (arguments.readPercentage != 100) {
      executor.execute(() -> generateWriteTraffic(client));
    }

    if (arguments.readPercentage != 0) {
      executor.execute(() -> generateReadTraffic(client));
    }

    Histogram writeReportHistogram = null;
    Histogram readReportHistogram = null;

    long oldTime = System.nanoTime();

    while (!closing) {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        break;
      }

      long now = System.nanoTime();
      double elapsed = (now - oldTime) / 1e9;

      double writeRate = writeOps.sumThenReset() / elapsed;
      double readRate = readOps.sumThenReset() / elapsed;
      double failedWriteRate = writeFailed.sumThenReset() / elapsed;
      double failedReadRate = readFailed.sumThenReset() / elapsed;

      writeReportHistogram = writeLatency.getIntervalHistogram(writeReportHistogram);
      readReportHistogram = readLatency.getIntervalHistogram(readReportHistogram);

      log.infof(
          """
              Stats - Total ops: %7.0f ops/s - Failed ops: %7.0f ops/s
                 Write ops %7.0f w/s  Latency ms: 50%% %7.1f - 95%% %7.1f - 99%% %7.1f - 99.9%% %7.1f - max %7.1f
                 Read  ops %7.0f r/s  Latency ms: 50%% %7.1f - 95%% %7.1f - 99%% %7.1f - 99.9%% %7.1f - max %7.1f""",
          writeRate + readRate,
          failedWriteRate + failedReadRate,
          writeRate,
          writeReportHistogram.getValueAtPercentile(50) / 1000.0,
          writeReportHistogram.getValueAtPercentile(95) / 1000.0,
          writeReportHistogram.getValueAtPercentile(99) / 1000.0,
          writeReportHistogram.getValueAtPercentile(99.9) / 1000.0,
          writeReportHistogram.getMaxValue() / 1000.0,
          readRate,
          readReportHistogram.getValueAtPercentile(50) / 1000.0,
          readReportHistogram.getValueAtPercentile(95) / 1000.0,
          readReportHistogram.getValueAtPercentile(99) / 1000.0,
          readReportHistogram.getValueAtPercentile(99.9) / 1000.0,
          readReportHistogram.getMaxValue() / 1000.0);

      writeReportHistogram.reset();
      readReportHistogram.reset();

      oldTime = now;
    }
  }

  private static void generateWriteTraffic(AsyncOxiaClient client) {
    double writeRate = arguments.requestsRate * (100.0 - arguments.readPercentage) / 100;
    RateLimiter limiter = RateLimiter.create(writeRate);
    Semaphore semaphore = new Semaphore(arguments.maxOutstandingRequests);

    byte[] value = new byte[arguments.valueSize];
    Random rand = new Random();

    while (!closing) {
      limiter.acquire();
      semaphore.acquireUninterruptibly();

      String key = keys.get(rand.nextInt(keys.size()));

      long start = System.nanoTime();
      client
          .put(key, value)
          .thenRun(
              () -> {
                writeOps.increment();
                long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
                writeLatency.recordValue(latencyMicros);
                semaphore.release();
              })
          .exceptionally(
              ex -> {
                log.warn().exceptionMessage(ex).log("Write operation failed");
                writeFailed.increment();
                semaphore.release();
                return null;
              });
    }
  }

  private static void generateReadTraffic(AsyncOxiaClient client) {
    double readRate = arguments.requestsRate * arguments.readPercentage / 100;
    RateLimiter limiter = RateLimiter.create(readRate);
    Semaphore semaphore = new Semaphore(arguments.maxOutstandingRequests);

    Random rand = new Random();

    while (!closing) {
      limiter.acquire();
      semaphore.acquireUninterruptibly();

      String key = keys.get(rand.nextInt(keys.size()));

      long start = System.nanoTime();
      client
          .get(key)
          .thenRun(
              () -> {
                readOps.increment();
                long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
                readLatency.recordValue(latencyMicros);
                semaphore.release();
              })
          .exceptionally(
              ex -> {
                log.warn().exceptionMessage(ex).log("Read operation failed");
                readFailed.increment();
                semaphore.release();
                return null;
              });
    }
  }

}
