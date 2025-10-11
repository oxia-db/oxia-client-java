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
package io.oxia.client.perf.ycsb;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.oxia.client.api.exceptions.OxiaException;
import io.oxia.client.metrics.Unit;
import io.oxia.client.perf.ycsb.generator.Generator;
import io.oxia.client.perf.ycsb.generator.GeneratorType;
import io.oxia.client.perf.ycsb.generator.Generators;
import io.oxia.client.perf.ycsb.generator.KeyGeneratorOptions;
import io.oxia.client.perf.ycsb.generator.OperationGeneratorOptions;
import io.oxia.client.perf.ycsb.generator.OperationType;
import io.oxia.client.perf.ycsb.operations.Operations;
import io.oxia.client.perf.ycsb.operations.Status;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class Worker implements Runnable, Closeable, Operations {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final WorkerOptions options;
    private final SyncOxiaClient client;
    private final Generator<String> keyGenerator;
    private final Generator<byte[]> valueGenerator;
    private final Generator<OperationType> operationGenerator;
    private final Output intervalOutput;
    private final Output globalOutput;

    private volatile CompletableFuture<Void> closeFuture;

    private final Semaphore outstandingSemaphore;

    /* Otl section */
    private final LongCounter operationCounter;
    private final ObservableLongGauge outstandingRequestGauge;
    private final Attributes operationWriteSuccessAttributes;
    private final Attributes operationWriteFailedAttributes;
    private final Attributes operationReadSuccessAttributes;
    private final Attributes operationReadFailedAttributes;

    private final DoubleHistogram operationLatency;
    private static final List<Double> LATENCY_BUCKET =
            Lists.newArrayList(
                    .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
                    90.0, 120.0, 240.0);
    private static final double MICROS = TimeUnit.SECONDS.toMicros(1);

    public Worker(WorkerOptions options, OpenTelemetry openTelemetry) {
        try {
            this.client =
                    OxiaClientBuilder.create(options.serviceAddr)
                            .batchLinger(Duration.ofMillis(options.batchLingerMs))
                            .maxRequestsPerBatch(options.maxRequestsPerBatch)
                            .requestTimeout(Duration.ofMillis(options.requestTimeoutMs))
                            .namespace(options.namespace)
                            .openTelemetry(openTelemetry)
                            .syncClient();
        } catch (OxiaException e) {
            throw new WorkerException(e);
        }
        final GeneratorType generatorType = GeneratorType.fromString(options.keyDistribution);

        this.outstandingSemaphore = new Semaphore(options.maxOutstandingRequests);
        this.keyGenerator =
                Generators.createKeyGenerator(
                        new KeyGeneratorOptions(
                                generatorType,
                                options.keyPrefix,
                                options.lowerBound,
                                options.upperBound,
                                options.elements,
                                options.exponent));
        this.valueGenerator = Generators.createFixedLengthValueGenerator(options.valueSize);
        this.operationGenerator =
                Generators.createOperationGenerator(
                        new OperationGeneratorOptions(
                                options.writePercentage, options.readPercentage, options.scanPercentage));
        this.intervalOutput = Outputs.createLogOutput(false);
        final OutputTypes outputTypes = OutputTypes.fromString(options.globalOutputType);
        this.globalOutput =
                Outputs.createOutput(
                        outputTypes,
                        new OutputOptions(
                                options.globalOutputLogPretty,
                                new PulsarOutputOptions(
                                        options.globalOutputPulsarServiceURL,
                                        options.globalOutputPulsarTargetTopic,
                                        options.globalOutputPulsarAuthenticationPlugin,
                                        options.globalOutputPulsarAuthenticationParams)));
        this.options = options;
        final var meter = openTelemetry.getMeter("io.oxia.perf-ycsb");
        this.operationCounter =
                meter
                        .counterBuilder("oxia.perf.ycsb.op")
                        .setDescription("oxia perf operation counter")
                        .setUnit(Unit.Requests.toString())
                        .build();
        this.outstandingRequestGauge =
                meter
                        .gaugeBuilder("oxia.perf.ycsb.op.outstanding")
                        .setDescription("oxia outstanding request")
                        .setUnit(Unit.Requests.toString())
                        .ofLongs()
                        .buildWithCallback(
                                (ob) -> {
                                    ob.record(
                                            options.maxOutstandingRequests - outstandingSemaphore.availablePermits(),
                                            Attributes.builder()
                                                    .put("operation.num", options.operationNum)
                                                    .put("value.size", options.valueSize)
                                                    .put("worker", options.workerName)
                                                    .put("env", options.envName)
                                                    .put("env.shards", options.envShards)
                                                    .build());
                                });
        this.operationLatency =
                meter
                        .histogramBuilder("oxia.perf.ycsb.op.second")
                        .setUnit(Unit.Seconds.toString())
                        .setDescription("oxia perf operation latency")
                        .setExplicitBucketBoundariesAdvice(LATENCY_BUCKET)
                        .build();
        this.operationWriteSuccessAttributes =
                Attributes.builder()
                        .put("type", "write")
                        .put("operation.num", options.operationNum)
                        .put("value.size", options.valueSize)
                        .put("worker", options.workerName)
                        .put("env", options.envName)
                        .put("env.shards", options.envShards)
                        .put("response", "success")
                        .build();
        this.operationWriteFailedAttributes =
                Attributes.builder()
                        .put("type", "write")
                        .put("operation.num", options.operationNum)
                        .put("value.size", options.valueSize)
                        .put("worker", options.workerName)
                        .put("env", options.envName)
                        .put("env.shards", options.envShards)
                        .put("response", "failed")
                        .build();
        this.operationReadSuccessAttributes =
                Attributes.builder()
                        .put("type", "read")
                        .put("operation.num", options.operationNum)
                        .put("value.size", options.valueSize)
                        .put("worker", options.workerName)
                        .put("env", options.envName)
                        .put("env.shards", options.envShards)
                        .put("response", "success")
                        .build();
        this.operationReadFailedAttributes =
                Attributes.builder()
                        .put("type", "read")
                        .put("operation.num", options.operationNum)
                        .put("value.size", options.valueSize)
                        .put("worker", options.workerName)
                        .put("env", options.envName)
                        .put("env.shards", options.envShards)
                        .put("response", "failed")
                        .build();
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public void run() {
        try {
            final String optionsStr = MAPPER.writeValueAsString(options);
            log.info("starting worker. the options={}", optionsStr);
        } catch (JsonProcessingException ex) {
            throw new WorkerException(ex);
        }

        final RateLimiter operationRatelimiter = RateLimiter.create(options.requestsRate);
        final int maxOutstandingRequests = options.maxOutstandingRequests;

        final BenchmarkReport globalReport = BenchmarkReport.createDefault();
        final BenchmarkReport intervalReport = BenchmarkReport.createDefault();

        final Function<Long, BenchmarkReportSnapshot> globalSnapshotFunc =
                globalReport.snapshotFunc(options, false);
        final Function<Long, BenchmarkReportSnapshot> internalSnapshotFunc =
                intervalReport.snapshotFunc(options, true);

        log.info("performance test is starting");
        final AtomicLong operationNum = new AtomicLong(options.operationNum);
        final Thread intervalOutputTask =
                Thread.ofVirtual()
                        .start(
                                () -> {
                                    log.info("starting interval output task.");
                                    long lastSnapshotTime = System.nanoTime();
                                    //noinspection InfiniteLoopStatement
                                    while (true) {
                                        try {
                                            //noinspection BusyWait
                                            Thread.sleep(options.intervalOutputSec * 1000L);
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            log.info("exit interval output thread while sleeping by interrupt");
                                            return;
                                        }

                                        intervalOutput.report(internalSnapshotFunc.apply(lastSnapshotTime));
                                        if (options.operationNum > 0) {
                                            log.info("remain operation num {}", operationNum.get());
                                        }
                                        lastSnapshotTime = System.nanoTime();
                                    }
                                });
        final long taskStartTime = System.nanoTime();
        while ((options.operationNum > 0 ? operationNum.getAndDecrement() > 0 : closeFuture == null)) {
            // jump out by closing worker
            operationRatelimiter.acquire();
            try {
                outstandingSemaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new WorkerException(e);
            }

            final OperationType operationType = operationGenerator.nextValue();
            final String key = keyGenerator.nextValue();
            final byte[] value = valueGenerator.nextValue();
            Thread.ofVirtual()
                    .start(
                            () -> {
                                try {
                                    switch (operationType) {
                                        case WRITE -> {
                                            globalReport.writeTotal().increment();
                                            intervalReport.writeTotal().increment();
                                            final long start = System.nanoTime();
                                            final Status sts;
                                            if (options.writeWithSequence) {
                                                sts = writeWithSequence(key, value);
                                            } else {
                                                sts = write(key, value);
                                            }
                                            if (!sts.isSuccess()) {
                                                operationCounter.add(1, operationWriteFailedAttributes);
                                                log.warn("write failed. the error info {}", sts.getErrorInfo());
                                                final long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
                                                operationLatency.record(
                                                        latencyMicros / MICROS, operationWriteFailedAttributes);
                                                globalReport.writeFailed().increment();
                                                intervalReport.writeFailed().increment();
                                            } else {
                                                operationCounter.add(1, operationWriteSuccessAttributes);
                                                final long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
                                                operationLatency.record(
                                                        latencyMicros / MICROS, operationWriteSuccessAttributes);
                                                globalReport.writeLatency().recordValue(latencyMicros);
                                                intervalReport.writeLatency().recordValue(latencyMicros);
                                            }
                                        }
                                        case READ -> {
                                            globalReport.readTotal().increment();
                                            intervalReport.readTotal().increment();
                                            final long start = System.nanoTime();
                                            final Status sts = read(key);
                                            if (!sts.isSuccess()) {
                                                operationCounter.add(1, operationReadFailedAttributes);
                                                final long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
                                                operationLatency.record(
                                                        latencyMicros / MICROS, operationReadFailedAttributes);
                                                log.warn("read failed. the error info {}", sts.getErrorInfo());
                                                globalReport.readFailed().increment();
                                                intervalReport.readFailed().increment();
                                            } else {
                                                operationCounter.add(1, operationReadSuccessAttributes);
                                                final long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
                                                operationLatency.record(
                                                        latencyMicros / MICROS, operationReadSuccessAttributes);
                                                globalReport.readLatency().recordValue(latencyMicros);
                                                intervalReport.readLatency().recordValue(latencyMicros);
                                            }
                                        }
                                        default -> throw new UnsupportedOperationException("unsupported yet");
                                    }
                                } finally {
                                    outstandingSemaphore.release();
                                }
                            });
        }

        try {
            outstandingSemaphore.acquire(maxOutstandingRequests); // acquire all of permits
        } catch (InterruptedException e) {
            throw new WorkerException(e);
        }
        final BenchmarkReportSnapshot globalSnapshot = globalSnapshotFunc.apply(taskStartTime);

        // interrupt the interval output task
        intervalOutputTask.interrupt();

        globalOutput.report(globalSnapshot);

        if (closeFuture == null) {
            synchronized (this) {
                if (closeFuture == null) {
                    // avoid close after running
                    closeFuture = CompletableFuture.completedFuture(null);
                }
            }
        } else {
            if (!closeFuture.complete(null)) {
                log.warn("bug! unexpected behaviour: completed future and empty close future");
            }
        }
        log.info("performance test is done");
    }

    @Override
    public void close() {
        // mark the worker is closing
        if (closeFuture == null) {
            synchronized (this) {
                if (closeFuture == null) {
                    closeFuture = new CompletableFuture<>();
                }
            }
        }
        // wait for task run complete
        closeFuture.join();
        try {
            client.close();
            if (globalOutput != null) {
                globalOutput.close();
            }
            if (intervalOutput != null) {
                intervalOutput.close();
            }
        } catch (Exception ex) {
            throw new WorkerException(ex);
        }

        outstandingRequestGauge.close(); // close observer
    }

    @Override
    public Status write(String key, byte[] value) {
        try {
            final PutResult result = client.put(key, value);
            if (result != null) {
                return Status.success();
            }
            return Status.failed("empty result");
        } catch (Throwable ex) {
            return Status.failed(ex.getMessage());
        }
    }

    @Override
    public Status writeWithSequence(String key, byte[] value) {
        try {
            final PutResult result =
                    client.put(
                            key,
                            value,
                            Set.of(
                                    PutOption.PartitionKey(key),
                                    PutOption.SequenceKeysDeltas(List.of(1L, (long) value.length))));
            if (result != null) {
                return Status.success();
            }
            return Status.failed("empty result");
        } catch (Throwable ex) {
            return Status.failed(ex.getMessage());
        }
    }

    @Override
    public Status read(String key) {
        try {
            final GetResult result = client.get(key);
            if (result != null) {
                return Status.success(result.getValue());
            }
            return Status.failed("empty result");
        } catch (Throwable ex) {
            return Status.failed(ex.getMessage());
        }
    }
}
