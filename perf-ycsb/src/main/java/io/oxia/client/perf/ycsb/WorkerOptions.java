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

import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.oxia.client.OxiaClientBuilderImpl;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

import java.util.concurrent.TimeUnit;

@Slf4j
@CommandLine.Command(name = "ycsb")
public final class WorkerOptions implements Runnable {

    @CommandLine.Option(
            names = {"--name"},
            required = true,
            description = "worker name for this test"
    )
    String workerName = "";

    @CommandLine.Option(
            names = {"--env-name"},
            description = "the worker environment name"
    )
    String envName = "";

    @CommandLine.Option(
            names = {"--env-shards"},
            description = "the worker environment shards number"
    )
    int envShards = 0;

    @CommandLine.Option(
            names = {"--min-exit-time-sec"},
            description = "minimal exit time in the second")
    int minExitTimeSec = 60 * 5;  // at least exist 3 min to let metrics collector

    @CommandLine.Option(
            names = {"--service-addr"},
            description = "Oxia Service Address")
    String serviceAddr = "localhost:6648";

    @CommandLine.Option(
            names = {"--namespace"},
            description = "Oxia namespace")
    String namespace = "default";

    @CommandLine.Option(
            names = {"--rate"},
            description = "Request rate, ops/s")
    double requestsRate = 1000.0;

    @CommandLine.Option(
            names = {"--batch-linger-ms"},
            description = "Batch linger time")
    long batchLingerMs = OxiaClientBuilderImpl.DefaultBatchLinger.toMillis();

    @CommandLine.Option(
            names = {"--max-requests-per-batch"},
            description = "Maximum requests per batch")
    int maxRequestsPerBatch = OxiaClientBuilderImpl.DefaultMaxRequestsPerBatch;

    @CommandLine.Option(
            names = {"--request-timeout-ms"},
            description = "Requests timeout")
    long requestTimeoutMs = OxiaClientBuilderImpl.DefaultRequestTimeout.toMillis();

    @CommandLine.Option(
            names = {"--max-outstanding-requests"},
            description = "Max number of outstanding requests to server")
    int maxOutstandingRequests = 100_000;

    /* Operations */
    @CommandLine.Option(
            names = {"--write-percent"},
            description = "Percentage of write requests, compared to total requests")
    double writePercentage = 20.0;

    @CommandLine.Option(
            names = {"--scan-percent"},
            description = "Percentage of scan requests, compared to total requests")
    double scanPercentage = 0.0;

    @CommandLine.Option(
            names = {"--read-percent"},
            description = "Percentage of read requests, compared to total requests")
    double readPercentage = 80.0;

    @CommandLine.Option(
            names = {"--operation-num"},
            description = "Num of total operations. 0 means no limit")
    long operationNum = 1000;

    @CommandLine.Option(
            names = {"--write-with-sequence"},
            description = "Whether enable write with sequence."
    )
    boolean writeWithSequence = false;

    /* Generation */

    @CommandLine.Option(
            names = {"--key-distribution"},
            description = "The key distribution. support uniform,zipfian,sequential")
    String keyDistribution = "sequential";

    @CommandLine.Option(
            names = {"--key-prefix"},
            description = "The key prefix.")
    String keyPrefix = "key-";

    /* Uniform distribution */
    @CommandLine.Option(
            names = {"--key-uniform-lower-bound"},
            description =
                    "The lower bound of uniform key distribution.(inclusive)")
    int lowerBound = 0;

    @CommandLine.Option(
            names = {"--key-uniform-upper-bound"},
            description =
                    "The upper bound of uniform key distribution.(inclusive)")
    int upperBound = 100_000;


    @CommandLine.Option(
            names = {"--key-zipfian-elements"},
            description = "The number of elements for zipfain distribution"
    )
    int elements = 100_000;

    @CommandLine.Option(
            names = {"--key-zipfian-exponent"},
            description = """
                    The exponent for zipfain distribution.
                    When s=1, the Zipf distribution degenerates into a classic power-law distribution.
                    When s>1, the distribution will be more sparse, meaning fewer elements will occupy a larger proportion.
                    When s<1, the distribution will be more dense, meaning more elements will occupy a larger proportion.
                    """
    )
    double exponent = 1.0;

    /* Value Generation */
    @CommandLine.Option(
            names = {"--value-size"},
            description = "Size of the values to write")
    int valueSize = 64;

    /* Output */

    @CommandLine.Option(
            names = {"--output-interval"},
            description = "The interval(second) of output")
    int intervalOutputSec = 10;

    @CommandLine.Option(
            names = {"--output-global-type"},
            description = "The type of global output. supported: log,pulsar"
    )
    String globalOutputType = "log";

    /* output log */
    @CommandLine.Option(
            names = {"--output-global-log-pretty"},
            description = "Whether pretty the data for the log output type."
    )
    boolean globalOutputLogPretty = true;

    /* output pulsar */
    @CommandLine.Option(
            names = {"--output-global-pulsar-service-url"},
            description = "The pulsar service URL."
    )
    String globalOutputPulsarServiceURL;
    @CommandLine.Option(
            names = {"--output-global-pulsar-target-topic"},
            description = "The target pulsar service topic."
    )
    String globalOutputPulsarTargetTopic;
    @CommandLine.Option(
            names = {"--output-global-pulsar-authentication-plugin"},
            description = "The authentication plugin name."
    )
    String globalOutputPulsarAuthenticationPlugin;
    @CommandLine.Option(
            names = {"--output-global-pulsar-authentication-params"},
            description = "The authentication plugin params."
    )
    String globalOutputPulsarAuthenticationParams;

    @Override
    public void run() {
        final var sdk = AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk();
        try (Worker worker = new Worker(this, sdk)) {
            final long startRunningTime = System.currentTimeMillis();
            worker.run();
            long elapsedMs = System.currentTimeMillis() - startRunningTime;
            if (TimeUnit.MILLISECONDS.toSeconds(elapsedMs) < minExitTimeSec) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(elapsedMs));
            }
        } catch (Throwable ex) {
            log.error("unexpected error. ", ex);
        }
    }
}
