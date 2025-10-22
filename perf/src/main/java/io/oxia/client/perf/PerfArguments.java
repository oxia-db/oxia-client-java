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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.oxia.client.OxiaClientBuilderImpl;

@Parameters(commandDescription = "Test Oxia Java client performance.")
public class PerfArguments {

    @Parameter(
            names = {"-h", "--help"},
            description = "Help message",
            help = true)
    boolean help;

    @Parameter(
            names = {"-a", "--service-addr"},
            description = "Oxia Service Address")
    String serviceAddr = "localhost:6648";

    @Parameter(
            names = {"-n", "--namespace"},
            description = "Oxia namespace")
    String namespace = "default";

    @Parameter(
            names = {"-r", "--rate"},
            description = "Request rate, ops/s")
    double requestsRate = 100.0;

    @Parameter(
            names = {"-p", "--read-write-percent"},
            description = "Percentage of read requests, compared to total requests")
    double readPercentage = 80.0;

    @Parameter(
            names = {"-k", "--keys-cardinality"},
            description = "Number of unique keys")
    int keysCardinality = 1_000;

    @Parameter(
            names = {"-s", "--value-size"},
            description = "Size of the values to write")
    int valueSize = 128;

    @Parameter(
            names = {"--batch-linger-ms"},
            description = "Batch linger time")
    long batchLingerMs = OxiaClientBuilderImpl.DefaultBatchLinger.toMillis();

    @Parameter(
            names = {"--max-requests-per-batch"},
            description = "Maximum requests per batch")
    int maxRequestsPerBatch = OxiaClientBuilderImpl.DefaultMaxRequestsPerBatch;

    @Parameter(
            names = {"--request-timeout-ms"},
            description = "Requests timeout")
    long requestTimeoutMs = OxiaClientBuilderImpl.DefaultRequestTimeout.toMillis();

    @Parameter(
            names = {"-o", "--max-outstanding-requests"},
            description = "Max number of outstanding requests to server")
    int maxOutstandingRequests = 100_000;
}
