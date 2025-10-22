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
package io.oxia.client.batch;

import io.opentelemetry.api.common.Attributes;
import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.OxiaStubProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.metrics.LatencyHistogram;
import io.oxia.client.session.SessionManager;
import lombok.NonNull;

class WriteBatchFactory extends BatchFactory {
    final @NonNull SessionManager sessionManager;

    final LatencyHistogram writeRequestLatencyHistogram;

    public WriteBatchFactory(
            @NonNull OxiaStubProvider stubProvider,
            @NonNull SessionManager sessionManager,
            @NonNull ClientConfig config,
            @NonNull InstrumentProvider instrumentProvider) {
        super(stubProvider, config);
        this.sessionManager = sessionManager;

        writeRequestLatencyHistogram =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops.req",
                        "The latency of a get batch request to the server",
                        Attributes.builder().put("oxia.batch.type", "write").build());
    }

    @Override
    public Batch getBatch(long shardId) {
        return new WriteBatch(this, stubProvider, sessionManager, shardId, getConfig().maxBatchSize());
    }
}
