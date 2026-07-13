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
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.metrics.LatencyHistogram;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Getter;
import lombok.NonNull;

class ReadBatchFactory extends BatchFactory {

    @Getter private final LatencyHistogram readRequestLatencyHistogram;

    // In-flight dispatch window per shard, created lazily on first use.
    private final ConcurrentMap<Long, DispatchWindow> windows = new ConcurrentHashMap<>();

    public ReadBatchFactory(
            @NonNull RpcProvider rpcProvider,
            @NonNull ClientConfig config,
            @NonNull InstrumentProvider instrumentProvider) {
        super(rpcProvider, config);

        readRequestLatencyHistogram =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops.req",
                        "The latency of a get batch request to the server",
                        Attributes.builder().put("oxia.batch.type", "read").build());
    }

    @Override
    public Batch getBatch(long shardId) {
        return new ReadBatch(this, rpcProvider, shardId);
    }

    @Override
    DispatchWindow getDispatchWindow(long shardId) {
        return windows.computeIfAbsent(
                shardId, s -> new DispatchWindow(getConfig().maxReadBatchesInFlight()));
    }

    @Override
    void failWindows(Throwable error) {
        windows.values().forEach(window -> window.fail(error));
    }
}
