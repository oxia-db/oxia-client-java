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
package io.oxia.client.batch;

import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.session.SessionManager;
import lombok.NonNull;

/**
 * Routes operations to a fixed set of {@link Batcher} threads. A shard is always served by the same
 * batcher, so that the per-shard ordering of operations is preserved.
 */
public class BatchManager implements AutoCloseable {

    private final Batcher[] batchers;
    private volatile boolean closed;

    BatchManager(@NonNull ClientConfig config, String name, @NonNull BatchFactory batchFactory) {
        this.batchers = new Batcher[config.batchingThreads()];
        for (int i = 0; i < batchers.length; i++) {
            batchers[i] = new Batcher(config, name + "-" + i, batchFactory);
        }
    }

    public void add(@NonNull Operation<?> operation) {
        if (closed) {
            throw new IllegalStateException("Batch manager is closed");
        }
        batchers[(int) Math.floorMod(operation.shardId(), batchers.length)].add(operation);
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        for (Batcher batcher : batchers) {
            batcher.close();
        }
    }

    public static @NonNull BatchManager newReadBatchManager(
            @NonNull ClientConfig config,
            @NonNull RpcProvider rpcProvider,
            @NonNull InstrumentProvider instrumentProvider) {
        return new BatchManager(
                config, "oxia-read-batcher", new ReadBatchFactory(rpcProvider, config, instrumentProvider));
    }

    public static @NonNull BatchManager newWriteBatchManager(
            @NonNull ClientConfig config,
            @NonNull RpcProvider rpcProvider,
            @NonNull SessionManager sessionManager,
            @NonNull InstrumentProvider instrumentProvider) {
        return new BatchManager(
                config,
                "oxia-write-batcher",
                new WriteBatchFactory(rpcProvider, sessionManager, config, instrumentProvider));
    }
}
