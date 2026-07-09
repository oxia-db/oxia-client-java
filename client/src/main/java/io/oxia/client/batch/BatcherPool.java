/*
 * Copyright © 2026 The Oxia Authors
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

import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

/**
 * A fixed set of {@link Batcher} threads that operations are routed across by shard. A shard is
 * always served by the same batcher, so per-shard ordering is preserved.
 *
 * <p>The pool is factory-agnostic: each operation is routed together with the {@link BatchFactory}
 * of the client that submitted it, so one pool can be shared by many client instances (via {@link
 * io.oxia.client.api.SharedResources}) instead of every client owning its own batcher threads.
 */
public final class BatcherPool implements AutoCloseable {

    private final Batcher[] batchers;

    public BatcherPool(@NonNull String name, int batchingThreads) {
        this.batchers = new Batcher[batchingThreads];
        for (int i = 0; i < batchingThreads; i++) {
            batchers[i] = new Batcher(name + "-" + i);
        }
    }

    void route(@NonNull BatchFactory factory, @NonNull Operation<?> operation) {
        batchers[(int) Math.floorMod(operation.shardId(), batchers.length)].add(factory, operation);
    }

    /**
     * Flush and fail the open batches of {@code factory} across all batchers (a client is closing).
     * The returned future completes once every batcher has processed the request.
     */
    CompletableFuture<Void> closeFactory(@NonNull BatchFactory factory) {
        var futures = new CompletableFuture[batchers.length];
        for (int i = 0; i < batchers.length; i++) {
            futures[i] = batchers[i].closeFactory(factory);
        }
        return CompletableFuture.allOf(futures);
    }

    @Override
    public void close() {
        for (Batcher batcher : batchers) {
            batcher.close();
        }
    }
}
