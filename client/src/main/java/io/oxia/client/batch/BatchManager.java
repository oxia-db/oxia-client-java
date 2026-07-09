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
 * Per-client entry point into the batching layer. It binds a client's {@link BatchFactory} (its
 * {@code RpcProvider}, session and config) to a {@link BatcherPool} and routes the client's
 * operations onto it.
 *
 * <p>The pool may be private to this client (standalone client — the manager owns and closes it) or
 * shared across many clients (a {@link io.oxia.client.api.SharedResources} pool — closing this
 * manager only flushes and fails this client's pending operations, leaving the pool running).
 */
public class BatchManager implements AutoCloseable {

    private final @NonNull BatchFactory factory;
    private final @NonNull BatcherPool pool;
    private final boolean ownsPool;
    private volatile boolean closed;

    BatchManager(@NonNull BatchFactory factory, @NonNull BatcherPool pool, boolean ownsPool) {
        this.factory = factory;
        this.pool = pool;
        this.ownsPool = ownsPool;
    }

    public void add(@NonNull Operation<?> operation) {
        if (closed) {
            throw new IllegalStateException("Batch manager is closed");
        }
        pool.route(factory, operation);
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        if (ownsPool) {
            pool.close();
        } else {
            // Shared pool: only tear down this client's pending operations, not the pool.
            pool.closeFactory(factory).join();
        }
    }

    public static @NonNull BatchManager newReadBatchManager(
            @NonNull ClientConfig config,
            @NonNull RpcProvider rpcProvider,
            @NonNull InstrumentProvider instrumentProvider,
            @NonNull BatcherPool pool,
            boolean ownsPool) {
        return new BatchManager(
                new ReadBatchFactory(rpcProvider, config, instrumentProvider), pool, ownsPool);
    }

    public static @NonNull BatchManager newWriteBatchManager(
            @NonNull ClientConfig config,
            @NonNull RpcProvider rpcProvider,
            @NonNull SessionManager sessionManager,
            @NonNull InstrumentProvider instrumentProvider,
            @NonNull BatcherPool pool,
            boolean ownsPool) {
        return new BatchManager(
                new WriteBatchFactory(rpcProvider, sessionManager, config, instrumentProvider),
                pool,
                ownsPool);
    }
}
