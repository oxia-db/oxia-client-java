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

import static lombok.AccessLevel.PACKAGE;

import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.OxiaStubProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.session.SessionManager;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BatchManager implements AutoCloseable {

    private final ConcurrentMap<Long, Batcher> batchersByShardId = new ConcurrentHashMap<>();
    private final @NonNull Function<Long, Batcher> batcherFactory;
    private volatile boolean closed;

    public Batcher getBatcher(long shardId) {
        if (closed) {
            throw new IllegalStateException("Batch manager is closed");
        }
        return batchersByShardId.computeIfAbsent(shardId, this::createAndStartBatcher);
    }

    private Batcher createAndStartBatcher(long shardId) {
        return batcherFactory.apply(shardId);
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        batchersByShardId.values().forEach(Batcher::close);
    }

    @RequiredArgsConstructor(access = PACKAGE)
    public static class ShutdownException extends Exception {
        @Getter private final @NonNull List<Exception> exceptions;
    }

    public static @NonNull BatchManager newReadBatchManager(
            @NonNull ClientConfig config,
            @NonNull OxiaStubProvider stubProvider,
            @NonNull InstrumentProvider instrumentProvider) {
        return new BatchManager(
                Batcher.newReadBatcherFactory(config, stubProvider, instrumentProvider));
    }

    public static @NonNull BatchManager newWriteBatchManager(
            @NonNull ClientConfig config,
            @NonNull OxiaStubProvider stubProvider,
            @NonNull SessionManager sessionManager,
            @NonNull InstrumentProvider instrumentProvider) {
        return new BatchManager(
                Batcher.newWriteBatcherFactory(config, stubProvider, sessionManager, instrumentProvider));
    }
}
