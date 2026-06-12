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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.session.SessionManager;
import io.oxia.client.util.BatchedArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import lombok.NonNull;
import lombok.SneakyThrows;

/**
 * Groups the operations of one shard into batches. Producers enqueue into a multi-producer
 * single-consumer queue, and the assembly of batches runs on the shared batching executor: a shard
 * is scheduled when it has operations to process, so the executor threads are shared by all the
 * shards. A partial batch is flushed as soon as the queue is found empty, since there is nothing
 * left to coalesce with.
 */
public class Batcher implements AutoCloseable {

    private static final int DEFAULT_QUEUE_CAPACITY = 10_000;

    @NonNull private final ClientConfig config;
    private final long shardId;
    @NonNull private final BatchFactory batchFactory;
    @NonNull private final Executor executor;
    @NonNull private final BatchedArrayBlockingQueue<Operation<?>> operations;

    // Single-consumer: guarantees at most one processQueue() task at a time for this shard
    private final AtomicBoolean scheduled = new AtomicBoolean();

    private final Operation<?>[] localOperations;

    // Only accessed by the processQueue() task, which is serialized by the scheduled flag
    private Batch currentBatch;

    private volatile boolean closed;

    Batcher(
            @NonNull ClientConfig config,
            long shardId,
            @NonNull BatchFactory batchFactory,
            @NonNull Executor executor) {
        this(config, shardId, batchFactory, executor, DEFAULT_QUEUE_CAPACITY);
    }

    Batcher(
            @NonNull ClientConfig config,
            long shardId,
            @NonNull BatchFactory batchFactory,
            @NonNull Executor executor,
            int queueCapacity) {
        this.config = config;
        this.shardId = shardId;
        this.batchFactory = batchFactory;
        this.executor = executor;
        this.operations = new BatchedArrayBlockingQueue<>(queueCapacity);
        this.localOperations = new Operation<?>[Math.min(queueCapacity, config.maxRequestsPerBatch())];
    }

    @SneakyThrows
    public <R> void add(@NonNull Operation<R> operation) {
        if (closed) {
            operation.fail(new IllegalStateException("Batcher has been closed"));
            return;
        }
        try {
            operations.put(operation);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        schedule();
    }

    private void schedule() {
        if (scheduled.compareAndSet(false, true)) {
            executor.execute(this::processQueue);
        }
    }

    @SneakyThrows
    private void processQueue() {
        try {
            if (closed) {
                failPendingOperations();
                return;
            }

            // Process one chunk per task, so that the executor threads are shared fairly among
            // the shards: when more operations are queued, the shard is re-scheduled below.
            int count = operations.pollAll(localOperations, 0, NANOSECONDS);
            for (int i = 0; i < count; i++) {
                Operation<?> operation = localOperations[i];
                localOperations[i] = null;
                try {
                    if (currentBatch == null) {
                        currentBatch = batchFactory.getBatch(shardId);
                    }
                    if (!currentBatch.canAdd(operation) && currentBatch.size() > 0) {
                        currentBatch.send();
                        currentBatch = batchFactory.getBatch(shardId);
                    }
                    currentBatch.add(operation);
                    if (currentBatch.size() >= config.maxRequestsPerBatch()) {
                        currentBatch.send();
                        currentBatch = null;
                    }
                } catch (Exception e) {
                    operation.fail(e);
                }
            }

            if (currentBatch != null && currentBatch.size() > 0 && operations.isEmpty()) {
                // Nothing left to coalesce with: flush the partial batch immediately
                currentBatch.send();
                currentBatch = null;
            }
        } finally {
            scheduled.set(false);
            // Operations enqueued while we were clearing the flag would have lost the
            // schedule() race: re-check and re-schedule
            if (!operations.isEmpty() || closed && hasPendingOperations()) {
                schedule();
            }
        }
    }

    private boolean hasPendingOperations() {
        return currentBatch != null || !operations.isEmpty();
    }

    private void failPendingOperations() {
        var closedException = new IllegalStateException("Batcher has been closed");
        if (currentBatch != null) {
            currentBatch.fail(closedException);
            currentBatch = null;
        }
        Operation<?> operation;
        while ((operation = operations.poll()) != null) {
            operation.fail(closedException);
        }
    }

    static @NonNull Function<Long, Batcher> newReadBatcherFactory(
            @NonNull ClientConfig config,
            @NonNull RpcProvider rpcProvider,
            @NonNull Executor executor,
            InstrumentProvider instrumentProvider) {
        return s ->
                new Batcher(
                        config, s, new ReadBatchFactory(rpcProvider, config, instrumentProvider), executor);
    }

    static @NonNull Function<Long, Batcher> newWriteBatcherFactory(
            @NonNull ClientConfig config,
            @NonNull RpcProvider rpcProvider,
            @NonNull SessionManager sessionManager,
            @NonNull Executor executor,
            InstrumentProvider instrumentProvider) {
        return s ->
                new Batcher(
                        config,
                        s,
                        new WriteBatchFactory(rpcProvider, sessionManager, config, instrumentProvider),
                        executor);
    }

    @Override
    public void close() {
        closed = true;
        // The processQueue() task fails the pending operations; scheduling it here also covers
        // the case where no task is running
        schedule();
    }
}
