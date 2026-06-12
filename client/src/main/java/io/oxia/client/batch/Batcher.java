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

import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.oxia.client.ClientConfig;
import io.oxia.client.util.BatchedArrayBlockingQueue;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;

/**
 * One batching thread, serving the operations of all the shards assigned to it. Producers enqueue
 * into the thread's multi-producer single-consumer queue; the thread drains it and groups the
 * operations into per-shard batches. A batch is sent when full, and any partial batch is flushed as
 * soon as the queue is found empty, since there is nothing left to coalesce with.
 */
public class Batcher implements AutoCloseable {

    private static final int DEFAULT_QUEUE_CAPACITY = 10_000;

    @NonNull private final ClientConfig config;
    @NonNull private final BatchFactory batchFactory;
    @NonNull private final BatchedArrayBlockingQueue<Operation<?>> operations;

    // Open batches, grouped by shard. Only accessed by the batcher thread.
    private final Map<Long, Batch> batchesByShardId = new HashMap<>();

    private final Thread thread;
    private volatile boolean closed;

    Batcher(@NonNull ClientConfig config, String name, @NonNull BatchFactory batchFactory) {
        this(config, name, batchFactory, new BatchedArrayBlockingQueue<>(DEFAULT_QUEUE_CAPACITY));
    }

    Batcher(
            @NonNull ClientConfig config,
            String name,
            @NonNull BatchFactory batchFactory,
            @NonNull BatchedArrayBlockingQueue<Operation<?>> operations) {
        this.config = config;
        this.batchFactory = batchFactory;
        this.operations = operations;

        this.thread = new DefaultThreadFactory(name).newThread(this::batcherLoop);
        this.thread.start();
    }

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
    }

    private void batcherLoop() {
        Operation<?>[] localOperations = new Operation<?>[DEFAULT_QUEUE_CAPACITY];
        int localOperationsIndex = 0;
        int localOperationsCount = 0;

        while (true) {
            try {
                if (localOperationsIndex >= localOperationsCount) {
                    if (batchesByShardId.isEmpty()) {
                        // No pending batches — block until at least one operation arrives.
                        localOperationsCount = operations.takeAll(localOperations);
                    } else {
                        // There are open batches. Non-blocking drain to pick up any operations
                        // that arrived while we were processing.
                        localOperationsCount = operations.pollAll(localOperations, 0, NANOSECONDS);
                        if (localOperationsCount == 0) {
                            // Queue is empty — no concurrent producers are filling it right
                            // now. Flush the open batches immediately rather than lingering,
                            // since there is nothing to batch with.
                            sendAll();

                            // Block until the next operation arrives.
                            localOperationsCount = operations.takeAll(localOperations);
                        }
                    }
                    localOperationsIndex = 0;
                }
            } catch (InterruptedException e) {
                // Exiting thread
                failPendingOperations();
                return;
            }

            Operation<?> operation = localOperations[localOperationsIndex];
            localOperations[localOperationsIndex++] = null;
            try {
                Batch batch = batchesByShardId.get(operation.shardId());
                if (batch == null) {
                    batch = batchFactory.getBatch(operation.shardId());
                    batchesByShardId.put(operation.shardId(), batch);
                }
                if (!batch.canAdd(operation) && batch.size() > 0) {
                    batch.send();
                    batch = batchFactory.getBatch(operation.shardId());
                    batchesByShardId.put(operation.shardId(), batch);
                }
                batch.add(operation);
                if (batch.size() >= config.maxRequestsPerBatch()) {
                    batch.send();
                    batchesByShardId.remove(operation.shardId());
                }
            } catch (Exception e) {
                operation.fail(e);
                // Don't leave behind an open batch that the failed operation just created:
                // it would be flushed empty
                Batch open = batchesByShardId.get(operation.shardId());
                if (open != null && open.size() == 0) {
                    batchesByShardId.remove(operation.shardId());
                }
            }
        }
    }

    private void sendAll() {
        batchesByShardId.values().forEach(Batch::send);
        batchesByShardId.clear();
    }

    private void failPendingOperations() {
        var closedException = new IllegalStateException("Batcher has been closed");
        batchesByShardId.values().forEach(batch -> batch.fail(closedException));
        batchesByShardId.clear();
        Operation<?> operation;
        while ((operation = operations.poll()) != null) {
            operation.fail(closedException);
        }
    }

    @Override
    public void close() {
        closed = true;
        thread.interrupt();
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
