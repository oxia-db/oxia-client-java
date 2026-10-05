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

import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.collection.LongObjectMap;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.oxia.client.util.BatchedArrayBlockingQueue;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

/**
 * One batching thread, serving every shard — and, when it belongs to a shared pool, every client
 * instance — routed to it. Producers enqueue commands into the thread's multi-producer
 * single-consumer queue; the thread drains it and groups operations into per-(client, shard)
 * batches. A batch is sent when full, and any open batch is flushed as soon as the queue is found
 * empty, since there is nothing left to coalesce with.
 *
 * <p>Each operation carries the {@link BatchManager} of the client that submitted it, and through
 * it that client's {@link BatchFactory} (bound to the client's {@code RpcProvider}, session and
 * config), so a single batcher can serve many clients without being tied to any one of them.
 */
final class Batcher implements AutoCloseable {

    private static final int DEFAULT_QUEUE_CAPACITY = 10_000;

    record CloseFactory(BatchFactory factory, CompletableFuture<Void> done) {}

    // Operations, queued bare since each carries its client's batch manager, and CloseFactory
    // commands
    @NonNull private final BatchedArrayBlockingQueue<Object> commands;

    // Open batches, by client factory and then by shard, so that looking up an operation's batch
    // allocates no key. Only accessed by the batcher thread.
    private final Map<BatchFactory, LongObjectHashMap<Batch>> openBatches = new HashMap<>();

    private final Thread thread;
    private volatile boolean closed;

    Batcher(String name) {
        this.commands = new BatchedArrayBlockingQueue<>(DEFAULT_QUEUE_CAPACITY);
        this.thread = new DefaultThreadFactory(name).newThread(this::batcherLoop);
        this.thread.start();
    }

    <R> void add(@NonNull Operation<R> operation) {
        if (closed) {
            operation.fail(new IllegalStateException("Batcher has been closed"));
            return;
        }
        put(operation);
    }

    /**
     * Flush and fail the open batches belonging to {@code factory} — a client that is closing — so
     * its pending operations complete promptly instead of lingering until an unrelated flush. Runs on
     * the batcher thread (via the command queue) so it observes all of that client's earlier
     * operations. Returns a future that completes once the request has been processed.
     */
    CompletableFuture<Void> closeFactory(@NonNull BatchFactory factory) {
        var done = new CompletableFuture<Void>();
        if (closed) {
            done.complete(null);
            return done;
        }
        put(new CloseFactory(factory, done));
        return done;
    }

    private void put(Object command) {
        try {
            commands.put(command);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void batcherLoop() {
        Object[] local = new Object[DEFAULT_QUEUE_CAPACITY];
        int index = 0;
        int count = 0;

        while (true) {
            try {
                if (index >= count) {
                    if (!hasOpenBatches()) {
                        // No pending batches — block until at least one command arrives.
                        count = commands.takeAll(local);
                    } else {
                        // There are open batches. Non-blocking drain to pick up any commands
                        // that arrived while we were processing.
                        count = commands.pollAll(local, 0, NANOSECONDS);
                        if (count == 0) {
                            // Queue is empty — no concurrent producers are filling it right
                            // now. Flush the open batches immediately rather than lingering,
                            // since there is nothing to batch with.
                            sendAll();

                            // Block until the next command arrives.
                            count = commands.takeAll(local);
                        }
                    }
                    index = 0;
                }
            } catch (InterruptedException e) {
                // Exiting thread
                failPending();
                return;
            }

            Object command = local[index];
            local[index++] = null;
            if (command instanceof Operation<?> operation) {
                process(operation.batchManager().factory(), operation);
            } else if (command instanceof CloseFactory closeFactory) {
                closeFactoryBatches(closeFactory.factory());
                closeFactory.done().complete(null);
            }
        }
    }

    private void process(BatchFactory factory, Operation<?> operation) {
        long shardId = operation.shardId();
        LongObjectHashMap<Batch> shardBatches =
                openBatches.computeIfAbsent(factory, f -> new LongObjectHashMap<>());
        try {
            Batch batch = shardBatches.get(shardId);
            if (batch == null) {
                // Take back a batch parked in the shard's dispatch window, if any: it must keep
                // accumulating, and stay ahead of newer operations, until a slot frees up.
                DispatchWindow window = factory.getDispatchWindow(shardId);
                batch = window != null ? window.reclaim() : null;
                if (batch == null) {
                    batch = factory.getBatch(shardId);
                }
                shardBatches.put(shardId, batch);
            }
            if (!batch.canAdd(operation) && batch.size() > 0) {
                send(factory, shardId, batch);
                batch = factory.getBatch(shardId);
                shardBatches.put(shardId, batch);
            }
            batch.add(operation);
            if (batch.size() >= factory.getConfig().maxRequestsPerBatch()) {
                send(factory, shardId, batch);
                shardBatches.remove(shardId);
            }
        } catch (Exception e) {
            operation.fail(e);
            // Don't leave behind an open batch that the failed operation just created:
            // it would be flushed empty
            Batch open = shardBatches.get(shardId);
            if (open != null && open.size() == 0) {
                shardBatches.remove(shardId);
            }
        }
    }

    private boolean hasOpenBatches() {
        for (LongObjectHashMap<Batch> shardBatches : openBatches.values()) {
            if (!shardBatches.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private void closeFactoryBatches(BatchFactory factory) {
        var closedException = new IllegalStateException("Batch manager is closed");
        LongObjectHashMap<Batch> shardBatches = openBatches.remove(factory);
        if (shardBatches != null) {
            shardBatches.values().forEach(batch -> batch.fail(closedException));
        }
    }

    // Dispatch a batch that takes no more operations, through the shard's window when it has one.
    private static void send(BatchFactory factory, long shardId, Batch batch) {
        DispatchWindow window = factory.getDispatchWindow(shardId);
        if (window == null) {
            batch.send();
        } else {
            window.send(batch);
        }
    }

    private void sendAll() {
        openBatches.forEach(
                (factory, shardBatches) -> {
                    for (LongObjectMap.PrimitiveEntry<Batch> entry : shardBatches.entries()) {
                        // If the shard's window is exhausted, the batch is parked instead: it is
                        // flushed when an in-flight request completes, or reclaimed to accumulate
                        // more operations.
                        DispatchWindow window = factory.getDispatchWindow(entry.key());
                        if (window == null) {
                            entry.value().send();
                        } else {
                            window.sendOrPark(entry.value());
                        }
                    }
                    // Keep the emptied map for the factory's next operations
                    shardBatches.clear();
                });
    }

    private void failPending() {
        var closedException = new IllegalStateException("Batcher has been closed");
        for (LongObjectHashMap<Batch> shardBatches : openBatches.values()) {
            shardBatches.values().forEach(batch -> batch.fail(closedException));
        }
        openBatches.clear();
        Object command;
        while ((command = commands.poll()) != null) {
            if (command instanceof Operation<?> operation) {
                operation.fail(closedException);
            } else if (command instanceof CloseFactory closeFactory) {
                closeFactory.done().complete(null);
            }
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
