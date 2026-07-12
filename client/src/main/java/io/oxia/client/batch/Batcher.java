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
 * <p>Each operation carries the {@link BatchFactory} of the client that submitted it (the factory
 * binds to that client's {@code RpcProvider}, session and config), so a single batcher can serve
 * many clients without being tied to any one of them.
 */
final class Batcher implements AutoCloseable {

    private static final int DEFAULT_QUEUE_CAPACITY = 10_000;

    /** Identifies an open batch: the submitting client's factory and the target shard. */
    private record BatchKey(BatchFactory factory, long shardId) {}

    sealed interface Command permits Enqueue, CloseFactory {}

    record Enqueue(BatchFactory factory, Operation<?> operation) implements Command {}

    record CloseFactory(BatchFactory factory, CompletableFuture<Void> done) implements Command {}

    @NonNull private final BatchedArrayBlockingQueue<Command> commands;

    // Open batches, grouped by (client factory, shard). Only accessed by the batcher thread.
    private final Map<BatchKey, Batch> openBatches = new HashMap<>();

    private final Thread thread;
    private volatile boolean closed;

    Batcher(String name) {
        this.commands = new BatchedArrayBlockingQueue<>(DEFAULT_QUEUE_CAPACITY);
        this.thread = new DefaultThreadFactory(name).newThread(this::batcherLoop);
        this.thread.start();
    }

    <R> void add(@NonNull BatchFactory factory, @NonNull Operation<R> operation) {
        if (closed) {
            operation.fail(new IllegalStateException("Batcher has been closed"));
            return;
        }
        put(new Enqueue(factory, operation));
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

    private void put(Command command) {
        try {
            commands.put(command);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void batcherLoop() {
        Command[] local = new Command[DEFAULT_QUEUE_CAPACITY];
        int index = 0;
        int count = 0;

        while (true) {
            try {
                if (index >= count) {
                    if (openBatches.isEmpty()) {
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

            Command command = local[index];
            local[index++] = null;
            if (command instanceof Enqueue enqueue) {
                process(enqueue.factory(), enqueue.operation());
            } else if (command instanceof CloseFactory closeFactory) {
                closeFactoryBatches(closeFactory.factory());
                closeFactory.done().complete(null);
            }
        }
    }

    private void process(BatchFactory factory, Operation<?> operation) {
        var key = new BatchKey(factory, operation.shardId());
        try {
            Batch batch = openBatches.get(key);
            if (batch == null) {
                // Take back a batch parked in the shard's dispatch window, if any: it must keep
                // accumulating, and stay ahead of newer operations, until a slot frees up.
                DispatchWindow window = factory.getDispatchWindow(operation.shardId());
                batch = window != null ? window.reclaim() : null;
                if (batch == null) {
                    batch = factory.getBatch(operation.shardId());
                }
                openBatches.put(key, batch);
            }
            if (!batch.canAdd(operation) && batch.size() > 0) {
                send(factory, operation.shardId(), batch);
                batch = factory.getBatch(operation.shardId());
                openBatches.put(key, batch);
            }
            batch.add(operation);
            if (batch.size() >= factory.getConfig().maxRequestsPerBatch()) {
                send(factory, operation.shardId(), batch);
                openBatches.remove(key);
            }
        } catch (Exception e) {
            operation.fail(e);
            // Don't leave behind an open batch that the failed operation just created:
            // it would be flushed empty
            Batch open = openBatches.get(key);
            if (open != null && open.size() == 0) {
                openBatches.remove(key);
            }
        }
    }

    private void closeFactoryBatches(BatchFactory factory) {
        var closedException = new IllegalStateException("Batch manager is closed");
        openBatches
                .entrySet()
                .removeIf(
                        entry -> {
                            if (entry.getKey().factory() == factory) {
                                entry.getValue().fail(closedException);
                                return true;
                            }
                            return false;
                        });
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
                (key, batch) -> {
                    // If the shard's window is exhausted, the batch is parked instead: it is
                    // flushed when an in-flight request completes, or reclaimed to accumulate
                    // more operations.
                    DispatchWindow window = key.factory().getDispatchWindow(key.shardId());
                    if (window == null) {
                        batch.send();
                    } else {
                        window.sendOrPark(batch);
                    }
                });
        openBatches.clear();
    }

    private void failPending() {
        var closedException = new IllegalStateException("Batcher has been closed");
        openBatches.values().forEach(batch -> batch.fail(closedException));
        openBatches.clear();
        Command command;
        while ((command = commands.poll()) != null) {
            if (command instanceof Enqueue enqueue) {
                enqueue.operation().fail(closedException);
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
