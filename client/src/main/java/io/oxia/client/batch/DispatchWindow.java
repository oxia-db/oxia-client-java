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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded in-flight window for the batches of one (client, shard) pair.
 *
 * <p>The window limits how many batches may be outstanding to the server at any time, so that
 * batching adapts to the rate at which the server is servicing requests: while the window is
 * exhausted, the shard's open batch keeps accumulating operations (up to the batch limits) instead
 * of being flushed as a new tiny request. A fast server keeps the window open and batches stay
 * small, minimizing latency; a saturated server exhausts the window and batches grow, maximizing
 * throughput.
 *
 * <p>No method ever blocks: the batcher thread is shared with other shards and clients, so a slow
 * shard must not stall it. Batches that cannot be dispatched are held back — full batches in a FIFO
 * queue, the open batch in a single parked slot — and dispatched by {@link #release} as in-flight
 * batches complete. The batcher parks the open batch only when it goes idle, and takes it back
 * through {@link #reclaim} before opening a new batch for the shard, so the parked batch always
 * carries the youngest operations and dispatch order matches submission order.
 */
final class DispatchWindow {

    private final ReentrantLock lock = new ReentrantLock();
    private final int maxBatchesInFlight;
    private int batchesInFlight;

    // Full batches waiting for a slot, oldest first.
    private final ArrayDeque<Batch> readyBatches = new ArrayDeque<>();

    // Open batch parked at idle, awaiting either a slot or more operations.
    private Batch parkedBatch;

    DispatchWindow(int maxBatchesInFlight) {
        this.maxBatchesInFlight = maxBatchesInFlight;
    }

    /** Dispatch a batch that accepts no more operations, queueing it if the window is exhausted. */
    void send(Batch batch) {
        lock.lock();
        try {
            if (batchesInFlight >= maxBatchesInFlight) {
                readyBatches.addLast(batch);
                return;
            }
            batchesInFlight++;
        } finally {
            lock.unlock();
        }
        batch.send();
    }

    /**
     * Dispatch the open batch if the window has a free slot, otherwise park it. A parked batch is
     * either flushed by {@link #release} when an in-flight batch completes, or taken back by the
     * batcher through {@link #reclaim} to accumulate more operations.
     */
    void sendOrPark(Batch batch) {
        lock.lock();
        try {
            if (batchesInFlight >= maxBatchesInFlight) {
                parkedBatch = batch;
                return;
            }
            batchesInFlight++;
        } finally {
            lock.unlock();
        }
        batch.send();
    }

    /** Take back the parked batch, if it has not been flushed yet. */
    Batch reclaim() {
        lock.lock();
        try {
            Batch batch = parkedBatch;
            parkedBatch = null;
            return batch;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Return the slot of a completed batch, dispatching the oldest queued batch, or, when none is
     * queued, flushing the parked batch.
     */
    void release() {
        lock.lock();
        try {
            batchesInFlight--;
            Batch next = readyBatches.pollFirst();
            if (next == null) {
                next = parkedBatch;
                parkedBatch = null;
            }
            if (next != null) {
                batchesInFlight++;
                // Send while holding the lock: a slot freed concurrently must not let the batcher
                // dispatch a newer batch ahead of this one.
                next.send();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Fail every batch that is still held back — the owning client is closing. */
    void fail(Throwable error) {
        List<Batch> toFail;
        lock.lock();
        try {
            toFail = new ArrayList<>(readyBatches);
            readyBatches.clear();
            if (parkedBatch != null) {
                toFail.add(parkedBatch);
                parkedBatch = null;
            }
        } finally {
            lock.unlock();
        }
        toFail.forEach(batch -> batch.fail(error));
    }
}
