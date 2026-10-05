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
package io.oxia.client.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import lombok.NonNull;

/**
 * An unbounded, lock-free, multi-producer single-consumer queue that the consumer drains in
 * batches.
 *
 * <p>A producer claims a slot with a single atomic increment of the producer index, so contending
 * producers never retry, and then publishes its element into the slot. Slots live in fixed-size
 * chunks linked together: the queue grows by appending a chunk, never by copying, and producers
 * never block. The next chunk is appended as soon as a chunk starts being used, so that producers
 * rarely allocate between claiming a slot and publishing into it. The consumer reads the producer
 * index to know how far it may go, takes every element up to it in one pass, stopping early at a
 * claimed slot whose element is not published yet, and advances the consumer index once for the
 * whole batch.
 *
 * <p>The consumer waits without a lock, and never yields: on macOS, {@code Thread.yield()} can
 * stall the thread for a whole 10 ms scheduler quantum. Whether the queue is empty or the next slot
 * is claimed but not published yet, the consumer spins briefly, then announces which slot it waits
 * for, re-checks it and parks. Only the producer that publishes that slot unparks it, so producers
 * pay for a wake-up only when the consumer is actually parked waiting for them.
 *
 * <p>The design follows JCTools' {@code MpscUnboundedXaddArrayQueue}, without chunk pooling and
 * with a lock-free chunk append.
 *
 * <p>Only one thread may consume at a time.
 */
public final class MpscUnboundedBatchedQueue<T> extends MpscUnboundedBatchedQueuePad3<T>
        implements BatchedBlockingQueue<T> {

    private static final int DEFAULT_CHUNK_SIZE = 1024;

    // How many times the consumer spins, on an empty queue or on a claimed but not yet published
    // slot, before it parks
    private static final int SPINS_BEFORE_PARK = 64;

    private static final VarHandle PRODUCER_INDEX;
    private static final VarHandle PRODUCER_CHUNK;
    private static final VarHandle CONSUMER_WAITING_FOR;
    private static final VarHandle CONSUMER_INDEX;
    private static final VarHandle NEXT;
    private static final VarHandle SLOT = MethodHandles.arrayElementVarHandle(Object[].class);

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            PRODUCER_INDEX =
                    lookup.findVarHandle(
                            MpscUnboundedBatchedQueueProducerFields.class, "producerIndex", long.class);
            PRODUCER_CHUNK =
                    lookup.findVarHandle(
                            MpscUnboundedBatchedQueueSharedFields.class, "producerChunk", Chunk.class);
            CONSUMER_WAITING_FOR =
                    lookup.findVarHandle(
                            MpscUnboundedBatchedQueueSharedFields.class, "consumerWaitingFor", long.class);
            CONSUMER_INDEX =
                    lookup.findVarHandle(
                            MpscUnboundedBatchedQueueConsumerFields.class, "consumerIndex", long.class);
            NEXT = lookup.findVarHandle(Chunk.class, "next", Chunk.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Slot {@code i} holds the element at queue index {@code (index << chunkShift) + i}. */
    static final class Chunk {
        final long index;
        final Object[] slots;

        // Set once, by the producer that appends the next chunk. Never cleared: a producer may
        // still walk forward from a chunk that the consumer has already left.
        volatile Chunk next;

        // Cleared by the consumer when it enters this chunk, letting the consumed ones be collected
        volatile Chunk prev;

        Chunk(long index, Chunk prev, int size) {
            this.index = index;
            this.prev = prev;
            this.slots = new Object[size];
        }
    }

    private final int chunkMask;
    private final int chunkShift;

    // Consumer-owned buffer for the single-element operations
    private final Object[] single = new Object[1];

    public MpscUnboundedBatchedQueue() {
        this(DEFAULT_CHUNK_SIZE);
    }

    MpscUnboundedBatchedQueue(int chunkSize) {
        if (chunkSize <= 0 || Integer.bitCount(chunkSize) != 1) {
            throw new IllegalArgumentException("chunkSize must be a power of 2: " + chunkSize);
        }
        this.chunkMask = chunkSize - 1;
        this.chunkShift = Integer.numberOfTrailingZeros(chunkSize);
        Chunk first = new Chunk(0, null, chunkSize);
        this.consumerChunk = first;
        this.producerChunk = first;
    }

    @Override
    public boolean offer(@NonNull T e) {
        long index = (long) PRODUCER_INDEX.getAndAdd(this, 1L);
        Chunk chunk = chunkFor(index);
        int offset = (int) (index & chunkMask);
        // Volatile: the publication must not be reordered with the check for a waiting consumer
        SLOT.setVolatile(chunk.slots, offset, e);
        wakeUpConsumer(index, index + 1);
        if (offset == 0) {
            appendNext(chunk);
        }
        return true;
    }

    @Override
    public boolean offer(@NonNull T e, long timeout, @NonNull TimeUnit unit) {
        return offer(e);
    }

    @Override
    public void put(@NonNull T e) {
        offer(e);
    }

    @Override
    public void putAll(@NonNull T[] a, int offset, int len) {
        if (len == 0) {
            return;
        }
        // Check before claiming: a claimed slot that is never published would stall the consumer
        for (int i = offset; i < offset + len; i++) {
            Objects.requireNonNull(a[i]);
        }
        long first = (long) PRODUCER_INDEX.getAndAdd(this, (long) len);
        Chunk chunk = chunkFor(first);
        Chunk started = null;
        for (int i = 0; i < len; i++) {
            long index = first + i;
            if (chunk.index != index >>> chunkShift) {
                chunk = findChunk(chunk, index >>> chunkShift);
            }
            int slot = (int) (index & chunkMask);
            SLOT.setVolatile(chunk.slots, slot, a[offset + i]);
            if (slot == 0) {
                started = chunk;
            }
        }
        wakeUpConsumer(first, first + len);
        if (started != null) {
            appendNext(started);
        }
    }

    // The producer that takes the first slot of a chunk appends the next one, after publishing its
    // element: a producer allocating a chunk between claiming its slot and publishing it would
    // stall the consumer meanwhile
    private void appendNext(Chunk chunk) {
        if (chunk.next == null) {
            NEXT.compareAndSet(chunk, (Chunk) null, new Chunk(chunk.index + 1, chunk, chunkMask + 1));
        }
    }

    private Chunk chunkFor(long index) {
        Chunk chunk = producerChunk;
        long chunkIndex = index >>> chunkShift;
        return chunk.index == chunkIndex ? chunk : findChunk(chunk, chunkIndex);
    }

    private Chunk findChunk(Chunk chunk, long chunkIndex) {
        // Producers that claimed later slots may have moved past this chunk: walk back. The links
        // are intact, since the consumer unlinks a chunk only after consuming all of it, including
        // this producer's slot.
        while (chunk.index > chunkIndex) {
            chunk = chunk.prev;
        }
        // Or the slot is past the last chunk: walk forward, appending the missing chunks
        while (chunk.index < chunkIndex) {
            Chunk next = chunk.next;
            if (next == null) {
                Chunk appended = new Chunk(chunk.index + 1, chunk, chunkMask + 1);
                next = NEXT.compareAndSet(chunk, (Chunk) null, appended) ? appended : chunk.next;
            }
            chunk = next;
        }
        // Let the later producers start from here
        Chunk last = producerChunk;
        while (last.index < chunk.index && !PRODUCER_CHUNK.compareAndSet(this, last, chunk)) {
            last = producerChunk;
        }
        return chunk;
    }

    // Wakes up the consumer if it waits for one of the slots just published, in [from, to)
    private void wakeUpConsumer(long from, long to) {
        long waitingFor = consumerWaitingFor;
        if (waitingFor >= from
                && waitingFor < to
                && CONSUMER_WAITING_FOR.compareAndSet(this, waitingFor, -1L)) {
            LockSupport.unpark(consumerThread);
        }
    }

    @Override
    public int takeAll(@NonNull T[] array) throws InterruptedException {
        return drainOrWait(array, false, 0L);
    }

    @Override
    public int pollAll(@NonNull T[] array, long timeout, @NonNull TimeUnit unit)
            throws InterruptedException {
        return drainOrWait(array, true, unit.toNanos(timeout));
    }

    @Override
    @SuppressWarnings("unchecked")
    public T take() throws InterruptedException {
        drainOrWait((T[]) single, false, 0L);
        return takeSingle();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T poll(long timeout, @NonNull TimeUnit unit) throws InterruptedException {
        return drainOrWait((T[]) single, true, unit.toNanos(timeout)) > 0 ? takeSingle() : null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T poll() {
        return drain((T[]) single, 1) > 0 ? takeSingle() : null;
    }

    @SuppressWarnings("unchecked")
    private T takeSingle() {
        T e = (T) single[0];
        single[0] = null;
        return e;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T peek() {
        long index = consumerIndex;
        if (producerIndex == index) {
            return null;
        }
        Chunk chunk = consumerChunk;
        if (chunk.index != index >>> chunkShift) {
            chunk = chunk.next;
            if (chunk == null) {
                return null;
            }
        }
        return (T) SLOT.getAcquire(chunk.slots, (int) (index & chunkMask));
    }

    private int drainOrWait(T[] array, boolean timed, long nanos) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        int drained = drain(array, array.length);
        if (drained > 0 || array.length == 0 || (timed && nanos <= 0)) {
            return drained;
        }
        long deadline = timed ? System.nanoTime() + nanos : 0L;
        int spins = 0;
        while (true) {
            // Wait for the next slot: on an empty queue, for a producer to claim and publish it;
            // otherwise for the producer that claimed it to publish it, usually a few instructions
            // away. Spin briefly, then park: that producer wakes us up.
            if (++spins < SPINS_BEFORE_PARK) {
                Thread.onSpinWait();
            } else {
                park(timed, deadline);
            }
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            drained = drain(array, array.length);
            if (drained > 0) {
                return drained;
            }
            if (timed && deadline - System.nanoTime() <= 0) {
                return 0;
            }
        }
    }

    private void park(boolean timed, long deadline) {
        consumerThread = Thread.currentThread();
        long index = consumerIndex;
        consumerWaitingFor = index;
        // Re-check after announcing: from now on, the producer that publishes this slot wakes us up
        if (!isPublished(index)) {
            if (timed) {
                LockSupport.parkNanos(this, deadline - System.nanoTime());
            } else {
                LockSupport.park(this);
            }
        }
        consumerWaitingFor = -1;
    }

    // Whether the element at the consumer index is published. It may be in the next chunk.
    private boolean isPublished(long index) {
        Chunk chunk = consumerChunk;
        if (chunk.index != index >>> chunkShift) {
            chunk = chunk.next;
            if (chunk == null) {
                return false;
            }
        }
        return SLOT.getVolatile(chunk.slots, (int) (index & chunkMask)) != null;
    }

    // Moves the published elements, up to the first one that is not published yet, into the
    // array and advances the consumer index once for all of them
    @SuppressWarnings("unchecked")
    private int drain(T[] array, int limit) {
        long index = consumerIndex;
        int available = (int) Math.min(producerIndex - index, limit);
        Chunk chunk = consumerChunk;
        int drained = 0;
        while (drained < available) {
            if (chunk.index != index >>> chunkShift) {
                Chunk next = chunk.next;
                if (next == null) {
                    // The producer that claimed this slot is still appending its chunk
                    break;
                }
                next.prev = null;
                chunk = next;
            }
            int offset = (int) (index & chunkMask);
            Object e = SLOT.getAcquire(chunk.slots, offset);
            if (e == null) {
                // Claimed, not published yet
                break;
            }
            chunk.slots[offset] = null;
            array[drained++] = (T) e;
            index++;
        }
        consumerChunk = chunk;
        CONSUMER_INDEX.setRelease(this, index);
        return drained;
    }

    @Override
    public int drainTo(@NonNull Collection<? super T> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(@NonNull Collection<? super T> c, int maxElements) {
        int drained = 0;
        T e;
        while (drained < maxElements && (e = poll()) != null) {
            c.add(e);
            drained++;
        }
        return drained;
    }

    @Override
    public int size() {
        long consumed = (long) CONSUMER_INDEX.getAcquire(this);
        return (int) Math.min(producerIndex - consumed, Integer.MAX_VALUE);
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public @NonNull Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }
}

// Padding, as in JCTools, keeps the fields that the producers write, the ones they only read and
// the ones that the consumer writes on separate cache lines.

abstract class MpscUnboundedBatchedQueuePad0<T> extends AbstractQueue<T> {
    long p00, p01, p02, p03, p04, p05, p06, p07, p08, p09, p10, p11, p12, p13, p14, p15;
}

abstract class MpscUnboundedBatchedQueueProducerFields<T> extends MpscUnboundedBatchedQueuePad0<T> {
    volatile long producerIndex;
}

abstract class MpscUnboundedBatchedQueuePad1<T> extends MpscUnboundedBatchedQueueProducerFields<T> {
    long p00, p01, p02, p03, p04, p05, p06, p07, p08, p09, p10, p11, p12, p13, p14, p15;
}

// Read by the producers on every offer, written rarely: when a chunk is appended, and when the
// consumer parks or wakes up
abstract class MpscUnboundedBatchedQueueSharedFields<T> extends MpscUnboundedBatchedQueuePad1<T> {
    volatile MpscUnboundedBatchedQueue.Chunk producerChunk;
    // The index of the slot that the parked consumer waits for, or -1
    volatile long consumerWaitingFor = -1;
    Thread consumerThread;
}

abstract class MpscUnboundedBatchedQueuePad2<T> extends MpscUnboundedBatchedQueueSharedFields<T> {
    long p00, p01, p02, p03, p04, p05, p06, p07, p08, p09, p10, p11, p12, p13, p14, p15;
}

abstract class MpscUnboundedBatchedQueueConsumerFields<T> extends MpscUnboundedBatchedQueuePad2<T> {
    long consumerIndex;
    MpscUnboundedBatchedQueue.Chunk consumerChunk;
}

abstract class MpscUnboundedBatchedQueuePad3<T> extends MpscUnboundedBatchedQueueConsumerFields<T> {
    long p00, p01, p02, p03, p04, p05, p06, p07, p08, p09, p10, p11, p12, p13, p14, p15;
}
