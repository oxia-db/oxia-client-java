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

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class MpscUnboundedBatchedQueueTest {

    @Test
    public void simple() throws Exception {
        var queue = new MpscUnboundedBatchedQueue<Integer>(4);

        assertNull(queue.poll());
        assertNull(queue.peek());
        assertEquals(0, queue.size());
        assertEquals(Integer.MAX_VALUE, queue.remainingCapacity());
        assertThrows(NoSuchElementException.class, queue::element);
        assertThrows(UnsupportedOperationException.class, queue::iterator);
        assertThrows(NullPointerException.class, () -> queue.offer(null));

        // Across many chunks
        for (int i = 0; i < 100; i++) {
            queue.add(i);
            assertEquals(i, queue.peek());
            assertEquals(i, queue.take());
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(queue.offer(i));
        }
        assertEquals(10, queue.size());

        List<Integer> list = new ArrayList<>();
        assertEquals(3, queue.drainTo(list, 3));
        assertEquals(List.of(0, 1, 2), list);
        assertEquals(7, queue.size());
        assertEquals(3, queue.remove());

        queue.clear();
        assertTrue(queue.isEmpty());
        assertNull(queue.poll());
    }

    @Test
    public void drainInBatches() throws Exception {
        var queue = new MpscUnboundedBatchedQueue<Integer>(4);
        for (int i = 0; i < 100; i++) {
            queue.put(i);
        }

        Integer[] batch = new Integer[7];
        int next = 0;
        while (next < 100) {
            int count = queue.takeAll(batch);
            assertEquals(Math.min(batch.length, 100 - next), count);
            for (int i = 0; i < count; i++) {
                assertEquals(next++, batch[i]);
            }
        }
        assertEquals(0, queue.pollAll(batch, 0, TimeUnit.MILLISECONDS));
        assertEquals(0, queue.size());
    }

    @Test
    public void putAll() throws Exception {
        var queue = new MpscUnboundedBatchedQueue<Integer>(4);
        queue.putAll(new Integer[] {-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1}, 1, 10);

        Integer[] batch = new Integer[20];
        assertEquals(10, queue.takeAll(batch));
        for (int i = 0; i < 10; i++) {
            assertEquals(i, batch[i]);
        }

        // A null is rejected before any slot is claimed: a slot claimed and never published would
        // stall the consumer
        assertThrows(NullPointerException.class, () -> queue.putAll(new Integer[] {1, null, 3}, 0, 3));
        assertEquals(0, queue.size());
        queue.put(42);
        assertEquals(42, queue.poll());
    }

    @Test
    @Timeout(10)
    public void takeAllWaitsForProducer() throws Exception {
        var queue = new MpscUnboundedBatchedQueue<Integer>();
        var taken = new CompletableFuture<List<Integer>>();
        startDaemon(
                () -> {
                    try {
                        Integer[] batch = new Integer[10];
                        int count = queue.takeAll(batch);
                        taken.complete(List.of(Arrays.copyOf(batch, count)));
                    } catch (Throwable t) {
                        taken.completeExceptionally(t);
                    }
                });

        Thread.sleep(100);
        assertFalse(taken.isDone());

        queue.put(7);
        assertEquals(List.of(7), taken.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void pollTimeout() throws Exception {
        var queue = new MpscUnboundedBatchedQueue<Integer>();
        Integer[] batch = new Integer[10];

        long start = System.nanoTime();
        assertEquals(0, queue.pollAll(batch, 50, TimeUnit.MILLISECONDS));
        assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(50));
        assertNull(queue.poll(1, TimeUnit.MILLISECONDS));

        // 0 timeout should not block
        assertEquals(0, queue.pollAll(batch, 0, TimeUnit.HOURS));

        queue.put(1);
        assertEquals(1, queue.pollAll(batch, 1, TimeUnit.HOURS));
        assertEquals(1, batch[0]);
        queue.put(2);
        assertEquals(2, queue.poll(1, TimeUnit.HOURS));
    }

    @Test
    @Timeout(10)
    public void interruptWakesUpConsumer() throws Exception {
        var queue = new MpscUnboundedBatchedQueue<Integer>();
        var outcome = new CompletableFuture<Throwable>();
        Thread consumer =
                startDaemon(
                        () -> {
                            try {
                                queue.takeAll(new Integer[10]);
                                outcome.complete(null);
                            } catch (Throwable t) {
                                outcome.complete(t);
                            }
                        });

        Thread.sleep(100);
        consumer.interrupt();
        assertInstanceOf(InterruptedException.class, outcome.get(5, TimeUnit.SECONDS));
    }

    @Test
    @Timeout(60)
    public void concurrentProducers() throws Exception {
        // Chunks of 2 slots make the producers append chunks, and walk back to theirs, all the time
        var queue = new MpscUnboundedBatchedQueue<Long>(2);
        int producers = 8;
        int perProducer = 200_000;

        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            long id = p;
            threads.add(startDaemon(() -> produce(queue, id, perProducer)));
        }

        // Every producer's elements come out in order, none lost or duplicated
        int[] nextSequence = new int[producers];
        Long[] batch = new Long[64];
        int received = 0;
        int drains = 0;
        while (received < producers * perProducer) {
            int count =
                    drains++ % 2 == 0 ? queue.takeAll(batch) : queue.pollAll(batch, 0, TimeUnit.NANOSECONDS);
            for (int i = 0; i < count; i++) {
                int id = (int) (batch[i] >>> 32);
                int sequence = (int) (long) batch[i];
                assertEquals(nextSequence[id]++, sequence, "producer " + id);
            }
            received += count;
        }

        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(0, queue.size());
        assertNull(queue.poll());
    }

    @Test
    @Timeout(60)
    public void noLostWakeUps() throws Exception {
        // Each element arrives while the consumer is going to sleep, or already parked
        var queue = new MpscUnboundedBatchedQueue<Integer>();
        var consumed = new Semaphore(0);
        int elements = 50_000;
        startDaemon(
                () -> {
                    Integer[] batch = new Integer[16];
                    try {
                        int received = 0;
                        while (received < elements) {
                            int count = queue.takeAll(batch);
                            received += count;
                            consumed.release(count);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

        for (int i = 0; i < elements; i++) {
            queue.put(i);
            assertTrue(consumed.tryAcquire(5, TimeUnit.SECONDS), "lost wake-up at element " + i);
        }
    }

    // Publishes the elements (id << 32 | sequence): odd producers in batches of 3, the others one by
    // one
    private static void produce(MpscUnboundedBatchedQueue<Long> queue, long id, int count) {
        int i = 0;
        while (i < count) {
            if (id % 2 == 0 || count - i < 3) {
                queue.put(id << 32 | i++);
            } else {
                queue.putAll(new Long[] {id << 32 | i, id << 32 | (i + 1), id << 32 | (i + 2)}, 0, 3);
                i += 3;
            }
        }
    }

    private static Thread startDaemon(Runnable task) {
        Thread thread = new Thread(task);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }
}
