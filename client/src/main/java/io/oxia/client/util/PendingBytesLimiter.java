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
package io.oxia.client.util;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Caps the total size of the operations that the client has accepted and not yet completed. {@link
 * #acquire} blocks the calling thread until enough capacity is released, providing backpressure to
 * the application. It must only be invoked from application threads, never from a gRPC transport
 * thread: the capacity is released when responses are processed, so blocking a transport thread
 * could prevent the release that would unblock it.
 *
 * <p>Within the limit, acquire and release are single compare-and-set operations: the lock is only
 * touched when a thread has to wait for capacity.
 */
public final class PendingBytesLimiter {

    private final long maxBytes;
    private final AtomicLong pendingBytes = new AtomicLong();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition released = lock.newCondition();

    // Written under the lock, read without it by release()
    private volatile int waiters;

    public PendingBytesLimiter(long maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be greater than zero: " + maxBytes);
        }
        this.maxBytes = maxBytes;
    }

    /**
     * Blocks until the given number of bytes fits within the limit, then takes it. An operation
     * larger than the whole limit is clamped, so that it can still proceed (alone).
     */
    public void acquire(long bytes) {
        final long required = Math.min(bytes, maxBytes);
        if (tryAcquire(required)) {
            return;
        }

        lock.lock();
        try {
            waiters++;
            try {
                // Re-check after registering as a waiter: a release that happened in between is
                // observed here, while later releases will see the waiter and signal
                while (!tryAcquire(required)) {
                    released.await();
                }
            } finally {
                waiters--;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting on the pending bytes limit", e);
        } finally {
            lock.unlock();
        }
    }

    private boolean tryAcquire(long required) {
        while (true) {
            long current = pendingBytes.get();
            if (current + required > maxBytes) {
                return false;
            }
            if (pendingBytes.compareAndSet(current, current + required)) {
                return true;
            }
        }
    }

    /** Returns capacity taken by {@link #acquire}. Must be called with the same byte count. */
    public void release(long bytes) {
        pendingBytes.addAndGet(-Math.min(bytes, maxBytes));
        if (waiters > 0) {
            lock.lock();
            try {
                released.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    @VisibleForTesting
    long pendingBytes() {
        return pendingBytes.get();
    }
}
