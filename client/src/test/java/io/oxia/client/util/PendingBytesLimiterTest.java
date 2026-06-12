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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class PendingBytesLimiterTest {

    @Test
    void acquireWithinLimitDoesNotBlock() {
        var limiter = new PendingBytesLimiter(100);
        limiter.acquire(60);
        limiter.acquire(40);
        assertThat(limiter.pendingBytes()).isEqualTo(100);

        limiter.release(60);
        limiter.release(40);
        assertThat(limiter.pendingBytes()).isZero();
    }

    @Test
    void blocksUntilCapacityIsReleased() throws Exception {
        var limiter = new PendingBytesLimiter(100);
        limiter.acquire(80);

        var thread = new Thread(() -> limiter.acquire(50));
        thread.start();

        await().until(() -> thread.getState() == Thread.State.WAITING);
        assertThat(limiter.pendingBytes()).isEqualTo(80);

        limiter.release(80);
        thread.join();
        assertThat(limiter.pendingBytes()).isEqualTo(50);
    }

    @Test
    void oversizedAcquireIsClamped() {
        var limiter = new PendingBytesLimiter(100);
        // An operation larger than the whole limit must still proceed
        limiter.acquire(1000);
        assertThat(limiter.pendingBytes()).isEqualTo(100);

        limiter.release(1000);
        assertThat(limiter.pendingBytes()).isZero();
    }

    @Test
    void interruptedAcquireThrows() throws Exception {
        var limiter = new PendingBytesLimiter(100);
        limiter.acquire(100);

        var failure = new AtomicReference<Throwable>();
        var interruptedFlag = new AtomicReference<Boolean>();
        var thread =
                new Thread(
                        () -> {
                            try {
                                limiter.acquire(1);
                            } catch (Throwable t) {
                                failure.set(t);
                                interruptedFlag.set(Thread.currentThread().isInterrupted());
                            }
                        });
        thread.start();
        await().until(() -> thread.getState() == Thread.State.WAITING);

        thread.interrupt();
        thread.join();
        assertThat(failure.get()).isInstanceOf(RuntimeException.class);
        assertThat(interruptedFlag.get()).isTrue();
    }

    @Test
    void invalidLimit() {
        assertThatThrownBy(() -> new PendingBytesLimiter(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PendingBytesLimiter(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
