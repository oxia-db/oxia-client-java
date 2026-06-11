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
package io.oxia.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.Version;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class GetResultIteratorTest {

    private static GetResult result(String key) {
        return new GetResult(
                key, new byte[0], new Version(0, 0, 0, 0, Optional.empty(), Optional.empty()));
    }

    private static class TestStreamHandle implements FlowControlledRangeScanConsumer.StreamHandle {
        final AtomicInteger requested = new AtomicInteger();
        final AtomicBoolean cancelled = new AtomicBoolean();

        @Override
        public void requestNext() {
            requested.incrementAndGet();
        }

        @Override
        public void cancel() {
            cancelled.set(true);
        }
    }

    @Test
    void producerIsNeverBlocked() {
        var it = new GetResultIterator();
        for (int i = 0; i < 100; i++) {
            assertThat(it.onNext(result("key-" + i))).isTrue();
        }
        it.onCompleted();

        for (int i = 0; i < 100; i++) {
            assertThat(it.hasNext()).isTrue();
            assertThat(it.next().key()).isEqualTo("key-" + i);
        }
        assertThat(it.hasNext()).isFalse();
        assertThatThrownBy(it::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void demandIsSignalledWhenBufferDrains() {
        var it = new GetResultIterator();
        var handle = new TestStreamHandle();
        it.onStreamStarted(handle);

        it.onNext(result("a"));
        it.onNext(result("b"));
        it.onStreamIdle(handle);

        // Buffer is non-empty: the stream must stay parked
        assertThat(handle.requested).hasValue(0);

        assertThat(it.next().key()).isEqualTo("a");
        assertThat(handle.requested).hasValue(0);

        // Draining the last record must resume the parked stream
        assertThat(it.next().key()).isEqualTo("b");
        assertThat(handle.requested).hasValue(1);
    }

    @Test
    void idleStreamIsResumedImmediatelyWhenBufferIsEmpty() {
        var it = new GetResultIterator();
        var handle = new TestStreamHandle();
        it.onStreamStarted(handle);

        it.onStreamIdle(handle);
        assertThat(handle.requested).hasValue(1);
    }

    @Test
    void closeCancelsAllStreams() {
        var it = new GetResultIterator();
        var handle1 = new TestStreamHandle();
        var handle2 = new TestStreamHandle();
        it.onStreamStarted(handle1);
        it.onStreamStarted(handle2);

        it.onNext(result("a"));
        it.close();

        assertThat(handle1.cancelled).isTrue();
        assertThat(handle2.cancelled).isTrue();
        assertThat(it.hasNext()).isFalse();
        assertThat(it.onNext(result("b"))).isFalse();
    }

    @Test
    void streamStartedAfterCloseIsCancelled() {
        var it = new GetResultIterator();
        it.close();

        var handle = new TestStreamHandle();
        it.onStreamStarted(handle);
        assertThat(handle.cancelled).isTrue();
    }

    @Test
    void errorPropagatesAndCloseCancelsStreams() {
        var it = new GetResultIterator();
        var handle = new TestStreamHandle();
        it.onStreamStarted(handle);

        it.onNext(result("a"));
        it.onError(new RuntimeException("scan failed"));

        assertThatThrownBy(it::hasNext)
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("scan failed");

        it.close();
        assertThat(handle.cancelled).isTrue();
    }

    @Test
    void concurrentProducerAndConsumer() throws Exception {
        final int batches = 20;
        final int recordsPerBatch = 5;

        var it = new GetResultIterator();
        var demand = new Semaphore(0);
        var handle =
                new FlowControlledRangeScanConsumer.StreamHandle() {
                    @Override
                    public void requestNext() {
                        demand.release();
                    }

                    @Override
                    public void cancel() {}
                };

        var producer =
                new Thread(
                        () -> {
                            it.onStreamStarted(handle);
                            for (int b = 0; b < batches; b++) {
                                if (b > 0) {
                                    try {
                                        demand.acquire();
                                    } catch (InterruptedException e) {
                                        return;
                                    }
                                }
                                for (int r = 0; r < recordsPerBatch; r++) {
                                    it.onNext(result("key-" + (b * recordsPerBatch + r)));
                                }
                                it.onStreamIdle(handle);
                            }
                            it.onCompleted();
                        });
        producer.start();

        List<String> keys = new ArrayList<>();
        while (it.hasNext()) {
            keys.add(it.next().key());
        }
        producer.join();

        assertThat(keys).hasSize(batches * recordsPerBatch);
        for (int i = 0; i < keys.size(); i++) {
            assertThat(keys.get(i)).isEqualTo("key-" + i);
        }
    }
}
