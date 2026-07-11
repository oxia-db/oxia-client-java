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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

class WriteWindowTest {

    @Test
    void dispatchesWhileWindowHasSlots() {
        var window = new WriteWindow(2);
        var batch1 = mock(Batch.class);
        var batch2 = mock(Batch.class);

        window.send(batch1);
        window.send(batch2);

        verify(batch1).send();
        verify(batch2).send();
    }

    @Test
    void queuesFullBatchesWhileExhaustedAndDispatchesInOrder() {
        var window = new WriteWindow(1);
        var batch1 = mock(Batch.class);
        var batch2 = mock(Batch.class);
        var batch3 = mock(Batch.class);

        window.send(batch1);
        window.send(batch2);
        window.send(batch3);
        verify(batch1).send();
        verify(batch2, never()).send();
        verify(batch3, never()).send();

        window.release();
        verify(batch2).send();
        verify(batch3, never()).send();

        window.release();
        verify(batch3).send();
    }

    @Test
    void sendOrParkDispatchesWhenSlotAvailable() {
        var window = new WriteWindow(1);
        var batch = mock(Batch.class);

        window.sendOrPark(batch);

        verify(batch).send();
    }

    @Test
    void releaseFlushesParkedBatch() {
        var window = new WriteWindow(1);
        var batch1 = mock(Batch.class);
        var batch2 = mock(Batch.class);

        window.send(batch1);
        window.sendOrPark(batch2);
        verify(batch2, never()).send();

        window.release();
        verify(batch2).send();

        // The parked batch consumed the released slot: the window is still exhausted.
        var batch3 = mock(Batch.class);
        window.sendOrPark(batch3);
        verify(batch3, never()).send();
    }

    @Test
    void parkedBatchFlushedAfterQueuedBatches() {
        var window = new WriteWindow(1);
        var inflight = mock(Batch.class);
        var full = mock(Batch.class);
        var open = mock(Batch.class);

        window.send(inflight);
        window.send(full); // queued: full batch
        window.sendOrPark(open); // parked: open batch, younger operations

        window.release();
        verify(full).send();
        verify(open, never()).send();

        window.release();
        verify(open).send();
    }

    @Test
    void reclaimTakesBackParkedBatch() {
        var window = new WriteWindow(1);
        var batch1 = mock(Batch.class);
        var batch2 = mock(Batch.class);

        window.send(batch1);
        window.sendOrPark(batch2);

        assertThat(window.reclaim()).isSameAs(batch2);
        assertThat(window.reclaim()).isNull();

        // A reclaimed batch is not flushed when a slot is released...
        window.release();
        verify(batch2, never()).send();

        // ...and the freed slot is available for the next dispatch.
        window.sendOrPark(batch2);
        verify(batch2).send();
    }

    @Test
    void failsHeldBackBatches() {
        var window = new WriteWindow(1);
        var inflight = mock(Batch.class);
        var full = mock(Batch.class);
        var open = mock(Batch.class);

        window.send(inflight);
        window.send(full);
        window.sendOrPark(open);

        var error = new IllegalStateException("Batch manager is closed");
        window.fail(error);
        verify(full).fail(error);
        verify(open).fail(error);
        verify(full, never()).send();
        verify(open, never()).send();

        // Slots released afterwards have nothing left to dispatch.
        window.release();
        verify(full, never()).send();
        verify(open, never()).send();
    }
}
