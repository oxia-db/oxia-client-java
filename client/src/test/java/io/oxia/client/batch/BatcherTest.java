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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.oxia.client.ClientConfig;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.GetResult;
import io.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.oxia.client.options.GetOptions;
import io.oxia.proto.KeyComparisonType;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BatcherTest {

    @Mock BatchFactory batchFactory;
    @Mock Batch batch;
    ClientConfig config =
            new ClientConfig(
                    "address",
                    Duration.ofMillis(100),
                    10,
                    1024 * 1024,
                    256L * 1024 * 1024,
                    4,
                    4,
                    1,
                    Duration.ofMillis(1000),
                    "client_id",
                    null,
                    OxiaClientBuilderImpl.DefaultNamespace,
                    null,
                    false,
                    Duration.ofMillis(100),
                    Duration.ofSeconds(30),
                    Duration.ofSeconds(10),
                    Duration.ofSeconds(5),
                    1);

    Batcher batcher;

    @BeforeEach
    void setup() {
        batcher = new Batcher("test-batcher");
    }

    @AfterEach
    void teardown() {
        batcher.close();
    }

    private void add(Operation<?> operation) {
        batcher.add(batchFactory, operation);
    }

    private static Operation<?> newOp(long shardId) {
        return new GetOperation(
                shardId,
                new CompletableFuture<GetResult>(),
                "key",
                new GetOptions(null, true, KeyComparisonType.EQUAL, null));
    }

    @Test
    void singleOperationIsFlushedImmediately() {
        var op = newOp(1L);
        when(batchFactory.getConfig()).thenReturn(config);
        when(batchFactory.getBatch(1L)).thenReturn(batch);
        when(batch.canAdd(any())).thenReturn(true);
        when(batch.size()).thenReturn(1);

        add(op);

        // Single operation with empty queue: the batch is flushed immediately
        await()
                .untilAsserted(
                        () -> {
                            verify(batch).add(op);
                            verify(batch).send();
                        });
    }

    @Test
    void sendBatchWhenFull() {
        when(batchFactory.getConfig()).thenReturn(config);
        when(batchFactory.getBatch(1L)).thenReturn(batch);
        when(batch.canAdd(any())).thenReturn(true);
        when(batch.size()).thenReturn(config.maxRequestsPerBatch());

        add(newOp(1L));

        await().untilAsserted(() -> verify(batch).send());
    }

    @Test
    void sealBatchWhenOperationDoesNotFit() {
        var op = newOp(1L);
        when(batchFactory.getConfig()).thenReturn(config);
        when(batchFactory.getBatch(1L)).thenReturn(batch);
        when(batch.canAdd(any())).thenReturn(false);
        when(batch.size()).thenReturn(1);

        add(op);

        // The operation does not fit in the open batch: the batch is sent and the operation
        // starts a new one
        await()
                .untilAsserted(
                        () -> {
                            verify(batchFactory, times(2)).getBatch(1L);
                            verify(batch).add(op);
                        });
    }

    @Test
    void groupOperationsByShard() {
        var batch2 = mock(Batch.class);
        when(batchFactory.getConfig()).thenReturn(config);
        when(batchFactory.getBatch(1L)).thenReturn(batch);
        when(batchFactory.getBatch(2L)).thenReturn(batch2);
        when(batch.canAdd(any())).thenReturn(true);
        when(batch.size()).thenReturn(1);
        when(batch2.canAdd(any())).thenReturn(true);
        when(batch2.size()).thenReturn(1);

        var op1 = newOp(1L);
        var op2 = newOp(2L);
        add(op1);
        add(op2);

        // Operations of different shards are grouped into different batches, all of them
        // flushed once the queue is empty
        await()
                .untilAsserted(
                        () -> {
                            verify(batch).add(op1);
                            verify(batch).send();
                            verify(batch2).add(op2);
                            verify(batch2).send();
                        });
    }

    @Test
    void failedOperationDoesNotBreakTheBatcher() {
        var op1 = newOp(1L);
        var op2 = newOp(1L);
        when(batchFactory.getConfig()).thenReturn(config);
        when(batchFactory.getBatch(1L)).thenReturn(batch);
        when(batch.canAdd(any())).thenReturn(true);
        doThrow(new RuntimeException("add failed")).when(batch).add(op1);
        // Empty after the failed add, then one operation
        when(batch.size()).thenReturn(0, 1);

        add(op1);
        await().untilAsserted(() -> assertThat(op1.callback()).isCompletedExceptionally());

        add(op2);
        await()
                .untilAsserted(
                        () -> {
                            verify(batch).add(op2);
                            verify(batch).send();
                        });
    }

    @Test
    void addAfterCloseFailsImmediately() {
        batcher.close();

        var op = newOp(1L);
        add(op);
        assertThat(op.callback()).isCompletedExceptionally();
    }

    @Test
    void accumulatesIntoOpenBatchWhileWindowExhausted() {
        var window = new DispatchWindow(1);
        var batch2 = mock(Batch.class);
        when(batchFactory.getConfig()).thenReturn(config);
        when(batchFactory.getDispatchWindow(1L)).thenReturn(window);
        when(batchFactory.getBatch(1L)).thenReturn(batch, batch2);
        when(batch.canAdd(any())).thenReturn(true);
        when(batch.size()).thenReturn(1);
        when(batch2.canAdd(any())).thenReturn(true);
        when(batch2.size()).thenReturn(1, 2);

        var op = newOp(1L);
        // The first batch is flushed immediately and takes the only window slot.
        add(op);
        await().untilAsserted(() -> verify(batch).send());

        // Window exhausted: operations accumulate into the open batch instead of being flushed.
        add(op);
        add(op);
        await().untilAsserted(() -> verify(batch2, times(2)).add(op));
        verify(batch2, never()).send();

        // Completing the in-flight batch frees the slot and flushes the accumulated batch.
        window.release();
        await().untilAsserted(() -> verify(batch2).send());
    }

    @Test
    void fullBatchIsQueuedWhileWindowExhausted() {
        var window = new DispatchWindow(1);
        var batch2 = mock(Batch.class);
        var batch3 = mock(Batch.class);
        when(batchFactory.getConfig()).thenReturn(config);
        when(batchFactory.getDispatchWindow(1L)).thenReturn(window);
        when(batchFactory.getBatch(1L)).thenReturn(batch, batch2, batch3);
        when(batch.canAdd(any())).thenReturn(true);
        when(batch.size()).thenReturn(1);
        when(batch2.canAdd(any())).thenReturn(true);
        when(batch2.size()).thenReturn(config.maxRequestsPerBatch());
        when(batch3.canAdd(any())).thenReturn(true);
        when(batch3.size()).thenReturn(1);

        var op = newOp(1L);
        add(op);
        await().untilAsserted(() -> verify(batch).send());

        // A batch that fills up while the window is exhausted is queued, not dispatched...
        add(op);
        await().untilAsserted(() -> verify(batch2).add(op));
        verify(batch2, never()).send();

        // ...and the batcher thread moves on to later operations without blocking.
        add(op);
        await().untilAsserted(() -> verify(batch3).add(op));
        verify(batch3, never()).send();

        // Freed slots dispatch the queued full batch first, then the parked open batch.
        window.release();
        await().untilAsserted(() -> verify(batch2).send());
        verify(batch3, never()).send();
        window.release();
        await().untilAsserted(() -> verify(batch3).send());
    }

    @Test
    void exhaustedWindowDoesNotBlockOtherShards() {
        var window1 = new DispatchWindow(1);
        var window2 = new DispatchWindow(1);
        var shard2Batch = mock(Batch.class);
        when(batchFactory.getConfig()).thenReturn(config);
        when(batchFactory.getDispatchWindow(1L)).thenReturn(window1);
        when(batchFactory.getDispatchWindow(2L)).thenReturn(window2);
        when(batchFactory.getBatch(1L)).thenReturn(batch);
        when(batchFactory.getBatch(2L)).thenReturn(shard2Batch);
        when(batch.canAdd(any())).thenReturn(true);
        when(batch.size()).thenReturn(1, 2);
        when(shard2Batch.canAdd(any())).thenReturn(true);
        when(shard2Batch.size()).thenReturn(1);

        // Exhaust shard 1's window: the first batch holds the only slot, the second one parks.
        add(newOp(1L));
        await().untilAsserted(() -> verify(batch).send());
        add(newOp(1L));
        await().untilAsserted(() -> verify(batch, times(2)).add(any()));

        // Shard 2 flows on the same batcher thread: its own window has a free slot.
        add(newOp(2L));
        await().untilAsserted(() -> verify(shard2Batch).send());
        verify(batch, times(1)).send();
    }
}
