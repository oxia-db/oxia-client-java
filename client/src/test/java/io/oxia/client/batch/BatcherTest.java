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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BatcherTest {

    @Mock BatchFactory batchFactory;
    @Mock Batch batch;
    long shardId = 1L;
    ClientConfig config =
            new ClientConfig(
                    "address",
                    Duration.ofMillis(100),
                    10,
                    1024 * 1024,
                    256L * 1024 * 1024,
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

    /** Collects the scheduled tasks, so that the tests control exactly when batching runs. */
    static class ManualExecutor implements Executor {
        final List<Runnable> tasks = new ArrayList<>();

        @Override
        public void execute(Runnable task) {
            tasks.add(task);
        }

        void runAll() {
            while (!tasks.isEmpty()) {
                tasks.remove(0).run();
            }
        }
    }

    ManualExecutor executor = new ManualExecutor();
    Batcher batcher;

    @BeforeEach
    void setup() {
        batcher = new Batcher(config, shardId, batchFactory, executor);
    }

    private static Operation<?> newOp() {
        return new GetOperation(
                new CompletableFuture<GetResult>(),
                "key",
                new GetOptions(null, true, KeyComparisonType.EQUAL, null));
    }

    @Test
    void singleOperationIsFlushedImmediately() {
        var op = newOp();
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.canAdd(any())).thenReturn(true);
        when(batch.size()).thenReturn(1);

        batcher.add(op);
        executor.runAll();

        verify(batch).add(op);
        verify(batch).send();
    }

    @Test
    void operationsAreCoalescedIntoOneBatch() {
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.canAdd(any())).thenReturn(true);
        when(batch.size()).thenReturn(1, 2, 3);

        // The three operations are already queued when the batching task runs
        batcher.add(newOp());
        batcher.add(newOp());
        batcher.add(newOp());
        executor.runAll();

        verify(batch, times(3)).add(any());
        verify(batch).send();
    }

    @Test
    void sealBatchWhenFull() {
        var batch2 = mock(Batch.class);
        when(batchFactory.getBatch(shardId)).thenReturn(batch, batch2);
        when(batch.canAdd(any())).thenReturn(true);
        when(batch.size()).thenReturn(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        when(batch2.canAdd(any())).thenReturn(true);
        when(batch2.size()).thenReturn(1);

        // maxRequestsPerBatch is 10: the first batch is sent when it fills up, the eleventh
        // operation goes to a second batch, flushed when the queue is empty
        for (int i = 0; i < 11; i++) {
            batcher.add(newOp());
        }
        executor.runAll();

        var order = inOrder(batch, batch2);
        verify(batch, times(10)).add(any());
        order.verify(batch).send();
        verify(batch2).add(any());
        order.verify(batch2).send();
    }

    @Test
    void sealBatchWhenOperationDoesNotFit() {
        var batch2 = mock(Batch.class);
        when(batchFactory.getBatch(shardId)).thenReturn(batch, batch2);
        when(batch.canAdd(any())).thenReturn(true, true, false);
        when(batch.size()).thenReturn(1, 2);
        when(batch2.size()).thenReturn(1);

        var op3 = newOp();
        batcher.add(newOp());
        batcher.add(newOp());
        batcher.add(op3);
        executor.runAll();

        // The third operation does not fit: the first batch is sent and the operation starts a
        // new batch
        var order = inOrder(batch, batch2);
        verify(batch, times(2)).add(any());
        order.verify(batch).send();
        verify(batch2).add(op3);
        order.verify(batch2).send();
    }

    @Test
    void failedOperationDoesNotBreakTheBatcher() {
        var op1 = newOp();
        var op2 = newOp();
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.canAdd(any())).thenReturn(true);
        doThrow(new RuntimeException("add failed")).when(batch).add(op1);
        when(batch.size()).thenReturn(1);

        batcher.add(op1);
        batcher.add(op2);
        executor.runAll();

        assertThat(op1.callback()).isCompletedExceptionally();
        verify(batch).add(op2);
        verify(batch).send();
    }

    @Test
    void closeFailsQueuedOperations() {
        var op = newOp();
        batcher.add(op);

        // The batching task has not run yet
        batcher.close();
        executor.runAll();

        assertThat(op.callback()).isCompletedExceptionally();
        verify(batchFactory, never()).getBatch(shardId);

        // Operations submitted after close fail immediately
        var late = newOp();
        batcher.add(late);
        assertThat(late.callback()).isCompletedExceptionally();
    }
}
