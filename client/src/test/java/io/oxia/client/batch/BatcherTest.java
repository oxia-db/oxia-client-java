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

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.oxia.client.ClientConfig;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.oxia.client.options.GetOptions;
import io.oxia.client.util.BatchedArrayBlockingQueue;
import io.oxia.proto.KeyComparisonType;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
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
    long shardId = 1L;
    ClientConfig config =
            new ClientConfig(
                    "address",
                    Duration.ofMillis(100),
                    Duration.ofMillis(1000),
                    10,
                    1024 * 1024,
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

    BatchedArrayBlockingQueue<Operation<?>> queue;
    Batcher batcher;

    @BeforeEach
    void mocking() {
        queue = spy(new BatchedArrayBlockingQueue<>(100));
        batcher = new Batcher(config, shardId, batchFactory, queue);
    }

    @AfterEach
    void teardown() {
        batcher.close();
    }

    @Test
    void createBatchAndAdd() throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op =
                new GetOperation(
                        callback, "key", new GetOptions(null, true, KeyComparisonType.EQUAL, null));
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(1);
        when(batch.canAdd(any())).thenReturn(true);
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).add(op));
        verify(batch, never()).send();
    }

    @Test
    void sendBatchOnFull() throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op =
                new GetOperation(
                        callback, "key", new GetOptions(null, true, KeyComparisonType.EQUAL, null));
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(config.maxRequestsPerBatch());
        when(batch.canAdd(any())).thenReturn(true);
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).send());
    }

    @Test
    void addWhenNextDoesNotFit() {
        var callback = new CompletableFuture<PutResult>();
        Operation<?> op =
                new Operation.WriteOperation.PutOperation(
                        callback,
                        "key",
                        Optional.empty(),
                        Optional.empty(),
                        "value".getBytes(StandardCharsets.UTF_8),
                        OptionalLong.empty(),
                        OptionalLong.empty(),
                        Optional.empty(),
                        Collections.emptyList(),
                        OptionalLong.empty(),
                        OptionalLong.empty());
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(config.maxRequestsPerBatch(), 1);
        when(batch.canAdd(any())).thenReturn(false);
        batcher.add(op);
        await()
                .untilAsserted(
                        () -> {
                            verify(batchFactory, times(2)).getBatch(shardId);
                            verify(batch).add(op);
                        });
    }

    @Test
    void sendBatchOnFullThenNewBatch() throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op =
                new GetOperation(
                        callback, "key", new GetOptions(null, true, KeyComparisonType.EQUAL, null));
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(config.maxRequestsPerBatch(), 1);
        when(batch.canAdd(any())).thenReturn(true);
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).send());
        batcher.add(op);
        await()
                .untilAsserted(
                        () -> {
                            verify(batchFactory, times(2)).getBatch(shardId);
                            verify(batch, times(2)).add(op);
                        });
    }

    @Test
    void sendBatchOnLingerExpiration() throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op =
                new GetOperation(
                        callback, "key", new GetOptions(null, true, KeyComparisonType.EQUAL, null));
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(1);
        when(batch.canAdd(any())).thenReturn(true);
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).add(op));
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).send());
    }

    @Test
    void sendBatchOnLingerExpirationMulti() throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op =
                new GetOperation(
                        callback, "key", new GetOptions(null, true, KeyComparisonType.EQUAL, null));
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(1);
        when(batch.canAdd(any())).thenReturn(true);
        when(batch.getStartTimeNanos()).thenReturn(System.nanoTime());
        batcher.add(op);
        batcher.add(op);
        batcher.add(op);
        await().untilAsserted(() -> verify(batch, times(3)).add(op));
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).send());
    }

    @Test
    void unboundedTakeAtStart() throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op =
                new GetOperation(
                        callback, "key", new GetOptions(null, true, KeyComparisonType.EQUAL, null));

        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(1);
        batcher.add(op);
        var inOrder = inOrder(queue);
        await()
                .untilAsserted(
                        () -> {
                            inOrder.verify(queue, atLeastOnce()).takeAll(any());
                        });
    }
}
