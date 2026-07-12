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

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
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
class BatcherPoolTest {

    // Two factories stand in for two different client instances sharing one pool.
    @Mock BatchFactory factoryA;
    @Mock BatchFactory factoryB;
    @Mock Batch batchA;
    @Mock Batch batchB;

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

    BatcherPool pool;

    @BeforeEach
    void setup() {
        pool = new BatcherPool("test-shared-batcher", 2);
    }

    @AfterEach
    void teardown() {
        pool.close();
    }

    private static Operation<?> newOp(long shardId) {
        return new GetOperation(
                shardId,
                new CompletableFuture<GetResult>(),
                "key",
                new GetOptions(null, true, KeyComparisonType.EQUAL, null));
    }

    @Test
    void oneThreadPoolServesMultipleClients() {
        when(factoryA.getConfig()).thenReturn(config);
        when(factoryB.getConfig()).thenReturn(config);
        when(factoryA.getBatch(1L)).thenReturn(batchA);
        when(factoryB.getBatch(1L)).thenReturn(batchB);
        when(batchA.canAdd(any())).thenReturn(true);
        when(batchA.size()).thenReturn(1);
        when(batchB.canAdd(any())).thenReturn(true);
        when(batchB.size()).thenReturn(1);

        // Both operations target shard 1, so they land on the same batcher thread, but each is
        // batched through its own client's factory.
        pool.route(factoryA, newOp(1L));
        pool.route(factoryB, newOp(1L));

        await()
                .untilAsserted(
                        () -> {
                            verify(factoryA).getBatch(1L);
                            verify(batchA).send();
                            verify(factoryB).getBatch(1L);
                            verify(batchB).send();
                        });
    }

    @Test
    void closingOneClientKeepsThePoolRunningForOthers() {
        when(factoryB.getConfig()).thenReturn(config);
        when(factoryB.getBatch(1L)).thenReturn(batchB);
        when(batchB.canAdd(any())).thenReturn(true);
        when(batchB.size()).thenReturn(1);

        // A closing client only detaches itself; the shared pool stays up.
        pool.closeFactory(factoryA).join();

        pool.route(factoryB, newOp(1L));
        await()
                .untilAsserted(
                        () -> {
                            verify(factoryB).getBatch(1L);
                            verify(batchB).send();
                        });
    }
}
