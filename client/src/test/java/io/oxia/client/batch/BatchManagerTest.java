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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.oxia.client.ClientConfig;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.GetResult;
import io.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.oxia.client.grpc.OxiaStatusCode;
import io.oxia.client.grpc.OxiaStatusException;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.options.GetOptions;
import io.oxia.client.session.SessionManager;
import io.oxia.proto.KeyComparisonType;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BatchManagerTest {

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
                    2,
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
    BatchManager manager;

    @BeforeEach
    void setup() {
        pool = new BatcherPool("test-batcher", config.batchingThreads());
        manager = new BatchManager(batchFactory, pool, true);
    }

    @AfterEach
    void teardown() throws Exception {
        manager.close();
    }

    private Operation<?> newOp(long shardId) {
        return new GetOperation(
                manager,
                shardId,
                new CompletableFuture<GetResult>(),
                "key",
                new GetOptions(null, true, KeyComparisonType.EQUAL, null));
    }

    @Test
    void routesOperationsByShard() {
        when(batchFactory.getConfig()).thenReturn(config);
        when(batchFactory.getDispatchWindow(anyLong()))
                .thenReturn(new DispatchWindow(Integer.MAX_VALUE));
        when(batchFactory.getBatch(anyLong())).thenReturn(batch);
        when(batch.canAdd(any())).thenReturn(true);
        when(batch.size()).thenReturn(1);

        // More shards than batching threads: every operation is still processed
        for (long shardId = 0; shardId < 4; shardId++) {
            manager.add(newOp(shardId));
        }

        await()
                .untilAsserted(
                        () -> {
                            for (long shardId = 0; shardId < 4; shardId++) {
                                verify(batchFactory).getBatch(shardId);
                            }
                        });
    }

    @Test
    void addAfterCloseThrows() throws Exception {
        manager.close();
        assertThatThrownBy(() -> manager.add(newOp(1L)))
                .isInstanceOfSatisfying(
                        OxiaStatusException.class,
                        e -> assertThat(e.getStatusCode()).isEqualTo(OxiaStatusCode.ALREADY_CLOSED));
    }

    @Test
    void closeFailsWindowHeldBatches() throws Exception {
        manager.close();
        verify(batchFactory).failWindows(any(OxiaStatusException.class));
    }

    @Test
    void rejectsNullArguments() {
        var rpc = mock(RpcProvider.class);
        var sessions = mock(SessionManager.class);
        var metrics = InstrumentProvider.NOOP;
        List.<ThrowingCallable>of(
                        () -> new BatchManager(null, pool, false),
                        () -> new BatchManager(batchFactory, null, false),
                        () -> manager.add(null),
                        () -> BatchManager.newReadBatchManager(null, rpc, metrics, pool, false),
                        () -> BatchManager.newReadBatchManager(config, null, metrics, pool, false),
                        () -> BatchManager.newReadBatchManager(config, rpc, null, pool, false),
                        () -> BatchManager.newReadBatchManager(config, rpc, metrics, null, false),
                        () -> BatchManager.newWriteBatchManager(null, rpc, sessions, metrics, pool, false),
                        () -> BatchManager.newWriteBatchManager(config, null, sessions, metrics, pool, false),
                        () -> BatchManager.newWriteBatchManager(config, rpc, null, metrics, pool, false),
                        () -> BatchManager.newWriteBatchManager(config, rpc, sessions, null, pool, false),
                        () -> BatchManager.newWriteBatchManager(config, rpc, sessions, metrics, null, false))
                .forEach(call -> assertThatThrownBy(call).isInstanceOf(NullPointerException.class));
    }

    @Test
    void createsReadAndWriteManagers() {
        var rpc = mock(RpcProvider.class);
        assertThat(
                        BatchManager.newReadBatchManager(config, rpc, InstrumentProvider.NOOP, pool, false)
                                .factory())
                .isInstanceOf(ReadBatchFactory.class);
        assertThat(
                        BatchManager.newWriteBatchManager(
                                        config, rpc, mock(SessionManager.class), InstrumentProvider.NOOP, pool, false)
                                .factory())
                .isInstanceOf(WriteBatchFactory.class);
    }

    @Test
    void closingAClientOfASharedPoolKeepsThePoolRunning() throws Exception {
        when(batchFactory.getConfig()).thenReturn(config);
        when(batchFactory.getDispatchWindow(1L)).thenReturn(new DispatchWindow(Integer.MAX_VALUE));
        when(batchFactory.getBatch(1L)).thenReturn(batch);
        when(batch.canAdd(any())).thenReturn(true);
        when(batch.size()).thenReturn(1);
        var otherFactory = mock(BatchFactory.class);
        var other = new BatchManager(otherFactory, pool, false);

        other.close();
        other.close();

        verify(otherFactory, times(1)).failWindows(any(OxiaStatusException.class));
        manager.add(newOp(1L));
        await().untilAsserted(() -> verify(batch).send());
    }
}
