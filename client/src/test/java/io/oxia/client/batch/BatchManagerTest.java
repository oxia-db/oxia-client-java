/*
 * Copyright © 2022-2025 The Oxia Authors
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BatchManagerTest {

    @Mock Function<Long, Batcher> batcherFactory;
    @Mock Batcher batcher;
    long shardId = 1;
    BatchManager manager;

    @BeforeEach
    void mocking() {
        manager = new BatchManager(batcherFactory);
    }

    @Test
    void computeAbsentBatcher() {
        when(batcherFactory.apply(shardId)).thenReturn(batcher);
        var computed = manager.getBatcher(shardId);
        verify(batcherFactory).apply(shardId);
        assertThat(computed).isSameAs(batcher);
    }

    @Test
    void getPrecomputedBatcher() {
        when(batcherFactory.apply(shardId)).thenReturn(batcher);
        var computed = manager.getBatcher(shardId);
        verify(batcherFactory).apply(shardId);
        var cached = manager.getBatcher(shardId);
        verifyNoMoreInteractions(batcherFactory);
        assertThat(computed).isSameAs(batcher);
        assertThat(cached).isSameAs(batcher);
    }

    @Test
    void getBatcherWhenClosed() throws Exception {
        manager.close();
        assertThatThrownBy(() -> manager.getBatcher(shardId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Batch manager is closed");
    }

    @Test
    void closeClean() throws Exception {
        when(batcherFactory.apply(shardId)).thenReturn(batcher);
        manager.getBatcher(shardId);
        manager.close();
        verify(batcher).close();
    }
}
