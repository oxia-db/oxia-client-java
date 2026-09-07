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
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.shard.ShardManager;
import io.oxia.proto.GetSequenceUpdatesRequest;
import io.oxia.proto.GetSequenceUpdatesResponse;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SequenceUpdatesTest {
    @Test
    void backsOffUnexpectedFailuresAndResetsAfterSuccessAndDeadline() throws Exception {
        var rpcProvider = mock(RpcProvider.class);
        var shardManager = mock(ShardManager.class);
        when(shardManager.getShardForKey(any())).thenReturn(0L);
        var observerRef = new AtomicReference<CancelableStreamObserver<GetSequenceUpdatesResponse>>();
        doAnswer(
                        invocation -> {
                            observerRef.set(invocation.getArgument(1));
                            return null;
                        })
                .when(rpcProvider)
                .getSequenceUpdates(any(GetSequenceUpdatesRequest.class), any());

        var scheduledTasks = new ArrayList<Runnable>();
        var retryDelays = new ArrayList<Long>();
        var executor = mock(ScheduledExecutorService.class);
        when(executor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenAnswer(
                        invocation -> {
                            scheduledTasks.add(invocation.getArgument(0));
                            retryDelays.add(invocation.getArgument(1));
                            return null;
                        });

        try (var updates =
                new SequenceUpdates(
                        "key",
                        "partition",
                        ignored -> {},
                        rpcProvider,
                        shardManager,
                        InstrumentProvider.NOOP,
                        ignored -> false,
                        executor)) {
            observerRef.get().onError(Status.UNAVAILABLE.asRuntimeException());
            assertThat(retryDelays.get(0)).isBetween(100L, 119L);

            scheduledTasks.remove(0).run();
            observerRef.get().onError(Status.UNAVAILABLE.asRuntimeException());
            assertThat(retryDelays.get(1)).isBetween(200L, 239L);

            scheduledTasks.remove(0).run();
            observerRef.get().onNext(new GetSequenceUpdatesResponse());
            observerRef.get().onError(Status.UNAVAILABLE.asRuntimeException());
            assertThat(retryDelays.get(2)).isBetween(100L, 119L);

            scheduledTasks.remove(0).run();
            observerRef.get().onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
            assertThat(retryDelays.get(3)).isZero();

            scheduledTasks.remove(0).run();
            observerRef.get().onError(Status.UNAVAILABLE.asRuntimeException());
            assertThat(retryDelays.get(4)).isBetween(100L, 119L);
        }
    }

    @Test
    void deliversAllSequenceUpdatesAcrossSubscriptionRenewal() throws Exception {
        var rpcProvider = mock(RpcProvider.class);
        var shardManager = mock(ShardManager.class);
        when(shardManager.getShardForKey(any())).thenReturn(0L);
        var observerRef = new AtomicReference<CancelableStreamObserver<GetSequenceUpdatesResponse>>();
        doAnswer(
                        invocation -> {
                            observerRef.set(invocation.getArgument(1));
                            return null;
                        })
                .when(rpcProvider)
                .getSequenceUpdates(any(GetSequenceUpdatesRequest.class), any());

        var delivered = new ArrayList<String>();
        var executor = Executors.newSingleThreadScheduledExecutor();
        try {
            try (var updates =
                    new SequenceUpdates(
                            "key",
                            "partition",
                            delivered::add,
                            rpcProvider,
                            shardManager,
                            InstrumentProvider.NOOP,
                            ignored -> false,
                            executor)) {
                var firstObserver = observerRef.get();
                var first =
                        new GetSequenceUpdatesResponse().setHighestSequenceKey("key-00000000000000000001");
                firstObserver.onNext(first);

                firstObserver.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
                await().untilAsserted(() -> assertThat(observerRef.get()).isNotSameAs(firstObserver));
                var renewedObserver = observerRef.get();
                renewedObserver.onNext(first);
                renewedObserver.onNext(
                        new GetSequenceUpdatesResponse().setHighestSequenceKey("key-00000000000000000000"));
                renewedObserver.onNext(first);
                renewedObserver.onNext(
                        new GetSequenceUpdatesResponse().setHighestSequenceKey("key-00000000000000000002"));

                assertThat(delivered)
                        .containsExactly(
                                "key-00000000000000000001",
                                "key-00000000000000000001",
                                "key-00000000000000000000",
                                "key-00000000000000000001",
                                "key-00000000000000000002");

                executor.shutdownNow();
                assertThatNoException().isThrownBy(renewedObserver::onCompleted);
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
