/*
 * Copyright © 2022-2025 StreamNative Inc.
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

import static io.oxia.client.api.options.PutOption.IfVersionIdEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.empty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.stub.StreamObserver;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.RangeScanConsumer;
import io.oxia.client.api.Version;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.batch.BatchManager;
import io.oxia.client.batch.Batcher;
import io.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.oxia.client.grpc.OxiaStub;
import io.oxia.client.grpc.OxiaStubManager;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.notify.NotificationManager;
import io.oxia.client.session.SessionManager;
import io.oxia.client.shard.ShardManager;
import io.oxia.proto.ListRequest;
import io.oxia.proto.ListResponse;
import io.oxia.proto.OxiaClientGrpc;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AsyncOxiaClientImplTest {
    @Mock OxiaStubManager stubManager;
    @Mock ShardManager shardManager;
    @Mock NotificationManager notificationManager;
    @Mock BatchManager readBatchManager;
    @Mock BatchManager writeBatchManager;
    @Mock SessionManager sessionManager;
    @Mock Batcher batcher;

    AsyncOxiaClientImpl client;

    private final Duration requestTimeout = Duration.ofSeconds(1);

    @BeforeEach
    void setUp() {
        client =
                new AsyncOxiaClientImpl(
                        "client-identity",
                        Executors.newSingleThreadScheduledExecutor(),
                        InstrumentProvider.NOOP,
                        stubManager,
                        shardManager,
                        notificationManager,
                        readBatchManager,
                        writeBatchManager,
                        sessionManager,
                        requestTimeout);
    }

    @AfterEach
    void cleanup() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void put() {
        var opCaptor = ArgumentCaptor.forClass(PutOperation.class);
        var shardId = 1L;
        var key = "key";
        var value = "hello".getBytes(UTF_8);
        when(shardManager.getShardForKey(key)).thenReturn(shardId);
        when(writeBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doNothing().when(batcher).add(opCaptor.capture());
        var result = client.put(key, value);
        assertThat(result).isNotCompleted();
        assertThat(opCaptor.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.key()).isEqualTo(key);
                            assertThat(o.expectedVersionId()).isEmpty();
                            assertThat(o.value()).isEqualTo(value);
                            var putResult = new PutResult(key, new Version(1, 2, 3, 4, empty(), empty()));
                            o.callback().complete(putResult);
                        });
    }

    @Test
    void putWithTimeout() {
        var opCaptor = ArgumentCaptor.forClass(PutOperation.class);
        var shardId = 1L;
        var key = "key";
        var value = "hello".getBytes(UTF_8);
        when(shardManager.getShardForKey(key)).thenReturn(shardId);
        when(writeBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doNothing().when(batcher).add(opCaptor.capture());
        var result = client.put(key, value);
        try {
            result.join();
            fail("unexpected");
        } catch (Throwable ex) {
            assertThat(ex).isInstanceOf(CompletionException.class);
            assertThat(ex.getCause()).isInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void putFails() {
        var opCaptor = ArgumentCaptor.forClass(PutOperation.class);
        var shardId = 1L;
        var key = "key";
        var value = "hello".getBytes(UTF_8);
        var throwable = new RuntimeException();
        when(shardManager.getShardForKey(key)).thenReturn(shardId);
        when(writeBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doThrow(throwable).when(batcher).add(opCaptor.capture());
        var result = client.put(key, value);
        assertThat(result).isCompletedExceptionally();
    }

    @Test
    void putClosed() throws Exception {
        var key = "key";
        var value = "hello".getBytes(UTF_8);
        client.close();
        assertThat(client.put(key, value)).isCompletedExceptionally();
    }

    @Test
    void putNullKey() throws Exception {
        var value = "hello".getBytes(UTF_8);
        assertThat(client.put(null, value)).isCompletedExceptionally();
    }

    @Test
    void putNullValue() throws Exception {
        var key = "key";
        assertThat(client.put(key, null)).isCompletedExceptionally();
    }

    @Test
    void putExpectedVersion() {
        var opCaptor = ArgumentCaptor.forClass(PutOperation.class);
        var shardId = 1L;
        var key = "key";
        var expectedVersionId = 2L;
        var value = "hello".getBytes(UTF_8);
        when(shardManager.getShardForKey(key)).thenReturn(shardId);
        when(writeBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doNothing().when(batcher).add(opCaptor.capture());
        var result = client.put(key, value, Set.of(IfVersionIdEquals(expectedVersionId)));
        assertThat(result).isNotCompleted();
        assertThat(opCaptor.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.key()).isEqualTo(key);
                            assertThat(o.expectedVersionId()).hasValue(expectedVersionId);
                            assertThat(o.value()).isEqualTo(value);
                        });
    }

    @Test
    void putInvalidOptions() {
        var key = "key";
        var value = "hello".getBytes(UTF_8);
        var result = client.put(key, value, Set.of(IfVersionIdEquals(1L), IfVersionIdEquals(2L)));
        assertThat(result).isCompletedExceptionally();
    }

    @Test
    void delete() {
        var opCaptor = ArgumentCaptor.forClass(DeleteOperation.class);
        var shardId = 1L;
        var key = "key";
        when(shardManager.getShardForKey(key)).thenReturn(shardId);
        when(writeBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doNothing().when(batcher).add(opCaptor.capture());
        var result = client.delete(key);
        assertThat(result).isNotCompleted();
        assertThat(opCaptor.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.key()).isEqualTo(key);
                            assertThat(o.expectedVersionId()).isEmpty();
                            o.callback().complete(true);
                        });
    }

    @Test
    void deleteFails() {
        var opCaptor = ArgumentCaptor.forClass(DeleteOperation.class);
        var shardId = 1L;
        var key = "key";
        var throwable = new RuntimeException();
        when(shardManager.getShardForKey(key)).thenReturn(shardId);
        when(writeBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doThrow(throwable).when(batcher).add(opCaptor.capture());
        var result = client.delete(key);
        assertThat(result).isNotCompleted();
    }

    @Test
    void deleteWithTimeout() {
        var shardId = 1L;
        var key = "key";
        when(shardManager.getShardForKey(key)).thenReturn(shardId);
        when(writeBatchManager.getBatcher(shardId)).thenReturn(batcher);
        var result = client.delete(key);
        try {
            result.join();
            fail("unexpected");
        } catch (Throwable ex) {
            assertThat(ex).isInstanceOf(CompletionException.class);
            assertThat(ex.getCause()).isInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void deleteClosed() throws Exception {
        client.close();
        var key = "key";
        assertThat(client.delete(key)).isCompletedExceptionally();
    }

    @Test
    void deleteNullKey() throws Exception {
        assertThat(client.delete(null)).isCompletedExceptionally();
    }

    @Test
    void deleteExpectedVersion() {
        var opCaptor = ArgumentCaptor.forClass(DeleteOperation.class);
        var shardId = 1L;
        var key = "key";
        var expectedVersionId = 2L;
        when(shardManager.getShardForKey(key)).thenReturn(shardId);
        when(writeBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doNothing().when(batcher).add(opCaptor.capture());
        var result = client.delete(key, Set.of(DeleteOption.IfVersionIdEquals(expectedVersionId)));
        assertThat(result).isNotCompleted();
        assertThat(opCaptor.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.key()).isEqualTo(key);
                            assertThat(o.expectedVersionId()).hasValue(expectedVersionId);
                        });
    }

    @Test
    void deleteInvalidOptions() {
        var key = "key";
        var result =
                client.delete(
                        key, Set.of(DeleteOption.IfVersionIdEquals(1L), DeleteOption.IfVersionIdEquals(2L)));
        assertThat(result).isCompletedExceptionally();
    }

    @Test
    void deleteRange() {
        var batcher1 = mock(Batcher.class);
        var batcher2 = mock(Batcher.class);
        var batcher3 = mock(Batcher.class);
        var opCaptor1 = ArgumentCaptor.forClass(DeleteRangeOperation.class);
        var opCaptor2 = ArgumentCaptor.forClass(DeleteRangeOperation.class);
        var opCaptor3 = ArgumentCaptor.forClass(DeleteRangeOperation.class);
        var startInclusive = "a-startInclusive";
        var endExclusive = "z-endExclusive";
        when(shardManager.allShardIds()).thenReturn(Set.of(1L, 2L, 3L));
        when(writeBatchManager.getBatcher(1L)).thenReturn(batcher1);
        when(writeBatchManager.getBatcher(2L)).thenReturn(batcher2);
        when(writeBatchManager.getBatcher(3L)).thenReturn(batcher3);
        doNothing().when(batcher1).add(opCaptor1.capture());
        doNothing().when(batcher2).add(opCaptor2.capture());
        doNothing().when(batcher3).add(opCaptor3.capture());
        var result = client.deleteRange(startInclusive, endExclusive);
        assertThat(result).isNotCompleted();

        assertThat(opCaptor1.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.startKeyInclusive()).isEqualTo(startInclusive);
                            assertThat(o.endKeyExclusive()).isEqualTo(endExclusive);
                            assertThat(o.callback()).isNotCompleted();
                        });

        assertThat(opCaptor2.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.startKeyInclusive()).isEqualTo(startInclusive);
                            assertThat(o.endKeyExclusive()).isEqualTo(endExclusive);
                            assertThat(o.callback()).isNotCompleted();
                        });

        assertThat(opCaptor3.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.startKeyInclusive()).isEqualTo(startInclusive);
                            assertThat(o.endKeyExclusive()).isEqualTo(endExclusive);
                            assertThat(o.callback()).isNotCompleted();
                        });

        opCaptor1.getValue().callback().complete(null);
        opCaptor2.getValue().callback().complete(null);
        opCaptor3.getValue().callback().complete(null);
        assertThat(result).isCompleted();
    }

    @Test
    void deleteRangeWithTimeout() {
        var batcher1 = mock(Batcher.class);
        var batcher2 = mock(Batcher.class);
        var batcher3 = mock(Batcher.class);
        var opCaptor1 = ArgumentCaptor.forClass(DeleteRangeOperation.class);
        var opCaptor2 = ArgumentCaptor.forClass(DeleteRangeOperation.class);
        var opCaptor3 = ArgumentCaptor.forClass(DeleteRangeOperation.class);
        var startInclusive = "a-startInclusive";
        var endExclusive = "z-endExclusive";
        when(shardManager.allShardIds()).thenReturn(Set.of(1L, 2L, 3L));
        when(writeBatchManager.getBatcher(1L)).thenReturn(batcher1);
        when(writeBatchManager.getBatcher(2L)).thenReturn(batcher2);
        when(writeBatchManager.getBatcher(3L)).thenReturn(batcher3);
        doNothing().when(batcher1).add(opCaptor1.capture());
        doNothing().when(batcher2).add(opCaptor2.capture());
        doNothing().when(batcher3).add(opCaptor3.capture());
        var result = client.deleteRange(startInclusive, endExclusive);
        assertThat(result).isNotCompleted();

        assertThat(opCaptor1.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.startKeyInclusive()).isEqualTo(startInclusive);
                            assertThat(o.endKeyExclusive()).isEqualTo(endExclusive);
                            assertThat(o.callback()).isNotCompleted();
                        });

        assertThat(opCaptor2.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.startKeyInclusive()).isEqualTo(startInclusive);
                            assertThat(o.endKeyExclusive()).isEqualTo(endExclusive);
                            assertThat(o.callback()).isNotCompleted();
                        });

        assertThat(opCaptor3.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.startKeyInclusive()).isEqualTo(startInclusive);
                            assertThat(o.endKeyExclusive()).isEqualTo(endExclusive);
                            assertThat(o.callback()).isNotCompleted();
                        });
        try {
            result.join();
            fail("unexpected");
        } catch (Throwable ex) {
            assertThat(ex).isInstanceOf(CompletionException.class);
            assertThat(ex.getCause()).isInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void deleteRangeClosed() throws Exception {
        client.close();
        var startInclusive = "a-startInclusive";
        var endExclusive = "z-endExclusive";
        assertThat(client.deleteRange(startInclusive, endExclusive)).isCompletedExceptionally();
    }

    @Test
    void deleteRangeNullStart() throws Exception {
        var endExclusive = "z-endExclusive";
        assertThat(client.deleteRange(null, endExclusive)).isCompletedExceptionally();
    }

    @Test
    void deleteRangeEnd() throws Exception {
        var startInclusive = "a-startInclusive";
        assertThat(client.deleteRange(startInclusive, null)).isCompletedExceptionally();
    }

    @Test
    void get() {
        var opCaptor = ArgumentCaptor.forClass(GetOperation.class);
        var shardId = 1L;
        var key = "key";
        when(shardManager.getShardForKey(key)).thenReturn(shardId);
        when(readBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doNothing().when(batcher).add(opCaptor.capture());
        var result = client.get(key);
        assertThat(result).isNotCompleted();
        assertThat(opCaptor.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.key()).isEqualTo(key);
                            var getResult =
                                    new GetResult(key, new byte[1], new Version(1, 2, 3, 4, empty(), empty()));
                            o.callback().complete(getResult);
                        });
    }

    @Test
    void getFails() {
        var opCaptor = ArgumentCaptor.forClass(GetOperation.class);
        var shardId = 1L;
        var key = "key";
        var throwable = new RuntimeException();
        when(shardManager.getShardForKey(key)).thenReturn(shardId);
        when(readBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doThrow(throwable).when(batcher).add(opCaptor.capture());
        var result = client.get(key);
        assertThat(result).isCompletedExceptionally();
    }

    @Test
    void getWithTimeout() {
        var shardId = 1L;
        var key = "key";
        when(shardManager.getShardForKey(key)).thenReturn(shardId);
        when(readBatchManager.getBatcher(shardId)).thenReturn(batcher);
        var result = client.get(key);
        try {
            result.join();
            fail("unexpected");
        } catch (Throwable ex) {
            assertThat(ex).isInstanceOf(CompletionException.class);
            assertThat(ex.getCause()).isInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void getClosed() throws Exception {
        client.close();
        var key = "key";
        assertThat(client.get(key)).isCompletedExceptionally();
    }

    @Test
    void getNullKey() throws Exception {
        assertThat(client.get(null)).isCompletedExceptionally();
    }

    @Test
    void list(@Mock OxiaStub stub0, @Mock OxiaStub stub1) {

        when(shardManager.allShardIds()).thenReturn(Set.of(0L, 1L));
        setupListStub(0L, "leader0", stub0);
        setupListStub(1L, "leader1", stub1);

        List<String> list = client.list("a", "e").join();

        assertThat(list)
                .containsExactlyInAnyOrder("0-a", "0-b", "0-c", "0-d", "1-a", "1-b", "1-c", "1-d");
    }

    @Test
    void listWithTimeout(@Mock OxiaStub stub0, @Mock OxiaStub stub1) {
        when(shardManager.allShardIds()).thenReturn(Set.of(0L, 1L));
        setupTimeoutStub(0L, "leader0", stub0);
        setupTimeoutStub(1L, "leader1", stub1);
        final var result = client.list("a", "e");
        try {
            result.join();
            fail("unexpected");
        } catch (Throwable ex) {
            assertThat(ex).isInstanceOf(CompletionException.class);
            assertThat(ex.getCause()).isInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void listClosed() throws Exception {

        client.close();
        assertThat(client.list("a", "e")).isCompletedExceptionally();
    }

    @Test
    void listNullStart() throws Exception {

        assertThat(client.list(null, "e")).isCompletedExceptionally();
    }

    @Test
    void listNullEnd() throws Exception {

        assertThat(client.list("a", null)).isCompletedExceptionally();
    }

    private void setupTimeoutStub(long shardId, String leader, OxiaStub stub) {
        when(shardManager.leader(shardId)).thenReturn(leader);
        when(stubManager.getStub(leader)).thenReturn(stub);

        var async = mock(OxiaClientGrpc.OxiaClientStub.class);
        when(stub.async()).thenReturn(async);
        doNothing().when(async).list(any(ListRequest.class), any(StreamObserver.class));
    }

    private void setupListStub(long shardId, String leader, OxiaStub stub) {
        when(shardManager.leader(shardId)).thenReturn(leader);
        when(stubManager.getStub(leader)).thenReturn(stub);

        var async = mock(OxiaClientGrpc.OxiaClientStub.class);
        when(stub.async()).thenReturn(async);

        doAnswer(
                        i -> {
                            var so = (StreamObserver<ListResponse>) i.getArgument(1);
                            so.onNext(listResponse(shardId, "a", "b"));
                            so.onNext(listResponse(shardId, "c", "d"));
                            so.onCompleted();
                            return null;
                        })
                .when(async)
                .list(any(ListRequest.class), any(StreamObserver.class));
    }

    private ListResponse listResponse(long shardId, String first, String second) {
        return ListResponse.newBuilder()
                .addAllKeys(List.of(shardId + "-" + first, shardId + "-" + second))
                .build();
    }

    @Test
    void close() throws Exception {
        client.close();
        var inOrder =
                inOrder(
                        readBatchManager, writeBatchManager, notificationManager, shardManager, stubManager);
        inOrder.verify(readBatchManager).close();
        inOrder.verify(writeBatchManager).close();
        inOrder.verify(notificationManager).close();
        inOrder.verify(shardManager).close();
        inOrder.verify(stubManager).close();
        client = null;
    }

    @Test
    void testShardShardRangeScanConsumer() {
        final int shards = 5;
        final List<GetResult> results = new ArrayList<>();
        final AtomicInteger onErrorCount = new AtomicInteger(0);
        final AtomicInteger onCompletedCount = new AtomicInteger(0);
        final Supplier<RangeScanConsumer> newShardRangeScanConsumer =
                () ->
                        new AsyncOxiaClientImpl.SharedRangeScanConsumer(
                                5,
                                new RangeScanConsumer() {
                                    @Override
                                    public void onNext(GetResult result) {
                                        results.add(result);
                                    }

                                    @Override
                                    public void onError(Throwable throwable) {
                                        onErrorCount.incrementAndGet();
                                    }

                                    @Override
                                    public void onCompleted() {
                                        onCompletedCount.incrementAndGet();
                                    }
                                });
        final var tasks = new ArrayList<ForkJoinTask<?>>();

        // (1) complete ok
        final var shardRangeScanConsumer1 = newShardRangeScanConsumer.get();
        for (int i = 0; i < shards; i++) {
            final int fi = i;
            final ForkJoinTask<?> task =
                    ForkJoinPool.commonPool()
                            .submit(
                                    () -> {
                                        shardRangeScanConsumer1.onNext(
                                                new GetResult(
                                                        "shard-" + fi + "-0",
                                                        new byte[10],
                                                        new Version(1, 2, 3, 4, empty(), empty())));
                                        shardRangeScanConsumer1.onNext(
                                                new GetResult(
                                                        "shard-" + fi + "-1",
                                                        new byte[10],
                                                        new Version(1, 2, 3, 4, empty(), empty())));
                                        shardRangeScanConsumer1.onCompleted();
                                    });
            tasks.add(task);
        }
        tasks.forEach(ForkJoinTask::join);
        var keys = results.stream().map(GetResult::key).toList();
        for (int i = 0; i < shards; i++) {
            Assertions.assertTrue(keys.contains("shard-" + i + "-0"));
            Assertions.assertTrue(keys.contains("shard-" + i + "-1"));
        }
        Assertions.assertEquals(0, onErrorCount.get());
        Assertions.assertEquals(1, onCompletedCount.get());

        tasks.clear();
        onErrorCount.set(0);
        onCompletedCount.set(0);
        results.clear();

        // (2) complete partial exception
        final var shardRangeScanConsumer2 = newShardRangeScanConsumer.get();
        for (int i = 0; i < shards; i++) {
            final int fi = i;
            final ForkJoinTask<?> task =
                    ForkJoinPool.commonPool()
                            .submit(
                                    () -> {
                                        if (fi % 2 == 0) {
                                            shardRangeScanConsumer2.onError(new IllegalStateException());
                                            return;
                                        }
                                        shardRangeScanConsumer2.onNext(
                                                new GetResult(
                                                        "shard-" + fi + "-0",
                                                        new byte[10],
                                                        new Version(1, 2, 3, 4, empty(), empty())));
                                        shardRangeScanConsumer2.onNext(
                                                new GetResult(
                                                        "shard-" + fi + "-1",
                                                        new byte[10],
                                                        new Version(1, 2, 3, 4, empty(), empty())));
                                        shardRangeScanConsumer2.onCompleted();
                                    });
            tasks.add(task);
        }
        tasks.forEach(ForkJoinTask::join);

        Assertions.assertEquals(1, onErrorCount.get());
        Assertions.assertEquals(0, onCompletedCount.get());

        tasks.clear();
        onErrorCount.set(0);
        onCompletedCount.set(0);
        results.clear();

        // (3) complete all exception
        final var shardRangeScanConsumer3 = newShardRangeScanConsumer.get();
        for (int i = 0; i < shards; i++) {
            final ForkJoinTask<?> task =
                    ForkJoinPool.commonPool()
                            .submit(() -> shardRangeScanConsumer3.onError(new IllegalStateException()));
            tasks.add(task);
        }
        tasks.forEach(ForkJoinTask::join);
        Assertions.assertEquals(1, onErrorCount.get());
        Assertions.assertEquals(0, onCompletedCount.get());
        Assertions.assertEquals(0, results.size());
    }
}
