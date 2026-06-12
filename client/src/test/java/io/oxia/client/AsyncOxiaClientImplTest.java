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
package io.oxia.client;

import static io.oxia.client.api.options.PutOption.IfVersionIdEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.empty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.RangeScanConsumer;
import io.oxia.client.api.Version;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.batch.BatchManager;
import io.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.notify.NotificationManager;
import io.oxia.client.operation.rangescan.CompositeRangeScanConsumer;
import io.oxia.client.session.SessionManager;
import io.oxia.client.shard.ShardManager;
import io.oxia.proto.ListRequest;
import io.oxia.proto.ListResponse;
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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AsyncOxiaClientImplTest {
    @Mock RpcProvider rpcProvider;
    @Mock ShardManager shardManager;
    @Mock NotificationManager notificationManager;
    @Mock BatchManager readBatchManager;
    @Mock BatchManager writeBatchManager;
    @Mock SessionManager sessionManager;

    AsyncOxiaClientImpl client;

    private final Duration requestTimeout = Duration.ofSeconds(1);
    private static final long maxPendingBytes = 256L * 1024 * 1024;

    private AsyncOxiaClientImpl newClient(long maxPendingBytes) {
        return new AsyncOxiaClientImpl(
                "client-identity",
                Executors.newSingleThreadScheduledExecutor(),
                InstrumentProvider.NOOP,
                rpcProvider,
                shardManager,
                notificationManager,
                readBatchManager,
                writeBatchManager,
                sessionManager,
                requestTimeout,
                maxPendingBytes);
    }

    @BeforeEach
    void setUp() {
        client = newClient(maxPendingBytes);
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
        doNothing().when(writeBatchManager).add(opCaptor.capture());
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
        doNothing().when(writeBatchManager).add(opCaptor.capture());
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
    @Timeout(30)
    void putBlocksWhenPendingBytesLimitIsReached() throws Exception {
        var opCaptor = ArgumentCaptor.forClass(PutOperation.class);
        var shardId = 1L;
        when(shardManager.getShardForKey(any())).thenReturn(shardId);
        doNothing().when(writeBatchManager).add(opCaptor.capture());

        // utf8("a") + 8 value bytes = 9: the first put fits within the 10 bytes limit
        var smallClient = newClient(10);
        try {
            var first = smallClient.put("a", new byte[8]);
            assertThat(first).isNotCompleted();

            var thread = new Thread(() -> smallClient.put("b", new byte[8]));
            thread.start();
            await().until(() -> thread.getState() == Thread.State.WAITING);
            assertThat(opCaptor.getAllValues()).hasSize(1);

            // Completing the first operation frees capacity and unblocks the second put
            opCaptor
                    .getValue()
                    .callback()
                    .complete(new PutResult("a", new Version(1, 2, 3, 4, empty(), empty())));
            thread.join();
            assertThat(opCaptor.getAllValues()).hasSize(2);
        } finally {
            smallClient.close();
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
        doThrow(throwable).when(writeBatchManager).add(opCaptor.capture());
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
        doNothing().when(writeBatchManager).add(opCaptor.capture());
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
        doNothing().when(writeBatchManager).add(opCaptor.capture());
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
        doThrow(throwable).when(writeBatchManager).add(opCaptor.capture());
        var result = client.delete(key);
        assertThat(result).isNotCompleted();
    }

    @Test
    void deleteWithTimeout() {
        var shardId = 1L;
        var key = "key";
        when(shardManager.getShardForKey(key)).thenReturn(shardId);
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
        doNothing().when(writeBatchManager).add(opCaptor.capture());
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
        var opCaptor = ArgumentCaptor.forClass(DeleteRangeOperation.class);
        var startInclusive = "a-startInclusive";
        var endExclusive = "z-endExclusive";
        when(shardManager.allShardIds()).thenReturn(Set.of(1L, 2L, 3L));
        doNothing().when(writeBatchManager).add(opCaptor.capture());
        var result = client.deleteRange(startInclusive, endExclusive);
        assertThat(result).isNotCompleted();

        assertThat(opCaptor.getAllValues()).hasSize(3);
        assertThat(opCaptor.getAllValues())
                .extracting(DeleteRangeOperation::shardId)
                .containsExactlyInAnyOrder(1L, 2L, 3L);
        opCaptor
                .getAllValues()
                .forEach(
                        o -> {
                            assertThat(o.startKeyInclusive()).isEqualTo(startInclusive);
                            assertThat(o.endKeyExclusive()).isEqualTo(endExclusive);
                            assertThat(o.callback()).isNotCompleted();
                        });

        opCaptor.getAllValues().forEach(o -> o.callback().complete(null));
        assertThat(result).isCompleted();
    }

    @Test
    void deleteRangeWithTimeout() {
        var opCaptor = ArgumentCaptor.forClass(DeleteRangeOperation.class);
        var startInclusive = "a-startInclusive";
        var endExclusive = "z-endExclusive";
        when(shardManager.allShardIds()).thenReturn(Set.of(1L, 2L, 3L));
        doNothing().when(writeBatchManager).add(opCaptor.capture());
        var result = client.deleteRange(startInclusive, endExclusive);
        assertThat(result).isNotCompleted();

        assertThat(opCaptor.getAllValues()).hasSize(3);
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
        doNothing().when(readBatchManager).add(opCaptor.capture());
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
        doThrow(throwable).when(readBatchManager).add(opCaptor.capture());
        var result = client.get(key);
        assertThat(result).isCompletedExceptionally();
    }

    @Test
    void getWithTimeout() {
        var shardId = 1L;
        var key = "key";
        when(shardManager.getShardForKey(key)).thenReturn(shardId);
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
    void list() {

        when(shardManager.allShardIds()).thenReturn(Set.of(0L, 1L));
        setupListStub();

        List<String> list = client.list("a", "e").join();

        assertThat(list)
                .containsExactlyInAnyOrder("0-a", "0-b", "0-c", "0-d", "1-a", "1-b", "1-c", "1-d");
    }

    @Test
    void listWithTimeout() {
        when(shardManager.allShardIds()).thenReturn(Set.of(0L, 1L));
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

    private void setupListStub() {
        doAnswer(
                        i -> {
                            var request = (ListRequest) i.getArgument(0);
                            var so = (CancelableStreamObserver<ListResponse>) i.getArgument(1);
                            var shardId = request.getShard();
                            so.onNext(listResponse(shardId, "a", "b"));
                            so.onNext(listResponse(shardId, "c", "d"));
                            so.onCompleted();
                            return null;
                        })
                .when(rpcProvider)
                .list(any(ListRequest.class), any(CancelableStreamObserver.class));
    }

    private ListResponse listResponse(long shardId, String first, String second) {
        var response = new ListResponse();
        response.addKey(shardId + "-" + first);
        response.addKey(shardId + "-" + second);
        return response;
    }

    @Test
    void close() throws Exception {
        client.close();
        var inOrder =
                inOrder(
                        readBatchManager, writeBatchManager, notificationManager, shardManager, rpcProvider);
        inOrder.verify(readBatchManager).close();
        inOrder.verify(writeBatchManager).close();
        inOrder.verify(notificationManager).close();
        inOrder.verify(shardManager).close();
        inOrder.verify(rpcProvider).close();
        client = null;
    }

    @Test
    void testCompositeRangeScanConsumer() {
        final int shards = 5;
        final List<GetResult> results = new ArrayList<>();
        final AtomicInteger onErrorCount = new AtomicInteger(0);
        final AtomicInteger onCompletedCount = new AtomicInteger(0);
        final Supplier<RangeScanConsumer> newShardRangeScanConsumer =
                () ->
                        new CompositeRangeScanConsumer(
                                shards,
                                new RangeScanConsumer() {
                                    @Override
                                    public boolean onNext(GetResult result) {
                                        results.add(result);
                                        return true;
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

    @Test
    void testCompositeRangeScanConsumerEarlyStop() {
        final int shards = 3;
        final int stopAfter = 2;
        final List<GetResult> results = new ArrayList<>();
        final AtomicInteger onErrorCount = new AtomicInteger(0);
        final AtomicInteger onCompletedCount = new AtomicInteger(0);

        final RangeScanConsumer userConsumer =
                new RangeScanConsumer() {
                    @Override
                    public boolean onNext(GetResult result) {
                        results.add(result);
                        return results.size() < stopAfter;
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        onErrorCount.incrementAndGet();
                    }

                    @Override
                    public void onCompleted() {
                        onCompletedCount.incrementAndGet();
                    }
                };

        final var shared = new CompositeRangeScanConsumer(shards, userConsumer);

        // First onNext returns true.
        Assertions.assertTrue(
                shared.onNext(new GetResult("k1", new byte[1], new Version(1, 2, 3, 4, empty(), empty()))));
        // Second onNext returns false (user requested stop).
        Assertions.assertFalse(
                shared.onNext(new GetResult("k2", new byte[1], new Version(1, 2, 3, 4, empty(), empty()))));

        // onCompleted was called exactly once on the user consumer.
        Assertions.assertEquals(1, onCompletedCount.get());
        Assertions.assertEquals(0, onErrorCount.get());
        Assertions.assertEquals(stopAfter, results.size());

        // Subsequent shard onNext calls are dropped (return false).
        Assertions.assertFalse(
                shared.onNext(new GetResult("k3", new byte[1], new Version(1, 2, 3, 4, empty(), empty()))));
        Assertions.assertEquals(stopAfter, results.size());

        // Subsequent per-shard onCompleted calls (e.g., from cancel-induced errors) do not
        // re-trigger user onCompleted.
        shared.onCompleted();
        shared.onCompleted();
        Assertions.assertEquals(1, onCompletedCount.get());
    }
}
