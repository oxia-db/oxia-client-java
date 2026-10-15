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
package io.oxia.client;

import static java.util.Optional.empty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.batch.BatchManager;
import io.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.notify.NotificationManager;
import io.oxia.client.session.SessionManager;
import io.oxia.client.shard.ShardManager;
import io.oxia.proto.KeySorting;
import io.oxia.proto.ListRequest;
import io.oxia.proto.ListResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AsyncOxiaClientKeyOrderTest {
    @Mock RpcProvider rpcProvider;
    @Mock ShardManager shardManager;
    @Mock NotificationManager notificationManager;
    @Mock BatchManager readBatchManager;
    @Mock BatchManager writeBatchManager;
    @Mock SessionManager sessionManager;

    private AsyncOxiaClientImpl client;

    @BeforeEach
    void setUp() {
        client =
                new AsyncOxiaClientImpl(
                        "key-order-test",
                        Executors.newSingleThreadScheduledExecutor(),
                        InstrumentProvider.NOOP,
                        rpcProvider,
                        shardManager,
                        notificationManager,
                        readBatchManager,
                        writeBatchManager,
                        sessionManager,
                        Duration.ofSeconds(3),
                        1024 * 1024,
                        true);
    }

    @AfterEach
    void tearDown() throws Exception {
        client.close();
    }

    static Stream<Arguments> comparisons() {
        return Stream.of(
                Arguments.of(
                        KeySorting.KEY_SORTING_HIERARCHICAL,
                        GetOption.ComparisonFloor,
                        "p0/k06",
                        Arrays.asList("p0/k05", null, null),
                        "p0/k05"),
                Arguments.of(
                        KeySorting.KEY_SORTING_HIERARCHICAL,
                        GetOption.ComparisonLower,
                        "p0/k06",
                        Arrays.asList("p0/k05", null, null),
                        "p0/k05"),
                Arguments.of(
                        KeySorting.KEY_SORTING_HIERARCHICAL,
                        GetOption.ComparisonCeiling,
                        "p0/k06",
                        Arrays.asList(null, "p0/k07", "da//z"),
                        "p0/k07"),
                Arguments.of(
                        KeySorting.KEY_SORTING_HIERARCHICAL,
                        GetOption.ComparisonHigher,
                        "p0/k06",
                        Arrays.asList(null, "p0/k07", "da//z"),
                        "p0/k07"),
                Arguments.of(
                        KeySorting.KEY_SORTING_HIERARCHICAL,
                        GetOption.ComparisonFloor,
                        "xx//a",
                        List.of("p0/k05", "p0/k07", "da//z"),
                        "da//z"),
                Arguments.of(
                        KeySorting.KEY_SORTING_HIERARCHICAL,
                        GetOption.ComparisonLower,
                        "xx//a",
                        List.of("p0/k05", "p0/k07", "da//z"),
                        "da//z"),
                Arguments.of(
                        KeySorting.KEY_SORTING_HIERARCHICAL,
                        GetOption.ComparisonCeiling,
                        "xx//a",
                        Arrays.asList(null, null, null),
                        null),
                Arguments.of(
                        KeySorting.KEY_SORTING_HIERARCHICAL,
                        GetOption.ComparisonHigher,
                        "xx//a",
                        Arrays.asList(null, null, null),
                        null),
                Arguments.of(
                        KeySorting.KEY_SORTING_NATURAL,
                        GetOption.ComparisonFloor,
                        "da10",
                        Arrays.asList("da1", "da//z", null),
                        "da1"),
                Arguments.of(
                        KeySorting.KEY_SORTING_NATURAL,
                        GetOption.ComparisonLower,
                        "da10",
                        Arrays.asList("da1", "da//z", null),
                        "da1"),
                Arguments.of(
                        KeySorting.KEY_SORTING_NATURAL,
                        GetOption.ComparisonCeiling,
                        "da10",
                        Arrays.asList(null, null, "db"),
                        "db"),
                Arguments.of(
                        KeySorting.KEY_SORTING_NATURAL,
                        GetOption.ComparisonHigher,
                        "da10",
                        Arrays.asList(null, null, "db"),
                        "db"));
    }

    @ParameterizedTest
    @MethodSource("comparisons")
    void mergesNearestCandidatesUsingNamespaceOrder(
            KeySorting sorting,
            GetOption comparison,
            String query,
            List<String> candidates,
            String expected) {
        when(shardManager.allShardIds()).thenReturn(Set.of(0L, 1L, 2L));
        when(shardManager.getKeyComparator()).thenReturn(KeyOrder.comparator(sorting));
        doAnswer(
                        invocation -> {
                            GetOperation operation = invocation.getArgument(0);
                            var candidate = candidates.get(Math.toIntExact(operation.shardId()));
                            operation.callback().complete(record(candidate));
                            return null;
                        })
                .when(readBatchManager)
                .add(any(GetOperation.class));

        var result = client.get(query, Set.of(comparison)).join();
        assertThat(result == null ? null : result.key()).isEqualTo(expected);
        verify(shardManager, times(1)).getKeyComparator();
    }

    static Stream<Arguments> listOrders() {
        return Stream.of(
                Arguments.of(KeySorting.KEY_SORTING_HIERARCHICAL, List.of("p0/k05", "p0/k07", "da//z")),
                Arguments.of(KeySorting.KEY_SORTING_NATURAL, List.of("da//z", "p0/k05", "p0/k07")),
                Arguments.of(KeySorting.KEY_SORTING_UNKNOWN, List.of("da//z", "p0/k05", "p0/k07")));
    }

    @ParameterizedTest
    @MethodSource("listOrders")
    void sortsGlobalListUsingNamespaceOrder(KeySorting sorting, List<String> expected) {
        var keys = List.of("p0/k05", "p0/k07", "da//z");
        when(shardManager.allShardIds()).thenReturn(Set.of(0L, 1L, 2L));
        when(shardManager.getKeyComparator()).thenReturn(KeyOrder.comparator(sorting));
        doAnswer(
                        invocation -> {
                            ListRequest request = invocation.getArgument(0);
                            CancelableStreamObserver<ListResponse> observer = invocation.getArgument(1);
                            var response = new ListResponse();
                            response.addKey(keys.get(Math.toIntExact(request.getShard())));
                            observer.onNext(response);
                            observer.onCompleted();
                            return null;
                        })
                .when(rpcProvider)
                .list(any(ListRequest.class), any(CancelableStreamObserver.class));

        assertThat(client.list("", "").join()).containsExactlyElementsOf(expected);
        verify(shardManager, times(1)).getKeyComparator();
    }

    static Stream<Arguments> indexComparisons() {
        return Stream.of(
                Arguments.of(
                        KeySorting.KEY_SORTING_HIERARCHICAL,
                        GetOption.ComparisonCeiling,
                        "aa/x",
                        List.of("p0/k07", "da//z"),
                        "p0/k07",
                        "da//z"),
                Arguments.of(
                        KeySorting.KEY_SORTING_HIERARCHICAL,
                        GetOption.ComparisonHigher,
                        "aa/x",
                        List.of("p0/k07", "da//z"),
                        "p0/k07",
                        "da//z"),
                Arguments.of(
                        KeySorting.KEY_SORTING_HIERARCHICAL,
                        GetOption.ComparisonFloor,
                        "zz//x",
                        List.of("p0/k07", "da//z"),
                        "da//z",
                        "p0/k07"),
                Arguments.of(
                        KeySorting.KEY_SORTING_HIERARCHICAL,
                        GetOption.ComparisonLower,
                        "zz//x",
                        List.of("p0/k07", "da//z"),
                        "da//z",
                        "p0/k07"),
                Arguments.of(
                        KeySorting.KEY_SORTING_NATURAL,
                        GetOption.ComparisonCeiling,
                        "aa",
                        List.of("da1", "da//z"),
                        "da//z",
                        "da1"),
                Arguments.of(
                        KeySorting.KEY_SORTING_NATURAL,
                        GetOption.ComparisonHigher,
                        "aa",
                        List.of("da1", "da//z"),
                        "da//z",
                        "da1"),
                Arguments.of(
                        KeySorting.KEY_SORTING_NATURAL,
                        GetOption.ComparisonFloor,
                        "zz",
                        List.of("da1", "da//z"),
                        "da1",
                        "da//z"),
                Arguments.of(
                        KeySorting.KEY_SORTING_NATURAL,
                        GetOption.ComparisonLower,
                        "zz",
                        List.of("da1", "da//z"),
                        "da1",
                        "da//z"));
    }

    @ParameterizedTest
    @MethodSource("indexComparisons")
    void preservesExistingIndexGetMergeOrderWhileOrdinaryGetsUseNamespaceOrder(
            KeySorting sorting,
            GetOption comparison,
            String ordinaryQuery,
            List<String> candidates,
            String ordinaryExpected,
            String indexExpected) {
        when(shardManager.allShardIds()).thenReturn(Set.of(0L, 1L));
        when(shardManager.getKeyComparator()).thenReturn(KeyOrder.comparator(sorting));
        var operations = new ArrayList<GetOperation>();
        doAnswer(
                        invocation -> {
                            GetOperation operation = invocation.getArgument(0);
                            operations.add(operation);
                            operation
                                    .callback()
                                    .complete(record(candidates.get(Math.toIntExact(operation.shardId()))));
                            return null;
                        })
                .when(readBatchManager)
                .add(any(GetOperation.class));

        assertThat(client.get(ordinaryQuery, Set.of(comparison)).join().key())
                .isEqualTo(ordinaryExpected);
        assertThat(
                        client
                                .get("indexed-value", Set.of(comparison, GetOption.UseIndex("by-value")))
                                .join()
                                .key())
                .isEqualTo(indexExpected)
                .isNotEqualTo(ordinaryExpected);
        assertThat(operations).hasSize(4);
        assertThat(operations.subList(0, 2))
                .allSatisfy(operation -> assertThat(operation.options().secondaryIndexName()).isNull());
        assertThat(operations.subList(2, 4))
                .allSatisfy(
                        operation -> {
                            assertThat(operation.options().secondaryIndexName()).isEqualTo("by-value");
                            assertThat(operation.key()).isEqualTo("indexed-value");
                        });
        // Only the ordinary query consults namespace ordering.
        verify(shardManager, times(1)).getKeyComparator();
    }

    static Stream<Arguments> indexListOrders() {
        return Stream.of(
                Arguments.of(
                        KeySorting.KEY_SORTING_HIERARCHICAL,
                        List.of("p0/k07", "da//z"),
                        List.of("p0/k07", "da//z"),
                        List.of("da//z", "p0/k07")),
                Arguments.of(
                        KeySorting.KEY_SORTING_NATURAL,
                        List.of("da1", "da//z"),
                        List.of("da//z", "da1"),
                        List.of("da1", "da//z")));
    }

    @ParameterizedTest
    @MethodSource("indexListOrders")
    void preservesExistingIndexListOrderWhileOrdinaryListsUseNamespaceOrder(
            KeySorting sorting,
            List<String> keys,
            List<String> ordinaryExpected,
            List<String> indexExpected) {
        when(shardManager.allShardIds()).thenReturn(Set.of(0L, 1L));
        when(shardManager.getKeyComparator()).thenReturn(KeyOrder.comparator(sorting));
        var requests = new ArrayList<ListRequest>();
        doAnswer(
                        invocation -> {
                            ListRequest request = invocation.getArgument(0);
                            requests.add(request);
                            CancelableStreamObserver<ListResponse> observer = invocation.getArgument(1);
                            var response = new ListResponse();
                            response.addKey(keys.get(Math.toIntExact(request.getShard())));
                            observer.onNext(response);
                            observer.onCompleted();
                            return null;
                        })
                .when(rpcProvider)
                .list(any(ListRequest.class), any(CancelableStreamObserver.class));

        assertThat(client.list("", "").join()).containsExactlyElementsOf(ordinaryExpected);
        assertThat(client.list("", "", Set.of(ListOption.UseIndex("by-value"))).join())
                .containsExactlyElementsOf(indexExpected);
        assertThat(indexExpected).isNotEqualTo(ordinaryExpected);
        assertThat(requests).hasSize(4);
        assertThat(requests.subList(0, 2))
                .allSatisfy(request -> assertThat(request.hasSecondaryIndexName()).isFalse());
        assertThat(requests.subList(2, 4))
                .allSatisfy(request -> assertThat(request.getSecondaryIndexName()).isEqualTo("by-value"));
        verify(shardManager, times(1)).getKeyComparator();
    }

    @Test
    void capturesOrderBeforeWaitingForShardResponses() {
        when(shardManager.allShardIds()).thenReturn(Set.of(0L, 1L));
        when(shardManager.getKeyComparator())
                .thenReturn(
                        KeyOrder.comparator(KeySorting.KEY_SORTING_NATURAL),
                        KeyOrder.comparator(KeySorting.KEY_SORTING_HIERARCHICAL));
        var operations = ArgumentCaptor.forClass(GetOperation.class);
        doNothing().when(readBatchManager).add(operations.capture());

        var result = client.get("zz", Set.of(GetOption.ComparisonFloor));
        assertThat(result).isNotCompleted();
        for (var operation : operations.getAllValues()) {
            operation.callback().complete(record(operation.shardId() == 0 ? "da1" : "da//z"));
        }

        assertThat(result.join().key()).isEqualTo("da1");
        verify(shardManager, times(1)).getKeyComparator();
    }

    private static GetResult record(String key) {
        return key == null
                ? null
                : new GetResult(key, new byte[0], new Version(1, 2, 3, 4, empty(), empty()));
    }
}
