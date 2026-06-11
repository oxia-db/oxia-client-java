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
package io.oxia.client.shard;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class HashRangeShardStrategyTest {

    private static Stream<Arguments> rangesArgs() {
        return Stream.of(
                Arguments.of(0, 0, false),
                Arguments.of(1, 1, false),
                Arguments.of(1, 2, true),
                Arguments.of(1, 3, true),
                Arguments.of(2, 2, true),
                Arguments.of(2, 3, true),
                Arguments.of(3, 3, false));
    }

    @ParameterizedTest
    @MethodSource("rangesArgs")
    void constantHashFunction(long min, long max, boolean matches) {
        var strategy = new HashRangeShardStrategy(s -> 2L);
        var predicate = strategy.acceptsKeyPredicate("key");
        var shard = new Shard(1, "leader", new HashRange(min, max));
        assertThat(predicate.test(shard)).isEqualTo(matches);
    }

    // The key itself is the hash, so that the probes below are explicit
    private static final HashRangeShardStrategy keyAsHashStrategy =
            new HashRangeShardStrategy(Long::parseLong);

    private static Stream<Arguments> routerArgs() {
        return Stream.of(
                Arguments.of("5", -1L),
                Arguments.of("10", 1L),
                Arguments.of("15", 1L),
                Arguments.of("19", 1L),
                Arguments.of("20", 3L),
                Arguments.of("29", 3L),
                Arguments.of("30", 2L),
                Arguments.of("39", 2L),
                Arguments.of("40", -1L),
                Arguments.of("45", -1L));
    }

    @ParameterizedTest
    @MethodSource("routerArgs")
    void routerFindsShardByHash(String key, long expectedShardId) {
        // Deliberately unsorted, with a gap below 10 and above 39
        var shards =
                List.of(
                        new Shard(2, "leader", new HashRange(30, 39)),
                        new Shard(1, "leader", new HashRange(10, 19)),
                        new Shard(3, "leader", new HashRange(20, 29)));
        var router = keyAsHashStrategy.createRouter(shards);

        var shard = router.getShardForKey(key);
        if (expectedShardId < 0) {
            assertThat(shard).isNull();
        } else {
            assertThat(shard).isNotNull();
            assertThat(shard.id()).isEqualTo(expectedShardId);
        }
    }

    @Test
    void routerOnEmptyShards() {
        var router = keyAsHashStrategy.createRouter(List.of());
        assertThat(router.getShardForKey("0")).isNull();
    }

    @Test
    void routerMatchesLinearScan() {
        var random = new Random(0xacc7);
        for (int iteration = 0; iteration < 100; iteration++) {
            // Build random non-overlapping ranges, randomly dropping segments to create gaps
            List<Shard> shards = new ArrayList<>();
            long bound = 0;
            for (int segment = 0; segment < 100; segment++) {
                long min = bound + random.nextInt(100);
                long max = min + random.nextInt(100);
                if (random.nextBoolean()) {
                    shards.add(new Shard(segment, "leader", new HashRange(min, max)));
                }
                bound = max + 1;
            }

            var router = keyAsHashStrategy.createRouter(shards);
            for (int probe = 0; probe < 1_000; probe++) {
                String key = Long.toString(random.nextLong(bound + 100));
                var predicate = keyAsHashStrategy.acceptsKeyPredicate(key);
                Shard expected = shards.stream().filter(predicate).findFirst().orElse(null);
                assertThat(router.getShardForKey(key)).isEqualTo(expected);
            }
        }
    }
}
