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

import io.oxia.proto.Int32HashRange;
import io.oxia.proto.ShardAssignment;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ShardTest {

    @Test
    void shardFromProto() {
        var shard =
                Shard.fromProto(
                        ShardAssignment.newBuilder()
                                .setShard(1)
                                .setInt32HashRange(
                                        Int32HashRange.newBuilder()
                                                .setMinHashInclusive(2)
                                                .setMaxHashInclusive(3)
                                                .build())
                                .build());
        assertThat(shard)
                .satisfies(
                        s -> {
                            assertThat(s.id()).isEqualTo(1);
                            assertThat(s.hashRange())
                                    .satisfies(
                                            h -> {
                                                assertThat(h.minInclusive()).isEqualTo(2L);
                                                assertThat(h.maxInclusive()).isEqualTo(3L);
                                            });
                        });
    }

    private static Stream<Arguments> findOverlappingArgs() {
        return Stream.of(
                // Equal
                Arguments.of(shard(0, 0, 0, 0), Set.of(shard(0, 0, 0, 0)), Set.of()),
                // Leader change
                Arguments.of(shard(0, 0, 0, 0), Set.of(shard(0, 1, 0, 0)), Set.of(shard(0, 1, 0, 0))),
                // ID change
                Arguments.of(shard(0, 0, 0, 0), Set.of(shard(1, 0, 0, 0)), Set.of(shard(1, 0, 0, 0))),
                // No overlap - although ID remains the same - probably an error condition?
                Arguments.of(shard(0, 0, 0, 0), Set.of(shard(0, 0, 1, 1)), Set.of()),
                // Partial overlaps
                Arguments.of(
                        shard(0, 0, 1, 2),
                        Set.of(shard(1, 0, 0, 1), shard(2, 0, 2, 3)),
                        Set.of(shard(1, 0, 0, 1), shard(2, 0, 2, 3))));
    }

    @ParameterizedTest
    @MethodSource("findOverlappingArgs")
    void findOverlapping(Shard target, Set<Shard> updates, Set<Shard> overlaps) {
        assertThat(target.findOverlapping(updates))
                .containsExactlyInAnyOrder(overlaps.toArray(overlaps.toArray(new Shard[0])));
    }

    static Shard shard(long id, int leader, long min, long max) {
        return new Shard(id, Integer.toString(leader), new HashRange(min, max));
    }
}
