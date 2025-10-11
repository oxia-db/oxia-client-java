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
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.oxia.proto.Int32HashRange;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class HashRangeTest {
    private static Stream<Arguments> rangesArgs() {
        return Stream.of(
                Arguments.of(0, 0, true),
                Arguments.of(1, 1, true),
                Arguments.of(1, 2, true),
                Arguments.of(2, 1, false),
                Arguments.of(-1, 1, false),
                Arguments.of(1, -1, false),
                Arguments.of(-2, -1, false));
    }

    @ParameterizedTest
    @MethodSource("rangesArgs")
    void ranges(long min, long max, boolean pass) {
        if (pass) {
            assertThatNoException().isThrownBy(() -> new HashRange(min, max));
        } else {
            assertThatThrownBy(() -> new HashRange(min, max))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static Stream<Arguments> overlapArgs() {
        return Stream.of(
                Arguments.of(0, 0, 0, 0, true),
                Arguments.of(1, 3, 2, 4, true),
                Arguments.of(2, 4, 1, 3, true),
                Arguments.of(0, 1, 1, 2, true),
                Arguments.of(1, 2, 0, 1, true),
                Arguments.of(0, 1, 2, 3, false),
                Arguments.of(2, 3, 0, 1, false),
                Arguments.of(10, 10, 20, 20, false));
    }

    @ParameterizedTest
    @MethodSource("overlapArgs")
    void overlaps(long min1, long max1, long min2, long max2, boolean overlaps) {
        var range1 = new HashRange(min1, max1);
        var range2 = new HashRange(min2, max2);
        assertThat(range1.overlaps(range2)).isEqualTo(overlaps);
    }

    @Test
    void fromProto() {
        var range =
                HashRange.fromProto(
                        Int32HashRange.newBuilder()
                                .setMinHashInclusive(Integer.MIN_VALUE)
                                .setMaxHashInclusive(-1)
                                .build());
        assertThat(range.minInclusive()).isEqualTo(((long) Integer.MAX_VALUE) + 1L);
        assertThat(range.maxInclusive()).isEqualTo(((long) Integer.MAX_VALUE * 2L) + 1L);
    }
}
