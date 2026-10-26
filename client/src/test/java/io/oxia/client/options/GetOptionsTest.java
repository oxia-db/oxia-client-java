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
package io.oxia.client.options;

import static org.assertj.core.api.Assertions.assertThat;

import io.oxia.client.api.options.GetOption;
import io.oxia.proto.KeyComparisonType;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class GetOptionsTest {

    @Test
    void defaults() {
        assertThat(GetOptions.parseFrom(Set.of()))
                .isEqualTo(new GetOptions(null, true, KeyComparisonType.EQUAL, null));
    }

    @Test
    void parsesAllOptions() {
        assertThat(
                        GetOptions.parseFrom(
                                Set.of(
                                        GetOption.IncludeValue(false),
                                        GetOption.ComparisonFloor,
                                        GetOption.PartitionKey("pk"),
                                        GetOption.UseIndex("idx"))))
                .isEqualTo(new GetOptions("pk", false, KeyComparisonType.FLOOR, "idx"));
    }

    @ParameterizedTest
    @MethodSource("comparisons")
    void parsesComparisonType(GetOption option, KeyComparisonType expected) {
        assertThat(GetOptions.parseFrom(Set.of(option)).comparisonType()).isEqualTo(expected);
    }

    static Stream<Arguments> comparisons() {
        return Stream.of(
                Arguments.of(GetOption.ComparisonEqual, KeyComparisonType.EQUAL),
                Arguments.of(GetOption.ComparisonFloor, KeyComparisonType.FLOOR),
                Arguments.of(GetOption.ComparisonCeiling, KeyComparisonType.CEILING),
                Arguments.of(GetOption.ComparisonLower, KeyComparisonType.LOWER),
                Arguments.of(GetOption.ComparisonHigher, KeyComparisonType.HIGHER));
    }

    @Test
    void ignoresUnknownOptions() {
        assertThat(GetOptions.parseFrom(Set.of(new GetOption() {})))
                .isEqualTo(new GetOptions(null, true, KeyComparisonType.EQUAL, null));
    }
}
