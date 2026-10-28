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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import io.oxia.client.api.options.defs.OptionOverrideModificationsCount;
import io.oxia.client.api.options.defs.OptionOverrideVersionId;
import io.oxia.client.api.options.defs.OptionSecondaryIndex;
import io.oxia.client.api.options.defs.OptionSecondaryIndexName;
import io.oxia.client.grpc.OxiaStatusException;
import io.oxia.proto.KeyComparisonType;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class OptionsUtilsTest {

    static Stream<Arguments> extractors() {
        return Stream.of(
                Arguments.of(
                        (Function<Set<?>, Object>) OptionsUtils::getVersionId,
                        PutOption.IfVersionIdEquals(1),
                        PutOption.IfVersionIdEquals(2)),
                Arguments.of(
                        (Function<Set<?>, Object>) OptionsUtils::getPartitionKey,
                        PutOption.PartitionKey("a"),
                        PutOption.PartitionKey("b")),
                Arguments.of(
                        (Function<Set<?>, Object>) OptionsUtils::getSequenceKeysDeltas,
                        PutOption.SequenceKeysDeltas(List.of(1L)),
                        PutOption.SequenceKeysDeltas(List.of(2L))),
                Arguments.of(
                        (Function<Set<?>, Object>) OptionsUtils::getOverrideVersionId,
                        new OptionOverrideVersionId(1),
                        new OptionOverrideVersionId(2)),
                Arguments.of(
                        (Function<Set<?>, Object>) OptionsUtils::getOverrideModificationsCount,
                        new OptionOverrideModificationsCount(1),
                        new OptionOverrideModificationsCount(2)),
                Arguments.of(
                        (Function<Set<?>, Object>) OptionsUtils::getSecondaryIndexName,
                        new OptionSecondaryIndexName("a"),
                        new OptionSecondaryIndexName("b")));
    }

    @ParameterizedTest
    @MethodSource("extractors")
    void rejectsRepeatedOption(Function<Set<?>, Object> extractor, Object first, Object second) {
        assertThat(extractor.apply(null)).isEqualTo(extractor.apply(Set.of()));
        assertThat(extractor.apply(Set.of(PutOption.AsEphemeralRecord, first)))
                .isNotEqualTo(extractor.apply(Set.of()));
        assertThatThrownBy(() -> extractor.apply(Set.of(first, second)))
                .isInstanceOf(OxiaStatusException.class);
    }

    @Test
    void getSecondaryIndexes() {
        var a = new OptionSecondaryIndex("idx", "a");
        var b = new OptionSecondaryIndex("idx", "b");
        assertThat(OptionsUtils.getSecondaryIndexes(null)).isEmpty();
        assertThat(OptionsUtils.getSecondaryIndexes(Set.of(PutOption.AsEphemeralRecord))).isEmpty();
        assertThat(OptionsUtils.getSecondaryIndexes(Set.of(a, b, PutOption.AsEphemeralRecord)))
                .containsExactlyInAnyOrder(a, b);
    }

    static Stream<Arguments> comparisonTypes() {
        return Stream.of(
                Arguments.of(GetOption.ComparisonEqual, KeyComparisonType.EQUAL),
                Arguments.of(GetOption.ComparisonFloor, KeyComparisonType.FLOOR),
                Arguments.of(GetOption.ComparisonCeiling, KeyComparisonType.CEILING),
                Arguments.of(GetOption.ComparisonLower, KeyComparisonType.LOWER),
                Arguments.of(GetOption.ComparisonHigher, KeyComparisonType.HIGHER));
    }

    @ParameterizedTest
    @MethodSource("comparisonTypes")
    void getComparisonType(GetOption option, KeyComparisonType expected) {
        assertThat(OptionsUtils.getComparisonType(Set.of(option, GetOption.PartitionKey("a"))))
                .isEqualTo(expected);
    }

    @Test
    void getComparisonTypeDefaultsToEqual() {
        assertThat(OptionsUtils.getComparisonType(null)).isEqualTo(KeyComparisonType.EQUAL);
        assertThat(OptionsUtils.getComparisonType(Set.of())).isEqualTo(KeyComparisonType.EQUAL);
        assertThat(OptionsUtils.getComparisonType(Set.of(GetOption.PartitionKey("a"))))
                .isEqualTo(KeyComparisonType.EQUAL);
    }

    @Test
    void getComparisonTypeRejectsTwoComparisons() {
        assertThatThrownBy(
                        () ->
                                OptionsUtils.getComparisonType(
                                        Set.of(GetOption.ComparisonFloor, GetOption.ComparisonCeiling)))
                .isInstanceOf(OxiaStatusException.class);
    }

    @Test
    void includesValue() {
        assertThat(OptionsUtils.includesValue(Set.of())).isTrue();
        assertThat(OptionsUtils.includesValue(Set.of(RangeScanOption.PartitionKey("a")))).isTrue();
        assertThat(OptionsUtils.includesValue(Set.of(RangeScanOption.IncludeValue(false)))).isFalse();
        assertThat(OptionsUtils.includesValue(Set.of(RangeScanOption.IncludeValue(true)))).isTrue();
    }
}
