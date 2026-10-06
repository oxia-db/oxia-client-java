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

import static org.assertj.core.api.Assertions.assertThat;

import io.oxia.proto.KeySorting;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;

class KeyOrderTest {

    private static final Comparator<String> NATURAL =
            KeyOrder.comparator(KeySorting.KEY_SORTING_NATURAL);
    private static final Comparator<String> HIERARCHICAL =
            KeyOrder.comparator(KeySorting.KEY_SORTING_HIERARCHICAL);

    @Test
    void unknownAndAbsentSortingPreserveLegacyOrder() {
        assertThat(KeyOrder.comparator(null)).isSameAs(CompareWithSlash.INSTANCE);
        assertThat(KeyOrder.comparator(KeySorting.KEY_SORTING_UNKNOWN))
                .isSameAs(CompareWithSlash.INSTANCE);
        assertBefore(CompareWithSlash.INSTANCE, "a/y/z", "b/x");
        assertBefore(HIERARCHICAL, "b/x", "a/y/z");
    }

    @Test
    void naturalOrderingMatchesUtf8ByteOrderRatherThanNumericOrder() {
        assertBefore(NATURAL, "da//z", "da1");
        assertBefore(NATURAL, "da1", "da10");
        assertBefore(NATURAL, "k10", "k2");
        assertBefore(NATURAL, "", "\u0000");
        assertBefore(NATURAL, "\u0000", "/");
        assertBefore(NATURAL, "/", "a");
        assertBefore(NATURAL, "/a//", "/a/b");
    }

    @Test
    void hierarchicalOrderingComparesDepthBeforeSpans() {
        assertBefore(HIERARCHICAL, "z", "/a");
        assertBefore(HIERARCHICAL, "b/x", "a/y/z");
        assertBefore(HIERARCHICAL, "random-probe:p0/k07", "random-probe:da//z");
        assertBefore(HIERARCHICAL, "z/x", "a//y");
    }

    @Test
    void hierarchicalSeparatorsSortAfterOtherBytesAtTheSameDepth() {
        assertBefore(HIERARCHICAL, "aa/", "a/a");
        assertBefore(HIERARCHICAL, "/aa/", "/a/a");
        assertBefore(HIERARCHICAL, "a/\u0000", "a//");
        assertBefore(HIERARCHICAL, "a/\uD800\uDC00", "a//");
    }

    @Test
    void hierarchicalTrailingDoubleSlashSubtractsOnlyOneLevel() {
        assertBefore(HIERARCHICAL, "", "/");
        assertBefore(HIERARCHICAL, "/", "//");
        assertBefore(HIERARCHICAL, "//", "///");
        assertBefore(HIERARCHICAL, "z/x", "a///");
        assertBefore(HIERARCHICAL, "a/0", "a//");
        assertBefore(HIERARCHICAL, "a//0", "a///");
        assertBefore(HIERARCHICAL, "/a/z", "/a//");
    }

    @Test
    void bothOrdersCompareUnsignedUtf8RatherThanUtf16CodeUnits() {
        String privateUse = "\uE000";
        String supplementary = "\uD800\uDC00";
        assertThat(privateUse.compareTo(supplementary)).isPositive();
        assertBefore(NATURAL, privateUse, supplementary);
        assertBefore(HIERARCHICAL, privateUse, supplementary);
        assertBefore(NATURAL, "a/" + privateUse, "a/" + supplementary);
        assertBefore(HIERARCHICAL, "a/" + privateUse, "a/" + supplementary);
        assertBefore(NATURAL, "a", "\u00E9");
        assertBefore(HIERARCHICAL, "a", "\u00E9");
    }

    @Test
    void internalKeysSortAfterUserKeysWithoutTreatingSimilarPrefixesAsInternal() {
        assertBefore(NATURAL, "__oxia", "z");
        assertBefore(NATURAL, "z", "__oxia/a");
        assertBefore(NATURAL, "\uD800\uDC00", "__oxia/a");
        assertBefore(HIERARCHICAL, "__oxia", "z");
        assertBefore(HIERARCHICAL, "z/a/a", "__oxia/a");
        assertBefore(HIERARCHICAL, "__oxia0/a", "__oxia/a");
        assertBefore(HIERARCHICAL, "__oxia/a", "__oxia/b");
    }

    @Test
    void hierarchicalDepthSaturatesWithoutWrappingIntoUserOrInternalKeyRegions() {
        String maxDepth = "/".repeat(32_767) + "z";
        String deeper = "/".repeat(32_768) + "a";
        String muchDeeper = "/".repeat(65_536) + "a";
        assertBefore(HIERARCHICAL, maxDepth, deeper);
        assertBefore(HIERARCHICAL, deeper, muchDeeper);
        assertBefore(HIERARCHICAL, muchDeeper, "__oxia/a");
        // The double-slash range-boundary rule is applied before saturation.
        assertBefore(HIERARCHICAL, "/".repeat(32_767), "/".repeat(32_768));
    }

    @Test
    void naturalSortHasTheConcreteServerOrderForMixedKeys() {
        List<String> expected =
                List.of(
                        "",
                        "/",
                        "/a",
                        "__oxia",
                        "a",
                        "a//",
                        "a/0",
                        "a0",
                        "da//z",
                        "da1",
                        "da10",
                        "db",
                        "k10",
                        "k2",
                        "z",
                        "\uE000",
                        "\uD800\uDC00",
                        "__oxia/a");
        var reversed = new ArrayList<String>();
        for (int i = expected.size() - 1; i >= 0; i--) {
            reversed.add(expected.get(i));
        }
        reversed.sort(NATURAL);
        assertThat(reversed).containsExactlyElementsOf(expected);
    }

    private static void assertBefore(Comparator<String> comparator, String first, String second) {
        assertThat(comparator.compare(first, second)).as("compare(%s, %s)", first, second).isNegative();
        assertThat(comparator.compare(second, first)).as("compare(%s, %s)", second, first).isPositive();
        assertThat(comparator.compare(first, first)).isZero();
        assertThat(comparator.compare(second, second)).isZero();
    }
}
