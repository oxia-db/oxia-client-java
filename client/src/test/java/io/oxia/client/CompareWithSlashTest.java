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

import java.util.Random;
import org.junit.jupiter.api.Test;

class CompareWithSlashTest {

    @Test
    void compare() {
        assertThat(CompareWithSlash.INSTANCE.compare("aaaaa", "aaaaa")).isEqualTo(0);
        assertThat(CompareWithSlash.INSTANCE.compare("aaaaa", "zzzzz")).isEqualTo(-1);
        assertThat(CompareWithSlash.INSTANCE.compare("bbbbb", "aaaaa")).isEqualTo(+1);

        assertThat(CompareWithSlash.INSTANCE.compare("aaaaa", "")).isEqualTo(+1);
        assertThat(CompareWithSlash.INSTANCE.compare("", "aaaaaa")).isEqualTo(-1);
        assertThat(CompareWithSlash.INSTANCE.compare("", "")).isEqualTo(0);

        assertThat(CompareWithSlash.INSTANCE.compare("aaaaa", "aaaaaaaaaaa")).isEqualTo(-1);
        assertThat(CompareWithSlash.INSTANCE.compare("aaaaaaaaaaa", "aaa")).isEqualTo(+1);

        assertThat(CompareWithSlash.INSTANCE.compare("a", "/")).isEqualTo(-1);
        assertThat(CompareWithSlash.INSTANCE.compare("/", "a")).isEqualTo(+1);

        assertThat(CompareWithSlash.INSTANCE.compare("/aaaa", "/bbbbb")).isEqualTo(-1);
        assertThat(CompareWithSlash.INSTANCE.compare("/aaaa", "/aa/a")).isEqualTo(-1);
        assertThat(CompareWithSlash.INSTANCE.compare("/aaaa/a", "/aaaa/b")).isEqualTo(-1);

        assertThat(CompareWithSlash.INSTANCE.compare("/aaaa/a/a", "/bbbbbbbbbb")).isEqualTo(+1);
        assertThat(CompareWithSlash.INSTANCE.compare("/aaaa/a/a", "/aaaa/bbbbbbbbbb")).isEqualTo(+1);

        assertThat(CompareWithSlash.INSTANCE.compare("/a/b/a/a/a", "/a/b/a/b")).isEqualTo(+1);
    }

    @Test
    void withinRange() {
        assertThat(CompareWithSlash.withinRange("aaaaa", "aaaac", "aaaaa")).isTrue();
        assertThat(CompareWithSlash.withinRange("aaaaa", "aaaac", "aaaab")).isTrue();
        assertThat(CompareWithSlash.withinRange("aaaaa", "aaaac", "aaaac")).isFalse();
        assertThat(CompareWithSlash.withinRange("aaaaa", "aaaac", "aaaa")).isFalse();
    }

    @Test
    void compareMatchesLegacyImplementation() {
        var random = new Random(0xacc7);
        char[] alphabet = {'a', 'b', 'c', '/'};
        for (int i = 0; i < 100_000; i++) {
            String a = randomKey(random, alphabet);
            String b = randomKey(random, alphabet);
            assertThat(CompareWithSlash.INSTANCE.compare(a, b))
                    .as("compare(%s, %s)", a, b)
                    .isEqualTo(legacyCompare(a, b));
        }
    }

    private static String randomKey(Random random, char[] alphabet) {
        int len = random.nextInt(12);
        var sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(alphabet[random.nextInt(alphabet.length)]);
        }
        return sb.toString();
    }

    /** The substring-based implementation that predates the index-based rewrite. */
    private static int legacyCompare(String a, String b) {
        while (a.length() > 0 && b.length() > 0) {
            var idxA = a.indexOf('/');
            var idxB = b.indexOf('/');
            if (idxA < 0 && idxB < 0) {
                return Integer.compare(a.compareTo(b), 0);
            } else if (idxA < 0) {
                return -1;
            } else if (idxB < 0) {
                return +1;
            }

            var spanA = a.substring(0, idxA);
            var spanB = b.substring(0, idxB);

            var spanRes = Integer.compare(spanA.compareTo(spanB), 0);
            if (spanRes != 0) {
                return spanRes;
            }

            a = a.substring(idxA + 1);
            b = b.substring(idxB + 1);
        }

        if (a.length() < b.length()) {
            return -1;
        } else if (a.length() > 0) {
            return +1;
        } else {
            return 0;
        }
    }
}
