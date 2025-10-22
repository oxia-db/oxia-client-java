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
}
