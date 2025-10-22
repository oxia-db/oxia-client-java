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
package io.oxia.client.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.oxia.client.OptionsUtils;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import java.util.Collections;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DeleteOptionTest {

    @Nested
    @DisplayName("ifVersionIdEquals tests")
    class IfVersionIdEqualsTests {
        @Test
        void ifVersionIdEquals() {
            assertThat(DeleteOption.IfVersionIdEquals(1L))
                    .satisfies(
                            o -> {
                                assertThat(o).isInstanceOf(OptionVersionId.OptionVersionIdEqual.class);
                            });
        }

        @Test
        void versionIdLessThanZero() {
            assertThatNoException().isThrownBy(() -> DeleteOption.IfVersionIdEquals(0L));
            assertThatThrownBy(() -> DeleteOption.IfVersionIdEquals(-1L))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void toVersionId() {
        assertThat(OptionsUtils.getVersionId(Collections.emptySet())).isEmpty();
        assertThat(OptionsUtils.getVersionId(Set.of(DeleteOption.IfVersionIdEquals(1L)))).hasValue(1L);
        assertThatThrownBy(
                        () ->
                                OptionsUtils.getVersionId(
                                        Set.of(DeleteOption.IfVersionIdEquals(1L), DeleteOption.IfVersionIdEquals(1L))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                OptionsUtils.getVersionId(
                                        Set.of(DeleteOption.IfVersionIdEquals(1L), DeleteOption.IfVersionIdEquals(2L))))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
