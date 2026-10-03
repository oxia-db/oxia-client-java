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
package io.oxia.client.perf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.beust.jcommander.ParameterException;
import org.junit.jupiter.api.Test;

class SizeConverterTest {

    private final SizeConverter converter = new SizeConverter("--max-pending-bytes");

    @Test
    void plainBytes() {
        assertThat(converter.convert("0")).isEqualTo(0L);
        assertThat(converter.convert("1234")).isEqualTo(1234L);
    }

    @Test
    void unitSuffixes() {
        assertThat(converter.convert("512K")).isEqualTo(512L * 1024);
        assertThat(converter.convert("10M")).isEqualTo(10L * 1024 * 1024);
        assertThat(converter.convert("1G")).isEqualTo(1024L * 1024 * 1024);
        assertThat(converter.convert("2T")).isEqualTo(2L * 1024 * 1024 * 1024 * 1024);
        assertThat(converter.convert("256m")).isEqualTo(256L * 1024 * 1024);
    }

    @Test
    void invalid() {
        assertThatThrownBy(() -> converter.convert("")).isInstanceOf(ParameterException.class);
        assertThatThrownBy(() -> converter.convert("M")).isInstanceOf(ParameterException.class);
        assertThatThrownBy(() -> converter.convert("10X")).isInstanceOf(ParameterException.class);
        assertThatThrownBy(() -> converter.convert("1.5G")).isInstanceOf(ParameterException.class);
        assertThatThrownBy(() -> converter.convert("10000000T")).isInstanceOf(ParameterException.class);
    }
}
