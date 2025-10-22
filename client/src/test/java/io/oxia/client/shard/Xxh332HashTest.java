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

import static io.oxia.client.shard.HashRangeShardStrategy.Xxh332Hash;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

// This test should be used to verify cross-platform consistency
public class Xxh332HashTest {

    private static Stream<Arguments> hashArgs() {
        return Stream.of(
                Arguments.of("foo", 125730186L),
                Arguments.of("bar", 2687685474L),
                Arguments.of("baz", 862947621L));
    }

    @ParameterizedTest
    @MethodSource("hashArgs")
    void ranges(String key, long hash) {
        assertThat(Xxh332Hash.apply(key)).isEqualTo(hash);
    }
}
