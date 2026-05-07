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

import java.util.Arrays;
import java.util.Objects;

/**
 * The result of a successful get / range-scan operation.
 *
 * <p>A {@code null} {@code GetResult} from a {@code get} call signals that no record exists for the
 * requested key. When {@link io.oxia.client.api.options.GetOption#ExcludeValue} is used, the {@code
 * value} field will be {@code null} but {@code key} and {@code version} are still populated.
 *
 * @param key the key of the record returned by the server (may differ from the key supplied to
 *     {@code get} when a non-equal {@link io.oxia.client.api.options.GetOption comparison} is used)
 * @param value the record's value, or {@code null} if the value was not requested
 * @param version metadata describing the record at the time it was read
 */
public record GetResult(String key, byte[] value, Version version) {

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GetResult other) {
            return Objects.equals(key, other.key)
                    && Arrays.equals(value, other.value)
                    && Objects.equals(version, other.version);
        }
        return false;
    }
}
