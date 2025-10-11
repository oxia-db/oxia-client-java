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

import static io.oxia.client.ProtoUtil.uint32ToLong;

import io.oxia.proto.Int32HashRange;
import lombok.NonNull;

public record HashRange(long minInclusive, long maxInclusive) {
    public HashRange {
        checkHash(minInclusive);
        checkHash(maxInclusive);
        if (maxInclusive < minInclusive) {
            throw new IllegalArgumentException(
                    "Invalid HashRange: [" + minInclusive + ":" + maxInclusive + "]");
        }
    }

    private static void checkHash(long hash) {
        if (hash < 0) {
            throw new IllegalArgumentException("Invalid HashRange bound: " + hash);
        }
    }

    public boolean overlaps(@NonNull HashRange other) {
        return !(minInclusive > other.maxInclusive || maxInclusive < other.minInclusive);
    }

    static @NonNull HashRange fromProto(@NonNull Int32HashRange p) {
        return new HashRange(
                uint32ToLong(p.getMinHashInclusive()), uint32ToLong(p.getMaxHashInclusive()));
    }
}
