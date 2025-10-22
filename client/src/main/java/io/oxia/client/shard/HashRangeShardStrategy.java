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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.function.Function;
import java.util.function.Predicate;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.openhft.hashing.LongHashFunction;

@RequiredArgsConstructor
class HashRangeShardStrategy implements ShardStrategy {

    private final Function<String, Long> hashFn;

    @Override
    @NonNull
    public Predicate<Shard> acceptsKeyPredicate(@NonNull String key) {
        long hash = hashFn.apply(key);
        return shard ->
                shard.hashRange().minInclusive() <= hash && hash <= shard.hashRange().maxInclusive();
    }

    static final Function<String, Long> Xxh332Hash =
            s -> LongHashFunction.xx3().hashBytes(s.getBytes(UTF_8)) & 0x00000000FFFFFFFFL;

    static final ShardStrategy Xxh332HashRangeShardStrategy = new HashRangeShardStrategy(Xxh332Hash);
}
