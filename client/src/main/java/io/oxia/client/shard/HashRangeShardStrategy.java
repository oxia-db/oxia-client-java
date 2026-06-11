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
package io.oxia.client.shard;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.openhft.hashing.LongHashFunction;

@RequiredArgsConstructor
class HashRangeShardStrategy implements ShardStrategy {

    private final ToLongFunction<String> hashFn;

    @Override
    @NonNull
    public Predicate<Shard> acceptsKeyPredicate(@NonNull String key) {
        long hash = hashFn.applyAsLong(key);
        return shard ->
                shard.hashRange().minInclusive() <= hash && hash <= shard.hashRange().maxInclusive();
    }

    /**
     * The hash ranges are non-overlapping: sort them by their lower bound and binary-search the key
     * hash, so that the per-operation lookup is O(log n) and allocation-free.
     */
    @Override
    public @NonNull ShardRouter createRouter(@NonNull Collection<Shard> shards) {
        final Shard[] sorted = shards.toArray(new Shard[0]);
        Arrays.sort(sorted, Comparator.comparingLong(s -> s.hashRange().minInclusive()));
        final long[] minHashes = new long[sorted.length];
        final long[] maxHashes = new long[sorted.length];
        for (int i = 0; i < sorted.length; i++) {
            minHashes[i] = sorted[i].hashRange().minInclusive();
            maxHashes[i] = sorted[i].hashRange().maxInclusive();
        }
        return key -> {
            final long hash = hashFn.applyAsLong(key);
            int idx = Arrays.binarySearch(minHashes, hash);
            if (idx < 0) {
                // Not an exact match on a lower bound: take the floor entry, i.e. the one
                // preceding the insertion point
                idx = -idx - 2;
            }
            if (idx < 0 || hash > maxHashes[idx]) {
                return null;
            }
            return sorted[idx];
        };
    }

    static final ToLongFunction<String> Xxh332Hash =
            s -> LongHashFunction.xx3().hashBytes(s.getBytes(UTF_8)) & 0x00000000FFFFFFFFL;

    static final ShardStrategy Xxh332HashRangeShardStrategy = new HashRangeShardStrategy(Xxh332Hash);
}
