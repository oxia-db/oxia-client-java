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

import static java.util.stream.Collectors.toSet;

import io.oxia.proto.ShardAssignment;
import java.util.Collection;
import java.util.Set;
import lombok.NonNull;

public record Shard(long id, @NonNull String leader, @NonNull HashRange hashRange) {
    public boolean overlaps(@NonNull Shard other) {
        return hashRange.overlaps(other.hashRange);
    }

    public @NonNull Set<Shard> findOverlapping(@NonNull Collection<Shard> other) {
        return other.stream().filter(o -> !this.equals(o)).filter(this::overlaps).collect(toSet());
    }

    static @NonNull Shard fromProto(@NonNull ShardAssignment s) {
        return new Shard(s.getShard(), s.getLeader(), HashRange.fromProto(s.getInt32HashRange()));
    }
}
