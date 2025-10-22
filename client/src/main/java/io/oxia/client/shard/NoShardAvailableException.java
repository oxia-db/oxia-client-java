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

import lombok.Getter;
import lombok.NonNull;

/** A leader has not been assigned to the shard that maps to the key. */
public class NoShardAvailableException extends RuntimeException {
    @Getter private final String key;
    @Getter private final Long shardId;

    /**
     * Creates an instance of the exception.
     *
     * @param key The key specified in the call.
     */
    public NoShardAvailableException(@NonNull String key) {
        super("No shard available to accept to key: " + key);
        this.key = key;
        this.shardId = null;
    }

    /**
     * Creates an instance of the exception.
     *
     * @param shardId The shard id specified in the call.
     */
    public NoShardAvailableException(long shardId) {
        super("Shard not available : " + shardId);
        this.shardId = shardId;
        this.key = null;
    }
}
