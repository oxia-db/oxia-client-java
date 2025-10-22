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
package io.oxia.client.api.options;

import io.oxia.client.api.options.defs.OptionPartitionKey;
import io.oxia.client.api.options.defs.OptionVersionId;

/** Options for deleting a record. */
public interface DeleteOption {

    /**
     * Conditional delete will only succeed if the record's version matches the supplied versionId.
     *
     * @param versionId the versionId to compare with the record's version.
     * @return the delete option.
     */
    static DeleteOption IfVersionIdEquals(long versionId) {
        return new OptionVersionId.OptionVersionIdEqual(versionId);
    }

    /**
     * PartitionKey overrides the partition routing with the specified `partitionKey` instead of the
     * regular record key.
     *
     * <p>Records with the same partitionKey will always be guaranteed to be co-located in the same
     * Oxia shard.
     *
     * @param partitionKey the partition key to use
     * @return the delete option.
     */
    static DeleteOption PartitionKey(String partitionKey) {
        return new OptionPartitionKey(partitionKey);
    }
}
