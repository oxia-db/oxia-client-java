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

/** A notification from an Oxia server indicating a change to a record associated with a key. */
public sealed interface Notification
        permits Notification.KeyCreated,
                Notification.KeyDeleted,
                Notification.KeyModified,
                Notification.KeyRangeDelete {

    /**
     * @return The key of the record.
     */
    String key();

    /**
     * A record associated with the key has been created.
     *
     * @param key The key of the record created.
     * @param version The versionId of the new record.
     */
    record KeyCreated(String key, long version) implements Notification {}

    /**
     * The record associated with the key has been modified (updated).
     *
     * @param key The key of the record modified.
     * @param version The versionId of the record after the modification.
     */
    record KeyModified(String key, long version) implements Notification {}

    /**
     * The record associated with the key has been deleted.
     *
     * @param key The key of the deleted record.
     */
    record KeyDeleted(String key) implements Notification {}

    /**
     * The record associated with the key range has been deleted.
     *
     * @param startKeyInclusive The range deletion start key. (inclusive)
     * @param endKeyExclusive The range deletion end key. (exclusive)
     */
    record KeyRangeDelete(String startKeyInclusive, String endKeyExclusive) implements Notification {
        @Override
        public String key() {
            return startKeyInclusive;
        }
    }
}
