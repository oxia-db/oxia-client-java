/*
 * Copyright © 2022-2024 StreamNative Inc.
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
package io.streamnative.oxia.client.api;

public sealed interface OptionVersionId extends PutOption, DeleteOption
        permits OptionVersionId.OptionRecordDoesNotExist, OptionVersionId.OptionVersionIdEqual {

    long versionId();

    record OptionVersionIdEqual(long versionId) implements OptionVersionId {
        public OptionVersionIdEqual {
            if (versionId < 0) {
                throw new IllegalArgumentException("versionId cannot be less than 0 - was: " + versionId);
            }
        }
    }

    record OptionRecordDoesNotExist() implements OptionVersionId {
        @Override
        public long versionId() {
            return Version.KeyNotExists;
        }
    }
}
