/*
 * Copyright © 2022-2025 StreamNative Inc.
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
package io.streamnative.oxia.client.options;

import io.streamnative.oxia.client.api.GetOption;
import io.streamnative.oxia.client.api.OptionComparisonType;
import io.streamnative.oxia.client.api.OptionIncludeValue;
import io.streamnative.oxia.client.api.OptionPartitionKey;
import io.streamnative.oxia.proto.KeyComparisonType;
import java.util.Set;

public record GetOptions(
        String partitionKey, boolean includeValue, KeyComparisonType comparisonType) {

    public static GetOptions parseFrom(Set<GetOption> options) {
        boolean includeValue = true;
        KeyComparisonType comparisonType = KeyComparisonType.EQUAL;
        String partitionKey = null;
        for (GetOption option : options) {
            if (option instanceof OptionIncludeValue) {
                includeValue = ((OptionIncludeValue) option).includeValue();
                continue;
            }
            if (option instanceof OptionComparisonType) {
                comparisonType =
                        switch (((OptionComparisonType) option).comparisonType()) {
                            case Floor -> KeyComparisonType.FLOOR;
                            case Lower -> KeyComparisonType.LOWER;
                            case Higher -> KeyComparisonType.HIGHER;
                            case Ceiling -> KeyComparisonType.CEILING;
                            default -> KeyComparisonType.EQUAL;
                        };
                continue;
            }
            if (option instanceof OptionPartitionKey) {
                partitionKey = ((OptionPartitionKey) option).partitionKey();
                continue;
            }
        }
        return new GetOptions(partitionKey, includeValue, comparisonType);
    }
}
