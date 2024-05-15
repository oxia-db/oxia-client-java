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

package io.streamnative.oxia.client;

import io.streamnative.oxia.client.api.GetOption;
import io.streamnative.oxia.client.api.OptionComparisonType;
import io.streamnative.oxia.proto.KeyComparisonType;
import java.util.Set;
import lombok.experimental.UtilityClass;

@UtilityClass
public class GetOptionsUtil {

    public static KeyComparisonType getComparisonType(Set<GetOption> options) {
        if (options == null || options.isEmpty()) {
            return KeyComparisonType.EQUAL;
        }

        boolean alreadyHasComparisonType = false;
        KeyComparisonType comparisonType = KeyComparisonType.EQUAL;
        for (GetOption o : options) {
            if (o instanceof OptionComparisonType e) {

                if (alreadyHasComparisonType) {
                    throw new IllegalArgumentException(
                            "Incompatible " + GetOption.class.getSimpleName() + "s: " + options);
                }

                comparisonType =
                        switch (e.comparisonType()) {
                            case Equal -> KeyComparisonType.EQUAL;
                            case Floor -> KeyComparisonType.FLOOR;
                            case Ceiling -> KeyComparisonType.CEILING;
                            case Lower -> KeyComparisonType.LOWER;
                            case Higher -> KeyComparisonType.HIGHER;
                        };
                alreadyHasComparisonType = true;
            }
        }

        return comparisonType;
    }
}
