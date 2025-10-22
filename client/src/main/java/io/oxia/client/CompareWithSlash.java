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
package io.oxia.client;

import java.util.Comparator;
import java.util.function.Predicate;
import lombok.NonNull;

enum CompareWithSlash implements Comparator<String> {
    INSTANCE {
        @Override
        public int compare(@NonNull String a, @NonNull String b) {
            while (a.length() > 0 && b.length() > 0) {
                var idxA = a.indexOf('/');
                var idxB = b.indexOf('/');
                if (idxA < 0 && idxB < 0) {
                    return Integer.compare(a.compareTo(b), 0);
                } else if (idxA < 0) {
                    return -1;
                } else if (idxB < 0) {
                    return +1;
                }

                // At this point, both slices have '/'
                var spanA = a.substring(0, idxA);
                var spanB = b.substring(0, idxB);

                var spanRes = Integer.compare(spanA.compareTo(spanB), 0);
                if (spanRes != 0) {
                    return spanRes;
                }

                a = a.substring(idxA + 1);
                b = b.substring(idxB + 1);
            }

            if (a.length() < b.length()) {
                return -1;
            } else if (a.length() > 0) {
                return +1;
            } else {
                return 0;
            }
        }
    };

    static boolean withinRange(
            @NonNull String startKeyInclusive, @NonNull String endKeyExclusive, @NonNull String key) {
        return INSTANCE.compare(key, startKeyInclusive) >= 0
                && INSTANCE.compare(key, endKeyExclusive) < 0;
    }

    public static @NonNull Predicate<String> withinRange(
            @NonNull String startKeyInclusive, @NonNull String endKeyExclusive) {
        return k -> withinRange(startKeyInclusive, endKeyExclusive, k);
    }
}
