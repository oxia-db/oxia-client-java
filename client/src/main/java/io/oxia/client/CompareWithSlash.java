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
            final int lenA = a.length();
            final int lenB = b.length();
            int ia = 0;
            int ib = 0;
            while (ia < lenA && ib < lenB) {
                int idxA = a.indexOf('/', ia);
                int idxB = b.indexOf('/', ib);
                if (idxA < 0 && idxB < 0) {
                    return Integer.signum(compareSpans(a, ia, lenA, b, ib, lenB));
                } else if (idxA < 0) {
                    return -1;
                } else if (idxB < 0) {
                    return +1;
                }

                // At this point, both slices have '/'
                int spanRes = compareSpans(a, ia, idxA, b, ib, idxB);
                if (spanRes != 0) {
                    return Integer.signum(spanRes);
                }

                ia = idxA + 1;
                ib = idxB + 1;
            }

            final int remainingA = lenA - ia;
            final int remainingB = lenB - ib;
            if (remainingA < remainingB) {
                return -1;
            } else if (remainingA > 0) {
                return +1;
            } else {
                return 0;
            }
        }
    };

    /** Compares the [from, to) regions of the two strings, like {@link String#compareTo} does. */
    private static int compareSpans(String a, int fromA, int toA, String b, int fromB, int toB) {
        final int lim = Math.min(toA - fromA, toB - fromB);
        for (int i = 0; i < lim; i++) {
            char ca = a.charAt(fromA + i);
            char cb = b.charAt(fromB + i);
            if (ca != cb) {
                return ca - cb;
            }
        }
        return (toA - fromA) - (toB - fromB);
    }

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
