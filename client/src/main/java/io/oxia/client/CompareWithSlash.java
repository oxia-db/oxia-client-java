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

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.function.Predicate;
import lombok.NonNull;

enum CompareWithSlash implements Comparator<String> {
    INSTANCE {
        @Override
        public int compare(@NonNull String a, @NonNull String b) {
            byte[] bytesA = a.getBytes(StandardCharsets.UTF_8);
            byte[] bytesB = b.getBytes(StandardCharsets.UTF_8);
            final int lenA = bytesA.length;
            final int lenB = bytesB.length;
            int ia = 0;
            int ib = 0;
            while (ia < lenA && ib < lenB) {
                int idxA = indexOfSlash(bytesA, ia);
                int idxB = indexOfSlash(bytesB, ib);
                if (idxA < 0 && idxB < 0) {
                    return Integer.signum(compareSpans(bytesA, ia, lenA, bytesB, ib, lenB));
                } else if (idxA < 0) {
                    return -1;
                } else if (idxB < 0) {
                    return +1;
                }

                // At this point, both slices have '/'
                int spanRes = compareSpans(bytesA, ia, idxA, bytesB, ib, idxB);
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

    private static int indexOfSlash(byte[] bytes, int from) {
        for (int i = from; i < bytes.length; i++) {
            if (bytes[i] == '/') {
                return i;
            }
        }
        return -1;
    }

    /** Compares UTF-8 spans as unsigned bytes, matching the legacy Go comparator. */
    private static int compareSpans(byte[] a, int fromA, int toA, byte[] b, int fromB, int toB) {
        final int lim = Math.min(toA - fromA, toB - fromB);
        for (int i = 0; i < lim; i++) {
            int ca = Byte.toUnsignedInt(a[fromA + i]);
            int cb = Byte.toUnsignedInt(b[fromB + i]);
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
