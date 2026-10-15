/*
 * Copyright © 2022-2026 The Oxia Authors
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

import io.oxia.proto.KeySorting;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

/** Namespace key ordering, matching the server storage encodings. */
public final class KeyOrder {

    private static final String INTERNAL_KEY_PREFIX = "__oxia/";
    private static final int INTERNAL_KEY_MARKER = 1 << 15;
    private static final int MAX_DEPTH = INTERNAL_KEY_MARKER - 1;
    private static final byte ENCODED_SEPARATOR = (byte) 0xff;

    private static final Comparator<String> NATURAL =
            (left, right) -> Arrays.compareUnsigned(encodeNatural(left), encodeNatural(right));
    private static final Comparator<String> HIERARCHICAL =
            (left, right) -> Arrays.compareUnsigned(encodeHierarchical(left), encodeHierarchical(right));

    private KeyOrder() {}

    /** Old servers omit the sorting field, so absent or unknown values preserve the legacy order. */
    public static Comparator<String> comparator(KeySorting keySorting) {
        if (keySorting == null) {
            return CompareWithSlash.INSTANCE;
        }
        return switch (keySorting) {
            case KEY_SORTING_NATURAL -> NATURAL;
            case KEY_SORTING_HIERARCHICAL -> HIERARCHICAL;
            default -> CompareWithSlash.INSTANCE;
        };
    }

    private static byte[] encodeNatural(String key) {
        byte[] encoded = key.getBytes(StandardCharsets.UTF_8);
        if (key.startsWith(INTERNAL_KEY_PREFIX)) {
            encoded[0] = ENCODED_SEPARATOR;
            encoded[1] = ENCODED_SEPARATOR;
        }
        return encoded;
    }

    private static byte[] encodeHierarchical(String key) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] encoded = new byte[bytes.length + 2];
        int depth = 0;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == '/') {
                depth++;
                encoded[i + 2] = ENCODED_SEPARATOR;
            } else {
                encoded[i + 2] = bytes[i];
            }
        }
        // A trailing double slash is a range boundary and adds only one level.
        if (key.endsWith("//")) {
            depth--;
        }
        depth = Math.min(depth, MAX_DEPTH);
        if (key.startsWith(INTERNAL_KEY_PREFIX)) {
            depth |= INTERNAL_KEY_MARKER;
        }
        encoded[0] = (byte) (depth >>> 8);
        encoded[1] = (byte) depth;
        return encoded;
    }
}
