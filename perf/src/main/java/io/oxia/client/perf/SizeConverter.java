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
package io.oxia.client.perf;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.BaseConverter;

/** Parses a size in bytes, with an optional binary unit suffix: K, M, G or T (e.g. 10M, 1G). */
public class SizeConverter extends BaseConverter<Long> {

    public SizeConverter(String optionName) {
        super(optionName);
    }

    @Override
    public Long convert(String value) {
        try {
            return parseSize(value);
        } catch (NumberFormatException | ArithmeticException e) {
            throw new ParameterException(getErrorString(value, "a size (e.g. 512K, 10M, 1G)"));
        }
    }

    static long parseSize(String value) {
        String s = value.trim();
        int shift =
                switch (s.isEmpty() ? ' ' : Character.toUpperCase(s.charAt(s.length() - 1))) {
                    case 'K' -> 10;
                    case 'M' -> 20;
                    case 'G' -> 30;
                    case 'T' -> 40;
                    default -> 0;
                };
        if (shift > 0) {
            s = s.substring(0, s.length() - 1);
        }
        return Math.multiplyExact(Long.parseLong(s), 1L << shift);
    }
}
