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
package io.oxia.client.api;

/**
 * An {@link Iterable} that holds resources which must be released by calling {@link #close()}.
 *
 * <p>Intended for use with try-with-resources:
 *
 * <pre>{@code
 * try (CloseableIterable<GetResult> scan = client.rangeScan(start, end)) {
 *     for (GetResult r : scan) {
 *         if (done) break;
 *     }
 * }
 * }</pre>
 *
 * @param <T> the element type
 */
public interface CloseableIterable<T> extends Iterable<T>, AutoCloseable {

    /** Releases any resources held by this iterable, cancelling iteration in progress. */
    @Override
    void close();
}
