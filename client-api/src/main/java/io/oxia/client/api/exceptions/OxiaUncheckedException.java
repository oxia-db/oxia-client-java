/*
 * Copyright © 2026 The Oxia Authors
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
package io.oxia.client.api.exceptions;

import java.util.Objects;

/**
 * Wraps an {@link OxiaException} with an unchecked exception, as {@link
 * java.io.UncheckedIOException} does for an {@link java.io.IOException}. The synchronous client
 * throws it for the {@link OxiaException}s that a method does not declare, e.g. a {@link
 * SessionDoesNotExistException} from {@code put()}.
 */
public class OxiaUncheckedException extends RuntimeException {
    /**
     * Creates an instance of the exception.
     *
     * @param cause the {@link OxiaException} to wrap
     */
    public OxiaUncheckedException(OxiaException cause) {
        super(Objects.requireNonNull(cause, "cause").getMessage(), cause);
    }

    /**
     * Returns the wrapped {@link OxiaException}.
     *
     * @return the wrapped {@link OxiaException}
     */
    @Override
    public synchronized OxiaException getCause() {
        return (OxiaException) super.getCause();
    }
}
