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
package io.oxia.client.api.exceptions;

/** Unsupported authentication exception thrown by Oxia client. */
public class UnsupportedAuthenticationException extends OxiaException {
    /**
     * Constructs an {@code UnsupportedAuthenticationException} with the specified detail message.
     *
     * @param msg The detail message (which is saved for later retrieval by the {@link #getMessage()}
     *     method)
     */
    public UnsupportedAuthenticationException(String msg) {
        super(msg);
    }

    /**
     * Constructs an {@code UnsupportedAuthenticationException} with the specified detail message and
     * cause.
     *
     * @param msg The detail message (which is saved for later retrieval by the {@link #getMessage()}
     *     method)
     * @param cause The cause (which is saved for later retrieval by the {@link #getCause()} method).
     *     (A {@code null} value is permitted, and indicates that the cause is nonexistent or
     *     unknown.)
     */
    public UnsupportedAuthenticationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
