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

/** The operation was attempted on a client that has been closed, and was not sent. */
public class ClientClosedException extends OxiaException {
    /**
     * Creates an instance of the exception.
     *
     * @param message the exception message
     * @param cause the cause of the exception
     */
    public ClientClosedException(String message, Throwable cause) {
        super(message, cause);
    }
}
