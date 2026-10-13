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

/**
 * The server rejected an operation for an invalid argument, and did not apply it. For instance:
 *
 * <ul>
 *   <li>a sequential put that doesn't fit the keys already under its prefix: the last one is not a
 *       key of its sequence, or the deltas overflow the sequence;
 *   <li>a put with a secondary index name that contains {@code /};
 *   <li>a delete range that covers both regular keys and the server's internal keys ({@code
 *       __oxia/}).
 * </ul>
 */
public class InvalidArgumentException extends OxiaException {

    /**
     * Creates an instance of the exception.
     *
     * @param message the exception message
     */
    public InvalidArgumentException(String message) {
        super(message);
    }
}
