/*
 * Copyright © 2022-2025 StreamNative Inc.
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

/** A super-class of exceptions describing errors that occurred on an Oxia server. */
public abstract class OxiaException extends Exception {
    /**
     * Creates an instance of the exception.
     *
     * @param message the exception message
     */
    OxiaException(String message) {
        super(message);
    }

    /**
     * Creates an instance of the exception.
     *
     * @param message the exception message
     * @param cause the cause of the exception
     */
    OxiaException(String message, Throwable cause) {
        super(message, cause);
    }
}
