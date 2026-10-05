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

import lombok.Getter;

/**
 * The versionId at the server did not that match supplied in the call.
 *
 * <p>The exception has no stack trace: it is an expected outcome of a conditional put or delete,
 * created on the client's I/O threads, where the trace would only show transport frames.
 */
public class UnexpectedVersionIdException extends OxiaException {
    /** The record versionId present at the server. */
    @Getter private final long version;

    /** The key to which the call was scoped. */
    @Getter private final String key;

    /**
     * Creates an instance of the exception.
     *
     * @param key The key to which the call was scoped.
     * @param version The record versionId to which the call was scoped.
     */
    public UnexpectedVersionIdException(String key, long version) {
        super("key '" + key + "' has unexpected versionId (expected " + version + ")", false);
        this.version = version;
        this.key = key;
    }
}
