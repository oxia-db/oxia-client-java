/*
 * Copyright © 2022-2023 StreamNative Inc.
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
package io.streamnative.oxia.client.api;


import lombok.Getter;

/** The versionId at the server did not that match supplied in the call. */
public class UnexpectedVersionIdException extends OxiaException {
    @Getter private final long version;

    /**
     * Creates an instance of the exception.
     *
     * @param version The record versionId to which the call was scoped.
     */
    public UnexpectedVersionIdException(long version) {
        super("unexpected versionId: " + version);
        this.version = version;
    }
}
