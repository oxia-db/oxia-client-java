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
package io.oxia.client.shard;

import lombok.Getter;
import lombok.NonNull;

/** The namespace not found in shards assignments. */
public class NamespaceNotFoundException extends RuntimeException {
    @Getter private final String namespace;
    @Getter private final boolean retryable;

    /**
     * Creates an instance of the exception.
     *
     * @param namespace The namespace specified in the call.
     */
    public NamespaceNotFoundException(@NonNull String namespace) {
        super(String.format("namespace %s not found in shards assignments", namespace));
        this.retryable = false;
        this.namespace = namespace;
    }

    /**
     * Creates a retryable instance of the exception.
     *
     * @param namespace The namespace specified in the call.
     * @param retryable If the exception is retryable
     */
    public NamespaceNotFoundException(@NonNull String namespace, boolean retryable) {
        super(String.format("namespace %s not found in shards assignments", namespace));
        this.retryable = retryable;
        this.namespace = namespace;
    }
}
