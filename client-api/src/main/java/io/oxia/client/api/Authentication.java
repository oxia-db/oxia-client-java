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
package io.oxia.client.api;

import java.util.Map;

/**
 * Represents an interface for implementing authentication mechanisms. The implementations of this
 * interface are expected to provide a way to generate credentials in the form of key-value pairs.
 */
public interface Authentication {

    /**
     * Generates a set of credentials represented as key-value pairs.
     *
     * @return a map containing the generated credentials, where the keys represent the credential
     *     type (e.g., username, password) and the values represent the corresponding credential
     *     values.
     */
    Map<String, String> generateCredentials();
}
