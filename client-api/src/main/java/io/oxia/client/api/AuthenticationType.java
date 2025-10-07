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

/**
 * Enum representing the type of authentication used for accessing resources.
 *
 * <p>This enum defines the supported authentication methods that can be used to authorize and
 * authenticate a client or request against a service.
 */
public enum AuthenticationType {
    /**
     * Represents the Bearer authentication method.
     *
     * <p>Bearer is an authentication mechanism that uses a token-based approach where the client
     * includes an access token in each request to provide proof of authorization. This method is
     * commonly used in OAuth 2.0 authentication frameworks.
     */
    Bearer;
}
