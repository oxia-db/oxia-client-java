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
package io.oxia.client.grpc;

/** Oxia-specific status codes translated from current ErrorInfo or legacy GRPC statuses. */
public enum OxiaStatusCode {
    ABORTED,
    INVALID_SESSION_TIMEOUT,
    SESSION_NOT_FOUND,
    NAMESPACE_NOT_FOUND,
    SHARD_NOT_FOUND,
    INVALID_TERM,
    INVALID_STATUS,
    NOTIFICATIONS_NOT_ENABLED,
    NODE_IS_NOT_MEMBER,
    NODE_IS_NOT_LEADER,
    NOT_INITIALIZED,
    RESOURCE_CONFLICT,
    RESOURCE_UNAVAILABLE,
    UNKNOWN
}
