/*
 * Copyright © 2022-2026 The Oxia Authors
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

import java.util.function.Consumer;

/**
 * A session lifecycle event, delivered to the listener registered through {@link
 * OxiaClientBuilder#sessionListener(Consumer)}.
 *
 * <p>Sessions are created lazily by the client, on the first ephemeral operation that targets a
 * shard, and are re-created after expiring. {@link Type#ESTABLISHED} signals that a session was
 * established with the server. {@link Type#EXPIRED} signals that a previously established session
 * is gone — either the local session timeout elapsed without a successful keep-alive, or the server
 * responded <code>SESSION_NOT_FOUND</code> — so the server has deleted (or will delete) all
 * ephemeral records bound to that session. A session that is closed cleanly, by client shutdown or
 * because its shard moved away from this client, does <em>not</em> produce an {@code EXPIRED}
 * event.
 *
 * <p><b>Threading:</b> events are dispatched on threads owned by the client's internal machinery
 * (including gRPC transport threads), not on a dedicated dispatcher. Events for the same session
 * have a causal order — a session's {@code ESTABLISHED} event is fully delivered before that
 * session's {@code EXPIRED} event — but there is no ordering across different sessions or shards,
 * and the listener may be invoked concurrently from multiple threads. Consumers must be thread-safe
 * and must not block: dispatch happens on threads shared with the client's request machinery,
 * including the gRPC event loop that carries other sessions' heartbeats. Exceptions thrown by the
 * listener are swallowed and logged by the client.
 *
 * @param type The event type.
 * @param sessionId The server-assigned session id.
 * @param clientIdentifier The client identity the session was created with.
 * @param shardId The shard the session belongs to.
 */
public record SessionEvent(Type type, long sessionId, String clientIdentifier, long shardId) {

    /** The type of a {@link SessionEvent}. */
    public enum Type {
        /** A session was established with the server. */
        ESTABLISHED,

        /**
         * A previously established session expired (local timeout or <code>SESSION_NOT_FOUND
         * </code>); the server deletes the ephemeral records bound to it.
         */
        EXPIRED
    }
}
