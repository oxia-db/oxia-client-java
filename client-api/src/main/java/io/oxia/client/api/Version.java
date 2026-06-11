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
package io.oxia.client.api;

import java.util.Optional;

/**
 * Metadata associated with an Oxia record.
 *
 * <p>The {@code versionId} is the value that drives optimistic concurrency control: pass it back
 * via {@link io.oxia.client.api.options.PutOption#IfVersionIdEquals(long)} or {@link
 * io.oxia.client.api.options.DeleteOption#IfVersionIdEquals(long)} to make a write conditional on
 * the record not having changed since it was read.
 *
 * <p>{@link #sessionId()} and {@link #clientIdentifier()} are present only on ephemeral records —
 * see {@link io.oxia.client.api.options.PutOption#AsEphemeralRecord}.
 *
 * @param versionId the current versionId of the record (monotonically increasing per record)
 * @param createdTimestamp the instant at which the record was first created, in epoch milliseconds
 * @param modifiedTimestamp the instant at which the record was last updated, in epoch milliseconds
 * @param modificationsCount the number of modifications since the record was created
 * @param sessionId for ephemeral records, the session to which the record is scoped
 * @param clientIdentifier for ephemeral records, the client to which the record is scoped
 */
public record Version(
        long versionId,
        long createdTimestamp,
        long modifiedTimestamp,
        long modificationsCount,
        Optional<Long> sessionId,
        Optional<String> clientIdentifier) {}
