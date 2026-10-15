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
/**
 * Checked exceptions raised by the Oxia client.
 *
 * <p>All client-side errors extend the abstract {@link
 * io.oxia.client.api.exceptions.OxiaException}. The synchronous client throws them directly; the
 * asynchronous client surfaces them by completing the returned {@link
 * java.util.concurrent.CompletableFuture} exceptionally.
 *
 * <ul>
 *   <li>{@link io.oxia.client.api.exceptions.UnexpectedVersionIdException} — a conditional put or
 *       delete failed because the server's versionId does not match the expected one.
 *   <li>{@link io.oxia.client.api.exceptions.KeyAlreadyExistsException} — a put with {@code
 *       IfRecordDoesNotExist} found an existing record.
 *   <li>{@link io.oxia.client.api.exceptions.SessionDoesNotExistException} — an operation tied to
 *       an ephemeral session was attempted after the session expired.
 *   <li>{@link io.oxia.client.api.exceptions.InvalidArgumentException} — the server rejected a put
 *       or delete range for an invalid argument, such as a sequential put that doesn't fit its
 *       sequence.
 *   <li>{@link io.oxia.client.api.exceptions.UnsupportedAuthenticationException} — the configured
 *       authentication plugin could not be loaded.
 *   <li>{@link io.oxia.client.api.exceptions.ResourceUnavailableException} — the operation failed
 *       while the server was unavailable. The outcome of a write that fails with it is unknown: it
 *       may have been applied.
 *   <li>{@link io.oxia.client.api.exceptions.RequestTimeoutException} — the operation did not
 *       complete within the request timeout. The outcome of a write that fails with it is unknown.
 *   <li>{@link io.oxia.client.api.exceptions.NamespaceNotFoundException} — the namespace of the
 *       client does not exist.
 *   <li>{@link io.oxia.client.api.exceptions.OxiaServerException} — any other error reported by the
 *       server or the client transport.
 * </ul>
 */
package io.oxia.client.api.exceptions;
