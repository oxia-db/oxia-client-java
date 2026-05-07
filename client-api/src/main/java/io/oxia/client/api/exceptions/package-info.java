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
 *   <li>{@link io.oxia.client.api.exceptions.UnsupportedAuthenticationException} — the configured
 *       authentication plugin could not be loaded.
 * </ul>
 */
package io.oxia.client.api.exceptions;
