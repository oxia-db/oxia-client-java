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

/**
 * The result of a successful put operation.
 *
 * <p>The {@code key} returned here is the effective key stored on the server. It usually matches
 * the key supplied to the put call; with {@link
 * io.oxia.client.api.options.PutOption#SequenceKeysDeltas sequence-key} options the server assigns
 * the key, so callers must use this value to read the record back.
 *
 * @param key the effective key stored in Oxia
 * @param version metadata describing the record after the put was applied (use {@link
 *     Version#versionId()} for subsequent conditional writes)
 */
public record PutResult(String key, Version version) {}
