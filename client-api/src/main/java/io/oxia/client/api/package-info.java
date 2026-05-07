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
 * The public API for interacting with Oxia.
 *
 * <h2>Getting started</h2>
 *
 * Every client is created through {@link io.oxia.client.api.OxiaClientBuilder}. The builder lets
 * you produce either a {@link io.oxia.client.api.SyncOxiaClient blocking} or {@link
 * io.oxia.client.api.AsyncOxiaClient non-blocking} client; both speak to the same Oxia cluster and
 * expose the same operations. Pick whichever fits your code style — if in doubt, start with the
 * synchronous one.
 *
 * <h3>Synchronous client</h3>
 *
 * <pre>{@code
 * try (SyncOxiaClient client = OxiaClientBuilder.create("localhost:6648")
 *         .namespace("my-namespace")
 *         .syncClient()) {
 *
 *     PutResult put = client.put("my-key", "my-value".getBytes(StandardCharsets.UTF_8));
 *
 *     GetResult got = client.get("my-key");
 *     if (got != null) {
 *         String value = new String(got.value(), StandardCharsets.UTF_8);
 *     }
 *
 *     boolean wasPresent = client.delete("my-key");
 * }
 * }</pre>
 *
 * <h3>Asynchronous client</h3>
 *
 * <pre>{@code
 * AsyncOxiaClient client = OxiaClientBuilder.create("localhost:6648")
 *         .namespace("my-namespace")
 *         .asyncClient()
 *         .join();
 *
 * client.put("my-key", "my-value".getBytes(StandardCharsets.UTF_8))
 *         .thenCompose(put -> client.get("my-key"))
 *         .thenAccept(got -> System.out.println(new String(got.value(), StandardCharsets.UTF_8)));
 * }</pre>
 *
 * <h3>Conditional puts and optimistic concurrency</h3>
 *
 * <p>Operations can be made conditional by passing {@link io.oxia.client.api.options options}.
 * Conditional writes that fail produce an {@link
 * io.oxia.client.api.exceptions.UnexpectedVersionIdException}.
 *
 * <pre>{@code
 * // Only insert if the key does not already exist.
 * client.put("my-key", value, Set.of(PutOption.IfRecordDoesNotExist));
 *
 * // Read-modify-write with version check.
 * GetResult current = client.get("my-key");
 * client.put("my-key", newValue, Set.of(PutOption.IfVersionIdEquals(current.version().versionId())));
 * }</pre>
 *
 * <h3>Listing and scanning ranges</h3>
 *
 * <pre>{@code
 * List<String> keys = client.list("a", "z");
 *
 * try (CloseableIterable<GetResult> scan = client.rangeScan("a", "z")) {
 *     for (GetResult r : scan) {
 *         // process r
 *     }
 * }
 * }</pre>
 *
 * <h3>Watching for changes</h3>
 *
 * <pre>{@code
 * client.notifications(n -> {
 *     switch (n) {
 *         case Notification.KeyCreated c  -> { /* ... *\/ }
 *         case Notification.KeyModified m -> { /* ... *\/ }
 *         case Notification.KeyDeleted d  -> { /* ... *\/ }
 *         case Notification.KeyRangeDelete r -> { /* ... *\/ }
 *     }
 * });
 * }</pre>
 *
 * <h2>Package layout</h2>
 *
 * <ul>
 *   <li>{@link io.oxia.client.api} — client interfaces, builder, and result/notification types.
 *   <li>{@link io.oxia.client.api.options} — operation options ({@code PutOption}, {@code
 *       GetOption}, ...) passed as a {@link java.util.Set} to each call.
 *   <li>{@link io.oxia.client.api.exceptions} — checked exceptions raised by client operations.
 * </ul>
 *
 * @see io.oxia.client.api.OxiaClientBuilder
 * @see io.oxia.client.api.SyncOxiaClient
 * @see io.oxia.client.api.AsyncOxiaClient
 */
package io.oxia.client.api;
