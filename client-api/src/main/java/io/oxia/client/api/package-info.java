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

/**
 * The public API for interacting with Oxia.
 *
 * <p>
 *
 * <pre>
 *     SyncOxiaClient client = OxiaClientBuilder.create("localhost:6648")
 *         .namespace("my-namespace")
 *         .syncClient();
 *
 *     PutResult res = client.put("my-key", "my-value".getBytes());
 *
 *     GetResult res = client.get("my-key");
 * </pre>
 *
 * @see io.oxia.client.api.OxiaClientBuilder
 */
package io.oxia.client.api;
