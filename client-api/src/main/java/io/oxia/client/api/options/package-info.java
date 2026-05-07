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
 * Per-operation options for the Oxia client.
 *
 * <p>Every option is a marker or a value type implementing one of the operation-specific marker
 * interfaces ({@link io.oxia.client.api.options.PutOption}, {@link
 * io.oxia.client.api.options.GetOption}, {@link io.oxia.client.api.options.DeleteOption}, {@link
 * io.oxia.client.api.options.DeleteRangeOption}, {@link io.oxia.client.api.options.ListOption},
 * {@link io.oxia.client.api.options.RangeScanOption}, {@link
 * io.oxia.client.api.options.GetSequenceUpdatesOption}). They are passed to client methods as a
 * {@link java.util.Set}, so multiple compatible options can be combined.
 *
 * <h2>Examples</h2>
 *
 * <pre>{@code
 * // Insert only if the key is new.
 * client.put("k", value, Set.of(PutOption.IfRecordDoesNotExist));
 *
 * // Compare-and-swap with a known versionId.
 * client.put("k", value, Set.of(PutOption.IfVersionIdEquals(currentVersionId)));
 *
 * // Co-locate records under the same partition and add a secondary index.
 * client.put("k", value, Set.of(
 *         PutOption.PartitionKey("tenant-1"),
 *         PutOption.SecondaryIndex("by-name", "alice")));
 *
 * // Read using a comparison instead of equality (find the floor key).
 * GetResult floor = client.get("k", Set.of(GetOption.ComparisonFloor));
 *
 * // Read by following a secondary index.
 * GetResult byName = client.get("alice", Set.of(GetOption.UseIndex("by-name")));
 * }</pre>
 *
 * <h2>Common options</h2>
 *
 * <ul>
 *   <li><b>PartitionKey</b> — available on every operation. Routes the call to the shard determined
 *       by {@code partitionKey} instead of the record key, guaranteeing co-location of records that
 *       share the same {@code partitionKey}.
 *   <li><b>UseIndex</b> — available on {@code GetOption}, {@code ListOption} and {@code
 *       RangeScanOption}; follows a secondary index registered via {@code
 *       PutOption.SecondaryIndex}.
 *   <li><b>IfVersionIdEquals / IfRecordDoesNotExist</b> — conditional writes used for optimistic
 *       concurrency control.
 * </ul>
 */
package io.oxia.client.api.options;
