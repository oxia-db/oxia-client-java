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

import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.DeleteRangeOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.GetSequenceUpdatesOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/** Synchronous client for the Oxia service. */
public interface SyncOxiaClient extends AutoCloseable {

    /**
     * Associates a value with a key if the server's versionId of the record is as specified, at the
     * instant when the put is applied.
     *
     * @param key The key with which the value should be associated.
     * @param value The value to associate with the key.
     * @return The result of the put at the specified key.
     */
    PutResult put(String key, byte[] value);

    /**
     * Conditionally associates a value with a key if the server's versionId of the record is as
     * specified, at the instant when the put is applied. The put will not be applied if the server's
     * versionId of the record does not match the expectation set in the call.
     *
     * @param key The key with which the value should be associated.
     * @param value The value to associate with the key.
     * @param options Set {@link PutOption options} for the put.
     * @return The result of the put at the specified key.
     * @throws UnexpectedVersionIdException The versionId at the server did not that match supplied in
     *     the call.
     */
    PutResult put(String key, byte[] value, Set<PutOption> options)
            throws UnexpectedVersionIdException;

    /**
     * Unconditionally deletes the record associated with the key if the record exists.
     *
     * @param key Deletes the record with the specified key.
     * @return True if the key was actually present on the server, false otherwise.
     */
    boolean delete(String key);

    /**
     * Conditionally deletes the record associated with the key if the record exists, and the server's
     * versionId of the record is as specified, at the instant when the delete is applied. The delete
     * will not be applied if the server's versionId of the record does not match the expectation set
     * in the call.
     *
     * @param key Deletes the record with the specified key.
     * @param options Set {@link DeleteOption options} for the delete.
     * @return True if the key was actually present on the server, false otherwise.
     * @throws UnexpectedVersionIdException The versionId at the server did not that match supplied in
     *     the call.
     */
    boolean delete(String key, Set<DeleteOption> options) throws UnexpectedVersionIdException;

    /**
     * Deletes any records with keys within the specified range. For more information on how keys are
     * sorted, check the relevant section in the <a
     * href="https://github.com/streamnative/oxia/blob/main/docs/oxia-key-sorting.md">Oxia
     * documentation</a>.
     *
     * @param startKeyInclusive The key that declares start of the range, and is <b>included</b> from
     *     the range.
     * @param endKeyExclusive The key that declares the end of the range, and is <b>excluded</b> from
     *     the range.
     */
    void deleteRange(String startKeyInclusive, String endKeyExclusive);

    /**
     * Deletes any records with keys within the specified range. For more information on how keys are
     * sorted, check the relevant section in the <a
     * href="https://github.com/streamnative/oxia/blob/main/docs/oxia-key-sorting.md">Oxia
     * documentation</a>.
     *
     * @param startKeyInclusive The key that declares start of the range, and is <b>included</b> from
     *     the range.
     * @param endKeyExclusive The key that declares the end of the range, and is <b>excluded</b> from
     *     the range.
     * @param options Set {@link DeleteRangeOption options} for the delete range.
     */
    void deleteRange(
            String startKeyInclusive, String endKeyExclusive, Set<DeleteRangeOption> options);

    /**
     * Returns the record associated with the specified key. The returned value includes the value,
     * and other metadata.
     *
     * @param key The key associated with the record to be fetched.
     * @return The value associated with the supplied key, or {@code null} if the key did not exist.
     */
    GetResult get(String key);

    /**
     * Returns the record associated with the specified key. The returned value includes the value,
     * and other metadata.
     *
     * @param key The key associated with the record to be fetched.
     * @param options Set {@link GetOption options} for the get operation.
     * @return The value associated with the supplied key, or {@code null} if the key did not exist.
     */
    GetResult get(String key, Set<GetOption> options);

    /**
     * Lists any existing keys within the specified range. For more information on how keys are
     * sorted, check the relevant section in the <a
     * href="https://github.com/streamnative/oxia/blob/main/docs/oxia-key-sorting.md">Oxia
     * documentation</a>.
     *
     * @param startKeyInclusive The key that declares start of the range, and is <b>included</b> from
     *     the range.
     * @param endKeyExclusive The key that declares the end of the range, and is <b>excluded</b> from
     *     the range.
     * @return The list of keys that exist within the specified range, or an empty list if there were
     *     none.
     */
    List<String> list(String startKeyInclusive, String endKeyExclusive);

    /**
     * Lists any existing keys within the specified range. For more information on how keys are
     * sorted, check the relevant section in the <a
     * href="https://github.com/streamnative/oxia/blob/main/docs/oxia-key-sorting.md">Oxia
     * documentation</a>.
     *
     * @param startKeyInclusive The key that declares start of the range, and is <b>included</b> from
     *     the range.
     * @param endKeyExclusive The key that declares the end of the range, and is <b>excluded</b> from
     *     the range.
     * @param options Set {@link ListOption options} for the list operation.
     * @return The list of keys that exist within the specified range, or an empty list if there were
     *     none.
     */
    List<String> list(String startKeyInclusive, String endKeyExclusive, Set<ListOption> options);

    /**
     * Scan any existing records within the specified range of keys.
     *
     * @param startKeyInclusive The key that declares start of the range, and is <b>included</b> from
     *     the range.
     * @param endKeyExclusive The key that declares the end of the range, and is <b>excluded</b> from
     *     the range.
     * @return An iterable object that will provide all the records and their version objects.
     */
    Iterable<GetResult> rangeScan(String startKeyInclusive, String endKeyExclusive);

    /**
     * Scan any existing records within the specified range of keys.
     *
     * @param startKeyInclusive The key that declares start of the range, and is <b>included</b> from
     *     the range.
     * @param endKeyExclusive The key that declares the end of the range, and is <b>excluded</b> from
     *     the range.
     * @param options the range scan options
     * @return An iterable object that will provide all the records and their version objects.
     */
    Iterable<GetResult> rangeScan(
            String startKeyInclusive, String endKeyExclusive, Set<RangeScanOption> options);

    /**
     * Registers a callback to receive Oxia {@link Notification record change notifications}. Multiple
     * callbacks can be registered.
     *
     * @param notificationCallback A callback to receive notifications.
     */
    void notifications(Consumer<Notification> notificationCallback);

    /**
     * GetSequenceUpdates allows to subscribe to the updates happening on a sequential key The channel
     * will report the current latest sequence for a given key. Multiple updates can be collapsed into
     * one single event with the highest sequence.
     *
     * @param key The key to subscribe to.
     * @param listener The listener to receive the updates.
     * @param options The options to use for the subscription.
     * @return A handle to close the subscription.
     */
    Closeable getSequenceUpdates(
            String key, Consumer<String> listener, Set<GetSequenceUpdatesOption> options);
}
