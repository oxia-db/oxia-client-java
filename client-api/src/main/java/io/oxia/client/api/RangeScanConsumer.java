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
 * Callback used by {@link AsyncOxiaClient#rangeScan(String, String, RangeScanConsumer)} to deliver
 * records, errors, and the completion signal for a streaming range scan.
 *
 * <p>Exactly one of {@link #onError(Throwable)} or {@link #onCompleted()} is invoked per scan, and
 * always after the final {@link #onNext(GetResult)} call. Returning {@code false} from {@code
 * onNext} stops iteration early; the underlying server stream is cancelled and {@link
 * #onCompleted()} is invoked once.
 *
 * <pre>{@code
 * client.rangeScan("a", "z", new RangeScanConsumer() {
 *     public boolean onNext(GetResult result) {
 *         process(result);
 *         return true; // return false to abort iteration early
 *     }
 *     public void onError(Throwable t) { log.warn("scan failed", t); }
 *     public void onCompleted() { log.info("scan finished"); }
 * });
 * }</pre>
 *
 * <p>For a synchronous, iterator-style alternative, see {@link SyncOxiaClient#rangeScan(String,
 * String)} which returns a {@link CloseableIterable}.
 */
public interface RangeScanConsumer {

    /**
     * Invoked for each record returned by the range scan operation.
     *
     * @param result The GetResult for the record.
     * @return {@code true} to keep receiving records, {@code false} to stop the iteration. When
     *     {@code false} is returned, the underlying server stream is cancelled, no further {@link
     *     #onNext} invocations will be made, and {@link #onCompleted()} will be invoked once.
     */
    boolean onNext(GetResult result);

    /**
     * Invoked when an error occurs during the range scan operation.
     *
     * @param throwable the exception that occurred.
     */
    void onError(Throwable throwable);

    /** Invoked when the range scan operation completes. */
    void onCompleted();
}
