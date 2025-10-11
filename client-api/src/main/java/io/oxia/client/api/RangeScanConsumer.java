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
 * Interface defining a consumer for Range Scan operations, allowing handling of chunked results,
 * errors, or completion signals for a range scan process.
 */
public interface RangeScanConsumer {

    /**
     * Invoked for each record returned by the range scan operation.
     *
     * @param result The GetResult for the record.
     */
    void onNext(GetResult result);

    /**
     * Invoked when an error occurs during the range scan operation.
     *
     * @param throwable the exception that occurred.
     */
    void onError(Throwable throwable);

    /** Invoked when the range scan operation completes. */
    void onCompleted();
}
