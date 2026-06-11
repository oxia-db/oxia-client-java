/*
 * Copyright © 2026 The Oxia Authors
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
package io.oxia.client;

/**
 * Internal contract implemented by {@link io.oxia.client.api.RangeScanConsumer}s that consume
 * records on demand. When the consumer passed to a range scan implements this interface, the
 * underlying gRPC streams are switched to manual flow control: after the records of a response have
 * been delivered, the stream is paused until the consumer requests the next response through the
 * stream handle. This lets a slow consumer apply backpressure to the server instead of blocking the
 * gRPC transport threads.
 */
interface FlowControlledRangeScanConsumer {

    /** Invoked once per underlying gRPC stream, after the stream has been started. */
    void onStreamStarted(StreamHandle handle);

    /**
     * Invoked after all the records of a response have been delivered. The stream will not deliver
     * further responses until {@link StreamHandle#requestNext()} is invoked.
     */
    void onStreamIdle(StreamHandle handle);

    /** Control over a single underlying gRPC stream. Implementations are thread-safe. */
    interface StreamHandle {
        /** Requests the delivery of the next response on this stream. */
        void requestNext();

        /** Cancels this stream. */
        void cancel();
    }
}
