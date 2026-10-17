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
package io.oxia.client;

import static io.oxia.proto.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.RangeScanConsumer;
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.proto.RangeScanResponse;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class CompositeRangeScanConsumerTest {

    private final List<String> received = new ArrayList<>();
    private final List<Throwable> errors = new ArrayList<>();
    private int completed = 0;
    private int wanted = Integer.MAX_VALUE;

    private final RangeScanConsumer consumer =
            new RangeScanConsumer() {
                @Override
                public boolean onNext(GetResult result) {
                    received.add(result.key());
                    return received.size() < wanted;
                }

                @Override
                public void onError(Throwable throwable) {
                    errors.add(throwable);
                }

                @Override
                public void onCompleted() {
                    completed++;
                }
            };

    /** A response with the records "primary" or "primary:secondary". */
    private static RangeScanResponse response(String... records) {
        var response = new RangeScanResponse();
        for (String record : records) {
            String[] keys = record.split(":");
            var r = response.addRecord().setStatus(OK).setKey(keys[0]);
            if (keys.length > 1) {
                r.setSecondaryIndexKey(keys[1]);
            }
            r.setVersion().setVersionId(1L);
        }
        return response;
    }

    @Test
    void mergesShardsInKeyOrder() {
        var ordered = new CompositeRangeScanConsumer(consumer, null);
        CancelableStreamObserver<RangeScanResponse> shard0 = ordered.newShard();
        CancelableStreamObserver<RangeScanResponse> shard1 = ordered.newShard();

        shard0.onNext(response("a", "c"));
        // Waits for the other shard before delivering anything
        assertThat(received).isEmpty();

        shard1.onNext(response("b", "d"));
        // "d" waits for the next response of shard 0
        assertThat(received).containsExactly("a", "b", "c");

        shard0.onCompleted();
        assertThat(received).containsExactly("a", "b", "c", "d");
        assertThat(completed).isZero();

        shard1.onCompleted();
        assertThat(completed).isOne();
        assertThat(errors).isEmpty();
    }

    @Test
    void mergesIndexScansBySecondaryKey() {
        var ordered = new CompositeRangeScanConsumer(consumer, null);
        var shard0 = ordered.newShard();
        var shard1 = ordered.newShard();

        shard0.onNext(response("a:bob", "b:dan"));
        shard1.onNext(response("z:ann", "y:cat"));
        shard0.onCompleted();
        shard1.onCompleted();

        assertThat(received).containsExactly("z", "a", "y", "b");
        assertThat(completed).isOne();
    }

    @Test
    void resumesThroughTheFlowControlledConsumer() {
        var started = new ArrayList<FlowControlledRangeScanConsumer.StreamHandle>();
        var idle = new ArrayList<FlowControlledRangeScanConsumer.StreamHandle>();
        var ordered =
                new CompositeRangeScanConsumer(
                        consumer,
                        new FlowControlledRangeScanConsumer() {
                            @Override
                            public void onStreamStarted(StreamHandle handle) {
                                started.add(handle);
                            }

                            @Override
                            public void onStreamIdle(StreamHandle handle) {
                                idle.add(handle);
                            }
                        });
        var shard0 = ordered.newShard();
        var shard1 = ordered.newShard();
        assertThat(started).isEmpty();

        shard0.onNext(response("a"));
        assertThat(started).containsExactly(ordered);
        shard1.onNext(response("b"));
        // Shard 0 needs its next response, which waits for the consumer
        assertThat(received).containsExactly("a");
        assertThat(idle).containsExactly(ordered);

        ordered.requestNext();
        shard0.onNext(response("c"));
        assertThat(received).containsExactly("a", "b");
        assertThat(idle).containsExactly(ordered, ordered);
    }

    @Test
    void completesOnceWhenTheConsumerStops() {
        wanted = 1;
        var ordered = new CompositeRangeScanConsumer(consumer, null);
        var shard0 = ordered.newShard();
        var shard1 = ordered.newShard();

        shard0.onNext(response("a", "c"));
        shard1.onNext(response("b"));
        assertThat(received).containsExactly("a");
        assertThat(completed).isOne();

        shard1.onCompleted();
        assertThat(received).containsExactly("a");
        assertThat(completed).isOne();
    }

    @Test
    void forwardsTheFirstErrorOnly() {
        var ordered = new CompositeRangeScanConsumer(consumer, null);
        var shard0 = ordered.newShard();
        var shard1 = ordered.newShard();

        shard0.onNext(response("a"));
        var error = new RuntimeException("shard 1 failed");
        shard1.onError(error);
        shard0.onCompleted();

        assertThat(received).isEmpty();
        assertThat(errors).containsExactly(error);
        assertThat(completed).isZero();
    }
}
