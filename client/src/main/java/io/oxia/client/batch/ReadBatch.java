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
package io.oxia.client.batch;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.StreamObserver;
import io.oxia.client.grpc.OxiaStubProvider;
import io.oxia.proto.GetResponse;
import io.oxia.proto.ReadRequest;
import io.oxia.proto.ReadResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import lombok.NonNull;

final class ReadBatch extends BatchBase implements Batch, StreamObserver<ReadResponse> {

    private final ReadBatchFactory factory;

    @VisibleForTesting final List<Operation.ReadOperation.GetOperation> gets = new ArrayList<>();

    private int responseIndex = 0;
    long startSendTimeNanos;

    ReadBatch(ReadBatchFactory factory, OxiaStubProvider stubProvider, long shardId) {
        super(stubProvider, shardId);
        this.factory = factory;
    }

    @Override
    public boolean canAdd(@NonNull Operation<?> operation) {
        return true;
    }

    public void add(@NonNull Operation<?> operation) {
        if (operation instanceof Operation.ReadOperation.GetOperation g) {
            gets.add(g);
        }
    }

    @Override
    public int size() {
        return gets.size();
    }

    @Override
    public void send() {
        startSendTimeNanos = System.nanoTime();
        try {
            getStub().async().read(toProto(), this);
        } catch (Throwable t) {
            onError(t);
        }
    }

    @Override
    public void onNext(ReadResponse response) {
        for (int i = 0; i < response.getGetsCount(); i++) {
            GetResponse gr = response.getGets(i);
            gets.get(responseIndex).complete(gr);

            ++responseIndex;
        }
    }

    @Override
    public void onError(Throwable batchError) {
        gets.forEach(g -> g.fail(batchError));
        factory.getReadRequestLatencyHistogram().recordFailure(System.nanoTime() - startSendTimeNanos);
    }

    @Override
    public void onCompleted() {
        // complete pending request if the server close stream without any response
        gets.forEach(
                g -> {
                    if (!g.callback().isDone()) {
                        g.fail(new CancellationException());
                    }
                });
        factory.getReadRequestLatencyHistogram().recordSuccess(System.nanoTime() - startSendTimeNanos);
    }

    @NonNull
    ReadRequest toProto() {
        return ReadRequest.newBuilder()
                .setShard(getShardId())
                .addAllGets(gets.stream().map(Operation.ReadOperation.GetOperation::toProto).toList())
                .build();
    }
}
