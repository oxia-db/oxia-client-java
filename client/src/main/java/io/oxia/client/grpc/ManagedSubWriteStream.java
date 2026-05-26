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
package io.oxia.client.grpc;

import io.grpc.stub.StreamObserver;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;

final class ManagedSubWriteStream implements StreamObserver<WriteResponse> {

    private final ManagedWriteStream parent;
    private final StreamObserver<WriteRequest> requestObserver;

    ManagedSubWriteStream(
            ManagedWriteStream parent,
            RpcProvider rpcProvider,
            long shardId,
            OxiaStatusException leaderHint) {
        this.parent = parent;
        this.requestObserver = rpcProvider.writeStream(shardId, leaderHint, this);
    }

    void send(WriteRequest request) {
        requestObserver.onNext(request);
    }

    void complete() {
        requestObserver.onCompleted();
    }

    @Override
    public void onNext(WriteResponse value) {
        parent.handleResponse(this, value);
    }

    @Override
    public void onError(Throwable t) {
        parent.handleError(this, t);
    }

    @Override
    public void onCompleted() {
        parent.handleCompleted(this);
    }
}
