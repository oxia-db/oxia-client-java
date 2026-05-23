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
package io.oxia.client.operation.rangescan;

import io.oxia.client.ProtoUtil;
import io.oxia.client.api.RangeScanConsumer;
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.proto.RangeScanResponse;
import lombok.NonNull;

public final class CancelableRangeScanStreamObserver
        extends CancelableStreamObserver<RangeScanResponse> {
    private final RangeScanConsumer delegate;

    public CancelableRangeScanStreamObserver(RangeScanConsumer delegate) {
        this.delegate = delegate;
    }

    @Override
    protected void onNextValue(@NonNull RangeScanResponse response) {
        for (int i = 0; i < response.getRecordsCount(); i++) {
            final boolean needNext =
                    delegate.onNext(ProtoUtil.getResultFromProto("", response.getRecordAt(i)));
            if (!needNext) {
                cancel();
                delegate.onCompleted();
                return;
            }
        }
    }

    @Override
    protected void onErrorValue(@NonNull Throwable t) {
        delegate.onError(t);
    }

    @Override
    protected void onCompletedValue() {
        delegate.onCompleted();
    }
}
