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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.client.shard.NoShardAvailableException;
import io.oxia.proto.GetSequenceUpdatesRequest;
import io.oxia.proto.GetSequenceUpdatesResponse;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class GrpcRpcProviderTest {
    @Test
    void getSequenceUpdatesReportsLookupFailure() throws Exception {
        var executor = Executors.newSingleThreadScheduledExecutor();
        var config =
                ((OxiaClientBuilderImpl) OxiaClientBuilder.create("localhost:0")).getClientConfig();

        try (var provider =
                new GrpcRpcProvider(
                        config,
                        executor,
                        shardId -> {
                            throw new NoShardAvailableException(shardId);
                        })) {
            var request = new GetSequenceUpdatesRequest();
            request.setShard(1).setKey("key");
            var error = new AtomicReference<Throwable>();

            provider.getSequenceUpdates(
                    request,
                    new CancelableStreamObserver<>() {
                        @Override
                        public void onNext(GetSequenceUpdatesResponse value) {}

                        @Override
                        public void onError(Throwable throwable) {
                            error.set(throwable);
                        }

                        @Override
                        public void onCompleted() {}
                    });

            await()
                    .untilAsserted(
                            () -> assertThat(error.get()).isInstanceOf(NoShardAvailableException.class));
        } finally {
            executor.shutdownNow();
        }
    }
}
