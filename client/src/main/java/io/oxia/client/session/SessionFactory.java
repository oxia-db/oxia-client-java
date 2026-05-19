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
package io.oxia.client.session;

import static lombok.AccessLevel.PACKAGE;

import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.proto.CreateSessionRequest;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PACKAGE)
public class SessionFactory {
    @NonNull private final ScheduledExecutorService executor;
    @NonNull final ClientConfig config;

    @NonNull final SessionNotificationListener listener;

    @NonNull final RpcProvider rpcProvider;

    @NonNull final InstrumentProvider instrumentProvider;

    @NonNull
    CompletableFuture<Session> create(long shardId) {
        var request = new CreateSessionRequest();
        request
                .setSessionTimeoutMs((int) config.sessionTimeout().toMillis())
                .setShard(shardId)
                .setClientIdentity(config.clientIdentifier());

        return rpcProvider
                .createSession(request)
                .thenApply(
                        response ->
                                new Session(
                                        executor,
                                        rpcProvider,
                                        config,
                                        shardId,
                                        response.getSessionId(),
                                        instrumentProvider,
                                        listener));
    }
}
