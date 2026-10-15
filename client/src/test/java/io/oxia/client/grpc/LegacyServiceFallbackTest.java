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
package io.oxia.client.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.oxia.proto.CreateSessionRequest;
import io.oxia.proto.CreateSessionResponse;
import io.oxia.proto.OxiaClientGrpc;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class LegacyServiceFallbackTest {
    private final AtomicInteger sessions = new AtomicInteger();
    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void testCallsTheServiceOfCurrentServers() throws Exception {
        var stub = start(OxiaClientGrpc.bindService(new FakeOxiaClient()));

        assertEquals(1, createSession(stub));
        assertEquals(2, createSession(stub));
    }

    @Test
    void testSwitchesToTheLegacyServiceOfServersBefore015() throws Exception {
        var stub = start(asLegacyService(OxiaClientGrpc.bindService(new FakeOxiaClient())));

        // The server didn't run the call: it fails with a status that the client retries
        var error = assertThrows(StatusRuntimeException.class, () -> createSession(stub));
        assertEquals(Status.Code.UNAVAILABLE, error.getStatus().getCode());

        assertEquals(1, createSession(stub));
        assertEquals(2, createSession(stub));
    }

    private OxiaClientGrpc.OxiaClientBlockingStub start(ServerServiceDefinition service)
            throws Exception {
        var name = InProcessServerBuilder.generateName();
        server =
                InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        return OxiaClientGrpc.newBlockingStub(
                ClientInterceptors.intercept(channel, new LegacyServiceFallback(name)));
    }

    private static long createSession(OxiaClientGrpc.OxiaClientBlockingStub stub) {
        return stub.createSession(new CreateSessionRequest().setShard(0)).getSessionId();
    }

    /** The same service under its name before Oxia 0.15.0. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ServerServiceDefinition asLegacyService(ServerServiceDefinition service) {
        var legacy = ServerServiceDefinition.builder(LegacyServiceFallback.LEGACY_SERVICE_NAME);
        for (var method : service.getMethods()) {
            var descriptor = method.getMethodDescriptor();
            legacy.addMethod(
                    descriptor.toBuilder()
                            .setFullMethodName(
                                    MethodDescriptor.generateFullMethodName(
                                            LegacyServiceFallback.LEGACY_SERVICE_NAME, descriptor.getBareMethodName()))
                            .build(),
                    (io.grpc.ServerCallHandler) method.getServerCallHandler());
        }
        return legacy.build();
    }

    private class FakeOxiaClient extends OxiaClientGrpc.OxiaClientImplBase {
        @Override
        public void createSession(
                CreateSessionRequest request, StreamObserver<CreateSessionResponse> responseObserver) {
            responseObserver.onNext(new CreateSessionResponse().setSessionId(sessions.incrementAndGet()));
            responseObserver.onCompleted();
        }
    }
}
