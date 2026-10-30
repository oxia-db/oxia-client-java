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

import static io.oxia.client.grpc.LegacyServiceInterceptor.LEGACY_SERVICE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.HandlerRegistry;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.oxia.proto.CreateSessionRequest;
import io.oxia.proto.CreateSessionResponse;
import io.oxia.proto.OxiaClientGrpc;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class LegacyServiceInterceptorTest {
    private static final Status UNKNOWN_SERVICE =
            Status.UNIMPLEMENTED.withDescription("unknown service " + OxiaClientGrpc.SERVICE_NAME);
    private static final MethodDescriptor.Marshaller<byte[]> BYTES =
            new MethodDescriptor.Marshaller<>() {
                @Override
                public InputStream stream(byte[] value) {
                    return new ByteArrayInputStream(value);
                }

                @Override
                public byte[] parse(InputStream stream) {
                    try {
                        return stream.readAllBytes();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };

    private final List<String> calledMethods = new CopyOnWriteArrayList<>();
    private final LegacyServiceInterceptor interceptor = new LegacyServiceInterceptor();
    private Status unknownMethodStatus = UNKNOWN_SERVICE;
    private Server server;
    private ManagedChannel managedChannel;
    private Channel channel;

    @BeforeEach
    void setUp() throws IOException {
        var name = InProcessServerBuilder.generateName();
        server =
                InProcessServerBuilder.forName(name)
                        .directExecutor()
                        .addService(legacyService())
                        .fallbackHandlerRegistry(unknownMethods())
                        .build()
                        .start();
        managedChannel = InProcessChannelBuilder.forName(name).directExecutor().build();
        channel = ClientInterceptors.intercept(managedChannel, interceptor);
    }

    @AfterEach
    void tearDown() {
        managedChannel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void fallsBackToLegacyServiceAfterUnknownService() {
        assertThatThrownBy(this::createSession)
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED));
        assertThat(interceptor.isLegacy()).isTrue();

        assertThat(createSession().getSessionId()).isEqualTo(1);
        assertThat(createSession().getSessionId()).isEqualTo(1);

        assertThat(calledMethods)
                .containsExactly(
                        OxiaClientGrpc.getCreateSessionMethod().getFullMethodName(),
                        LEGACY_SERVICE_NAME + "/CreateSession",
                        LEGACY_SERVICE_NAME + "/CreateSession");
    }

    @Test
    void switchesOnceForConcurrentFailures() {
        var first = channel.newCall(OxiaClientGrpc.getCreateSessionMethod(), CallOptions.DEFAULT);
        var second = channel.newCall(OxiaClientGrpc.getCreateSessionMethod(), CallOptions.DEFAULT);

        var firstResult = ClientCalls.futureUnaryCall(first, new CreateSessionRequest());
        var secondResult = ClientCalls.futureUnaryCall(second, new CreateSessionRequest());

        assertThatThrownBy(firstResult::get).isInstanceOf(ExecutionException.class);
        assertThatThrownBy(secondResult::get).isInstanceOf(ExecutionException.class);
        assertThat(interceptor.isLegacy()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("otherFailures")
    void keepsServiceOnOtherFailures(Status status) {
        unknownMethodStatus = status;

        assertThatThrownBy(this::createSession).isInstanceOf(StatusRuntimeException.class);

        assertThat(interceptor.isLegacy()).isFalse();
    }

    @Test
    void passesOtherServicesThrough() {
        var health = HealthGrpc.newBlockingStub(channel);

        assertThatThrownBy(() -> health.check(HealthCheckRequest.getDefaultInstance()))
                .isInstanceOf(StatusRuntimeException.class);

        assertThat(interceptor.isLegacy()).isFalse();
        assertThat(calledMethods).containsExactly(HealthGrpc.getCheckMethod().getFullMethodName());
    }

    static Stream<Status> otherFailures() {
        return Stream.of(
                Status.UNIMPLEMENTED,
                Status.UNIMPLEMENTED.withDescription("Method not found: " + OxiaClientGrpc.SERVICE_NAME),
                Status.UNAVAILABLE.withDescription(UNKNOWN_SERVICE.getDescription()));
    }

    private CreateSessionResponse createSession() {
        return ClientCalls.blockingUnaryCall(
                channel,
                OxiaClientGrpc.getCreateSessionMethod(),
                CallOptions.DEFAULT,
                new CreateSessionRequest());
    }

    private ServerServiceDefinition legacyService() {
        var method =
                OxiaClientGrpc.getCreateSessionMethod().toBuilder()
                        .setFullMethodName(
                                MethodDescriptor.generateFullMethodName(LEGACY_SERVICE_NAME, "CreateSession"))
                        .build();
        return ServerServiceDefinition.builder(LEGACY_SERVICE_NAME)
                .addMethod(
                        method,
                        ServerCalls.asyncUnaryCall(
                                (request, response) -> {
                                    calledMethods.add(method.getFullMethodName());
                                    response.onNext(new CreateSessionResponse().setSessionId(1));
                                    response.onCompleted();
                                }))
                .build();
    }

    private HandlerRegistry unknownMethods() {
        return new HandlerRegistry() {
            @Override
            public ServerMethodDefinition<?, ?> lookupMethod(String methodName, String authority) {
                var method =
                        MethodDescriptor.<byte[], byte[]>newBuilder()
                                .setType(MethodDescriptor.MethodType.UNKNOWN)
                                .setFullMethodName(methodName)
                                .setRequestMarshaller(BYTES)
                                .setResponseMarshaller(BYTES)
                                .build();
                return ServerMethodDefinition.create(
                        method,
                        (ServerCall<byte[], byte[]> call, Metadata headers) -> {
                            calledMethods.add(methodName);
                            call.close(unknownMethodStatus, new Metadata());
                            return new ServerCall.Listener<>() {};
                        });
            }
        };
    }
}
