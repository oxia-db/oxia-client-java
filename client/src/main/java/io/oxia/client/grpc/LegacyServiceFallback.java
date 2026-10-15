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

import io.github.merlimat.slog.Logger;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.oxia.proto.OxiaClientGrpc;

/**
 * Calls the {@code io.oxia.proto.v1.OxiaClient} service, and switches to its name before Oxia
 * 0.15.0, {@code io.streamnative.oxia.proto.OxiaClient}, when the server doesn't serve it.
 *
 * <p>The call that finds out fails with {@code UNAVAILABLE}, which the client retries: the server
 * answered {@code UNIMPLEMENTED}, so it didn't run the call.
 */
final class LegacyServiceFallback implements ClientInterceptor {
    static final String LEGACY_SERVICE_NAME = "io.streamnative.oxia.proto.OxiaClient";
    private static final Logger log = Logger.get(LegacyServiceFallback.class);

    private final String address;
    private volatile boolean legacy;

    LegacyServiceFallback(String address) {
        this.address = address;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        if (!OxiaClientGrpc.SERVICE_NAME.equals(method.getServiceName())) {
            return next.newCall(method, callOptions);
        }
        if (legacy) {
            return next.newCall(
                    method.toBuilder()
                            .setFullMethodName(
                                    MethodDescriptor.generateFullMethodName(
                                            LEGACY_SERVICE_NAME, method.getBareMethodName()))
                            .build(),
                    callOptions);
        }
        return new SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                super.start(
                        new SimpleForwardingClientCallListener<>(responseListener) {
                            @Override
                            public void onClose(Status status, Metadata trailers) {
                                if (status.getCode() == Status.Code.UNIMPLEMENTED) {
                                    status = switchToLegacy(status);
                                }
                                super.onClose(status, trailers);
                            }
                        },
                        headers);
            }
        };
    }

    private Status switchToLegacy(Status unimplemented) {
        if (!legacy) {
            legacy = true;
            log.warn()
                    .attr("address", address)
                    .attr("service", LEGACY_SERVICE_NAME)
                    .log(
                            "The server doesn't serve io.oxia.proto.v1.OxiaClient, switching to its legacy name");
        }
        return Status.UNAVAILABLE
                .withDescription("The server only serves " + LEGACY_SERVICE_NAME + ", retrying with it")
                .withCause(unimplemented.asRuntimeException());
    }
}
