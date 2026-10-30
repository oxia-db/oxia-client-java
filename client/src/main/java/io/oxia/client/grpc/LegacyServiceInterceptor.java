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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

final class LegacyServiceInterceptor implements ClientInterceptor {
    static final String LEGACY_SERVICE_NAME = "io.streamnative.oxia.proto.OxiaClient";
    private static final String UNKNOWN_SERVICE = "unknown service " + OxiaClientGrpc.SERVICE_NAME;
    private static final Logger log = Logger.get(LegacyServiceInterceptor.class);

    private final AtomicBoolean legacy = new AtomicBoolean();
    private final Map<String, MethodDescriptor<?, ?>> legacyMethods = new ConcurrentHashMap<>();

    boolean isLegacy() {
        return legacy.get();
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        if (!OxiaClientGrpc.SERVICE_NAME.equals(method.getServiceName())) {
            return next.newCall(method, callOptions);
        }
        if (legacy.get()) {
            return next.newCall(toLegacy(method), callOptions);
        }
        return new SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                super.start(
                        new SimpleForwardingClientCallListener<>(responseListener) {
                            @Override
                            public void onClose(Status status, Metadata trailers) {
                                if (isUnknownService(status) && legacy.compareAndSet(false, true)) {
                                    log.warn()
                                            .attr("service", OxiaClientGrpc.SERVICE_NAME)
                                            .attr("fallback", LEGACY_SERVICE_NAME)
                                            .log("Server does not implement the gRPC service, falling back");
                                }
                                super.onClose(status, trailers);
                            }
                        },
                        headers);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private <ReqT, RespT> MethodDescriptor<ReqT, RespT> toLegacy(
            MethodDescriptor<ReqT, RespT> method) {
        return (MethodDescriptor<ReqT, RespT>)
                legacyMethods.computeIfAbsent(
                        method.getFullMethodName(),
                        name ->
                                method.toBuilder()
                                        .setFullMethodName(
                                                MethodDescriptor.generateFullMethodName(
                                                        LEGACY_SERVICE_NAME, method.getBareMethodName()))
                                        .build());
    }

    private static boolean isUnknownService(Status status) {
        return status.getCode() == Status.Code.UNIMPLEMENTED
                && status.getDescription() != null
                && status.getDescription().contains(UNKNOWN_SERVICE);
    }
}
