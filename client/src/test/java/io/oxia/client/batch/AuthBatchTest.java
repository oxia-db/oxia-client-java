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

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.oxia.client.auth.TokenAuthentication;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuthBatchTest extends BatchTest {

    public static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY =
            Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);

    static {
        authentication = new TokenAuthentication("123");
        serverInterceptor = new AuthInterceptor();
    }

    static class AuthInterceptor implements ServerInterceptor {

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata metadata, ServerCallHandler<ReqT, RespT> handler) {
            String token = metadata.get(AUTHORIZATION_METADATA_KEY);
            if (!"Bearer 123".equals(token)) {
                call.close(Status.UNAUTHENTICATED.withDescription("Token is wrong"), new Metadata());
                return new ServerCall.Listener<ReqT>() {};
            }
            return handler.startCall(call, metadata);
        }
    }
}
