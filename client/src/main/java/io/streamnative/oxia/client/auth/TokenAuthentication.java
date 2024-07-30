/*
 * Copyright © 2022-2024 StreamNative Inc.
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
package io.streamnative.oxia.client.auth;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Metadata;
import io.streamnative.oxia.client.api.Authentication;
import io.streamnative.oxia.client.api.EncodedAuthenticationParameterSupport;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Supplier;

public class TokenAuthentication implements Authentication, EncodedAuthenticationParameterSupport {

    private static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY =
            Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);
    private static final String BEARER_TYPE = "Bearer";

    private Supplier<String> tokenSupplier;

    /** Provide a default constructor for reflection. */
    private TokenAuthentication() {}

    public TokenAuthentication(Supplier<String> tokenSupplier) {
        this.tokenSupplier = tokenSupplier;
    }

    public TokenAuthentication(String token) {
        this(() -> token);
    }

    @Override
    public Metadata generateCredentials() {
        Metadata credentials = new Metadata();
        credentials.put(
                AUTHORIZATION_METADATA_KEY, String.format("%s %s", BEARER_TYPE, tokenSupplier.get()));
        return credentials;
    }

    @Override
    public void configure(String encodedAuthParamString) {
        // Interpret the whole param string as the token. If the string contains the notation
        // `token:xxxxx` then strip
        // the prefix
        if (encodedAuthParamString.startsWith("token:")) {
            this.tokenSupplier = new TokenSupplier(encodedAuthParamString.substring("token:".length()));
        } else if (encodedAuthParamString.startsWith("file:")) {
            // Read token from a file
            URI filePath = URI.create(encodedAuthParamString);
            this.tokenSupplier = new URITokenSupplier(filePath);
        } else {
            throw new IllegalArgumentException("Invalid token configuration: " + encodedAuthParamString);
        }
    }

    private record URITokenSupplier(URI uri) implements Supplier<String> {

        @Override
        public String get() {
            try {
                return Files.readString(Paths.get(uri)).trim();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read token from file", e);
            }
        }
    }

    private record TokenSupplier(String token) implements Supplier<String> {

        @Override
        public String get() {
            return token;
        }
    }
}
