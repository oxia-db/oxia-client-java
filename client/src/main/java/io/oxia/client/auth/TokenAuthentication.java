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
package io.oxia.client.auth;

import io.oxia.client.api.Authentication;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class TokenAuthentication implements Authentication {

    private static final String AUTHORIZATION_KEY = "Authorization";
    private static final String BEARER_TYPE = "Bearer";

    private Supplier<String> tokenSupplier;

    // The credentials of the last token, reused while the supplier returns the same token
    private volatile Credentials credentials;

    /** Provide a default constructor for reflection. */
    private TokenAuthentication() {}

    public TokenAuthentication(Supplier<String> tokenSupplier) {
        this.tokenSupplier = tokenSupplier;
    }

    public TokenAuthentication(String token) {
        this(() -> token);
    }

    @Override
    public Map<String, String> generateCredentials() {
        String token = tokenSupplier.get();
        Credentials current = credentials;
        if (current == null || !Objects.equals(current.token(), token)) {
            current =
                    new Credentials(
                            token, Collections.singletonMap(AUTHORIZATION_KEY, BEARER_TYPE + " " + token));
            credentials = current;
        }
        return current.headers();
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
            this.tokenSupplier = new FileTokenSupplier(Paths.get(filePath));
        } else {
            throw new IllegalArgumentException("Invalid token configuration: " + encodedAuthParamString);
        }
    }

    private record Credentials(String token, Map<String, String> headers) {}

    /**
     * Reads the token from a file, and reads it again only when the file's modification time changes,
     * e.g. when a Kubernetes service-account token is rotated.
     */
    private static final class FileTokenSupplier implements Supplier<String> {
        private final Path path;
        private volatile FileToken cached;

        private FileTokenSupplier(Path path) {
            this.path = path;
        }

        @Override
        public String get() {
            try {
                // Taken before the read, so that a change made during the read is picked up next time
                FileTime lastModified = Files.getLastModifiedTime(path);
                FileToken current = cached;
                if (current == null || !current.lastModified().equals(lastModified)) {
                    current = new FileToken(lastModified, Files.readString(path).trim());
                    cached = current;
                }
                return current.token();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read token from file", e);
            }
        }

        private record FileToken(FileTime lastModified, String token) {}
    }

    private record TokenSupplier(String token) implements Supplier<String> {

        @Override
        public String get() {
            return token;
        }
    }
}
