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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.oxia.client.api.Authentication;
import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TokenAuthenticationTest {

    @Test
    void testTokenAuthentication() {
        TokenAuthentication tokenAuthentication = new TokenAuthentication("1234");
        Map<String, String> metadata = tokenAuthentication.generateCredentials();
        assertThat(metadata).isNotNull();
        String token = metadata.get("Authorization");
        assertThat(token).isEqualTo("Bearer 1234");
    }

    @Test
    void testTokenAuthenticationSupplier() {
        TokenAuthentication tokenAuthentication = new TokenAuthentication(() -> "1234");
        Map<String, String> metadata = tokenAuthentication.generateCredentials();
        assertThat(metadata).isNotNull();
        String token = metadata.get("Authorization");
        assertThat(token).isEqualTo("Bearer 1234");
    }

    @Test
    void testTokenAuthenticationReusesCredentialsOfTheSameToken() {
        var token = new AtomicReference<>("1234");
        TokenAuthentication tokenAuthentication = new TokenAuthentication(token::get);
        Map<String, String> metadata = tokenAuthentication.generateCredentials();
        assertThat(metadata).containsExactly(Map.entry("Authorization", "Bearer 1234"));
        assertThat(tokenAuthentication.generateCredentials()).isSameAs(metadata);

        token.set("5678");
        assertThat(tokenAuthentication.generateCredentials())
                .containsExactly(Map.entry("Authorization", "Bearer 5678"));
    }

    @Test
    void testTokenFileReadAgainOnlyWhenModified(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("token");
        Files.writeString(file, "1234\n");
        FileTime lastModified = FileTime.fromMillis(1_000_000);
        Files.setLastModifiedTime(file, lastModified);
        TokenAuthentication auth = new TokenAuthentication("unused");
        auth.configure("file://" + file.toAbsolutePath());
        assertThat(auth.generateCredentials()).containsEntry("Authorization", "Bearer 1234");

        // Not read again while its modification time is the same
        Files.writeString(file, "5678\n");
        Files.setLastModifiedTime(file, lastModified);
        assertThat(auth.generateCredentials()).containsEntry("Authorization", "Bearer 1234");

        // Read again once it changes, e.g. on a token rotation
        Files.setLastModifiedTime(file, FileTime.fromMillis(2_000_000));
        assertThat(auth.generateCredentials()).containsEntry("Authorization", "Bearer 5678");

        Files.delete(file);
        assertThatThrownBy(auth::generateCredentials)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to read token from file");
    }

    @Test
    void testLegacyTokenAuthenticationClassName() throws Exception {
        Authentication authentication =
                AuthenticationFactory.create(
                        "io.streamnative.oxia.client.auth.TokenAuthentication", "token:1234");

        assertThat(authentication).isInstanceOf(TokenAuthentication.class);
        assertThat(authentication.generateCredentials()).containsEntry("Authorization", "Bearer 1234");
    }

    @Test
    void testTokenAuthenticationConfigure() throws Exception {
        Class<?> authClass = TokenAuthentication.class;
        Constructor<?> declaredConstructor = authClass.getDeclaredConstructor();
        declaredConstructor.setAccessible(true);
        Authentication auth = (Authentication) declaredConstructor.newInstance();
        ((TokenAuthentication) auth).configure("token:1234");
        Map<String, String> metadata = auth.generateCredentials();
        assertThat(metadata).isNotNull();
        String token = metadata.get("Authorization");
        assertThat(token).isEqualTo("Bearer 1234");

        ((TokenAuthentication) auth).configure("file:/path/to/file");
        assertThatThrownBy(auth::generateCredentials)
                .isInstanceOf(RuntimeException.class)
                .describedAs("Invalid token configuration: file:/path/to/file");

        File file = new File("src/test/resources/test-client-token");
        ((TokenAuthentication) auth).configure("file://" + file.getAbsolutePath());
        metadata = auth.generateCredentials();
        assertThat(metadata).isNotNull();
        token = metadata.get("Authorization");
        assertThat(token).isEqualTo("Bearer 4567");

        assertThatThrownBy(() -> ((TokenAuthentication) auth).configure("invalid:1234"))
                .isInstanceOf(IllegalArgumentException.class)
                .describedAs("Invalid token configuration: invalid:1234");
    }
}
