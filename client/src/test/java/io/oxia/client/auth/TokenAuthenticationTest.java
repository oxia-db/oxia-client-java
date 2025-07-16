/*
 * Copyright © 2022-2025 StreamNative Inc.
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
import java.util.Map;
import org.junit.jupiter.api.Test;

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
