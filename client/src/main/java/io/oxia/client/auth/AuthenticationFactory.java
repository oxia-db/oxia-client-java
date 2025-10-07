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

import com.google.common.base.Strings;
import io.oxia.client.api.Authentication;
import io.oxia.client.api.exceptions.UnsupportedAuthenticationException;
import java.lang.reflect.Constructor;

public class AuthenticationFactory {

    public static Authentication create(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException {
        try {
            if (!Strings.isNullOrEmpty(authPluginClassName)) {
                Class<?> authClass = Class.forName(authPluginClassName);
                Constructor<?> declaredConstructor = authClass.getDeclaredConstructor();
                declaredConstructor.setAccessible(true);
                Authentication auth = (Authentication) declaredConstructor.newInstance();
                auth.configure(authParamsString);
                return auth;
            }
        } catch (Throwable t) {
            throw new UnsupportedAuthenticationException("Failed to create authentication", t);
        }
        return null;
    }
}
