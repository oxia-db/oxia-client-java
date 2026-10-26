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
package io.oxia.client.internal;

import lombok.experimental.UtilityClass;

@UtilityClass
class ReflectionUtils {

    @SuppressWarnings("unchecked")
    static <T> Class<T> newClassInstance(String className) {
        try {
            try {
                // when the API is loaded in the same classloader as the impl
                return (Class<T>)
                        Class.forName(className, true, DefaultImplementation.class.getClassLoader());
            } catch (Exception e) {
                // when the API is loaded in a separate classloader as the impl
                // the classloader that loaded the impl needs to be a child classloader of the classloader
                // that loaded the API
                return (Class<T>)
                        Class.forName(className, true, Thread.currentThread().getContextClassLoader());
            }
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            throw new RuntimeException(e);
        }
    }
}
