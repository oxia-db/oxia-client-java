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
package io.oxia.client.internal;

import io.oxia.client.api.OxiaClientBuilder;
import java.lang.reflect.Constructor;

/**
 * This class loads the implementation for {@link OxiaClientBuilder} and allows you to decouple the
 * API from the actual implementation. <b>This class is internal to the Oxia API implementation, and
 * it is not part of the public API it is not meant to be used by client applications.</b>
 */
public class DefaultImplementation {
    private static final Constructor<?> CONSTRUCTOR;

    private static final String IMPL_CLASS_NAME = "io.oxia.client.OxiaClientBuilderImpl";

    static {
        Constructor<?> impl;
        try {
            impl = ReflectionUtils.newClassInstance(IMPL_CLASS_NAME).getConstructor(String.class);
        } catch (Throwable error) {
            throw new RuntimeException("Cannot load Oxia Client Implementation: " + error, error);
        }
        CONSTRUCTOR = impl;
    }

    /**
     * Access the actual implementation of the Oxia Client API.
     *
     * @return the loaded implementation.
     */
    public static OxiaClientBuilder getDefaultImplementation(String serviceAddress) {
        try {
            return (OxiaClientBuilder) CONSTRUCTOR.newInstance(serviceAddress);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
