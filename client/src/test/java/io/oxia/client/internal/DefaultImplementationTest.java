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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;

class DefaultImplementationTest {

    @Test
    void failsWithoutImplementation() throws Exception {
        try (var apiLoader = apiLoader()) {
            withContextClassLoader(
                    apiLoader,
                    () ->
                            assertThatThrownBy(
                                            () -> Class.forName(DefaultImplementation.class.getName(), true, apiLoader))
                                    .isInstanceOf(ExceptionInInitializerError.class)
                                    .cause()
                                    .hasMessageStartingWith("Cannot load Oxia Client Implementation"));
        }
    }

    @Test
    void loadsImplementationFromContextClassLoader() throws Exception {
        try (var apiLoader = apiLoader();
                var implLoader = new URLClassLoader(implUrls(), apiLoader)) {
            var defaultImplementation =
                    withContextClassLoader(
                            implLoader,
                            () -> Class.forName(DefaultImplementation.class.getName(), true, apiLoader));

            var builder =
                    defaultImplementation
                            .getMethod("getDefaultImplementation", String.class)
                            .invoke(null, "localhost:6648");
            var sharedResourcesBuilder =
                    defaultImplementation.getMethod("getSharedResourcesBuilder").invoke(null);

            assertThat(builder.getClass().getName()).isEqualTo("io.oxia.client.OxiaClientBuilderImpl");
            assertThat(builder.getClass().getClassLoader()).isSameAs(implLoader);
            assertThat(sharedResourcesBuilder.getClass().getClassLoader()).isSameAs(implLoader);
        }
    }

    private static URLClassLoader apiLoader() {
        return new URLClassLoader(new URL[] {apiLocation()}, ClassLoader.getPlatformClassLoader());
    }

    private static URL apiLocation() {
        return DefaultImplementation.class.getProtectionDomain().getCodeSource().getLocation();
    }

    private static URL[] implUrls() throws Exception {
        var api = Path.of(apiLocation().toURI());
        return Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
                .map(Path::of)
                .filter(path -> !path.toAbsolutePath().equals(api))
                .map(DefaultImplementationTest::toUrl)
                .toArray(URL[]::new);
    }

    private static URL toUrl(Path path) {
        try {
            return path.toUri().toURL();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static <T> T withContextClassLoader(ClassLoader loader, Callable<T> action)
            throws Exception {
        var thread = Thread.currentThread();
        var previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            return action.call();
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
}
