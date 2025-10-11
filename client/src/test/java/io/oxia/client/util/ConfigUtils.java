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
package io.oxia.client.util;

import io.oxia.client.ClientConfig;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.OxiaClientBuilder;
import java.util.function.Consumer;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class ConfigUtils {

    public static ClientConfig getDefaultClientConfig() {
        final OxiaClientBuilderImpl builder = (OxiaClientBuilderImpl) OxiaClientBuilder.create("");
        return builder.getClientConfig();
    }

    public static ClientConfig getDefaultClientConfig(Consumer<OxiaClientBuilder> callback) {
        final OxiaClientBuilder builder = OxiaClientBuilder.create("");
        callback.accept(builder);
        return ((OxiaClientBuilderImpl) builder).getClientConfig();
    }
}
