/*
 * Copyright © 2022-2026 The Oxia Authors
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
package io.oxia.client.grpc;

import io.grpc.Status;
import lombok.NonNull;

public final class GrpcStatusUtils {
    private GrpcStatusUtils() {}

    public static boolean isNamespaceNotFound(@NonNull Status status) {
        if (status.getCode() == Status.Code.NOT_FOUND) {
            return true;
        }
        final var description = status.getDescription();
        return description != null
                && CustomStatusCode.fromDescription(description) == CustomStatusCode.ErrorNamespaceNotFound;
    }
}
