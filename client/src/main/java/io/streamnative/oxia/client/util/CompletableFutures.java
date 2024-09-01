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
package io.streamnative.oxia.client.util;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class CompletableFutures {

    public static Throwable unwrap(Throwable ex) {
        final Throwable rc;
        if (ex instanceof CompletionException || ex instanceof ExecutionException) {
            rc = ex.getCause() != null ? ex.getCause() : ex;
        } else {
            rc = ex;
        }
        return rc;
    }

    public static CompletionException wrap(Throwable ex) {
        if (ex instanceof CompletionException) {
            return (CompletionException) ex;
        } else {
            return new CompletionException(ex);
        }
    }
}
