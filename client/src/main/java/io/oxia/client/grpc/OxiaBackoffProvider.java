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
package io.oxia.client.grpc;

import io.grpc.internal.BackoffPolicy;
import io.oxia.client.util.Backoff;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class OxiaBackoffProvider implements BackoffPolicy.Provider {
    public static final BackoffPolicy.Provider DEFAULT =
            new OxiaBackoffProvider(
                    Backoff.DEFAULT_INITIAL_DELAY_MILLIS,
                    TimeUnit.MILLISECONDS,
                    Backoff.DEFAULT_MAX_DELAY_SECONDS,
                    TimeUnit.MILLISECONDS);
    private final long initialDelay;
    private final TimeUnit unitInitialDelay;
    private final long maxDelay;
    private final TimeUnit unitMaxDelay;

    OxiaBackoffProvider(
            long initialDelay, TimeUnit unitInitialDelay, long maxDelay, TimeUnit unitMaxDelay) {
        this.initialDelay = initialDelay;
        this.unitInitialDelay = unitInitialDelay;
        this.maxDelay = maxDelay;
        this.unitMaxDelay = unitMaxDelay;
    }

    @Override
    public BackoffPolicy get() {
        return new Backoff(initialDelay, unitInitialDelay, maxDelay, unitMaxDelay);
    }

    public static BackoffPolicy.Provider create(Duration minDelay, Duration maxDelay) {
        return new OxiaBackoffProvider(
                minDelay.getNano(), TimeUnit.NANOSECONDS, maxDelay.getNano(), TimeUnit.NANOSECONDS);
    }
}
