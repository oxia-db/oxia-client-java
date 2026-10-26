/*
 * Copyright © 2026 The Oxia Authors
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class BackoffTest {

    @Test
    void startsAt100Millis() {
        assertThat(new Backoff().nextDelayMillis()).isBetween(100L, 120L);
    }

    @Test
    void doublesTheDelayUpToTheMaximum() {
        var backoff = new Backoff(100, TimeUnit.MILLISECONDS, 300, TimeUnit.MILLISECONDS);

        assertThat(backoff.nextDelayMillis()).isBetween(100L, 120L);
        assertThat(backoff.nextDelayMillis()).isBetween(200L, 240L);
        assertThat(backoff.nextDelayMillis()).isBetween(300L, 360L);
        assertThat(backoff.nextDelayMillis()).isBetween(300L, 360L);

        backoff.reset();
        assertThat(backoff.nextBackoffNanos())
                .isBetween(TimeUnit.MILLISECONDS.toNanos(100), TimeUnit.MILLISECONDS.toNanos(120));
    }
}
