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
package io.oxia.client.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.Meter;

public class UpDownCounter {

    private final LongUpDownCounter counter;
    private final Attributes attributes;

    UpDownCounter(
            Meter meter,
            String name,
            Unit unit,
            String description,
            String namespace,
            Attributes attributes) {
        LongUpDownCounterBuilder builder =
                meter.upDownCounterBuilder(name).setDescription(description).setUnit(unit.toString());

        if (namespace != null) {
            attributes = attributes.toBuilder().put("oxia.namespace", namespace).build();
        }

        this.counter = builder.build();
        this.attributes = attributes;
    }

    public void increment() {
        add(1);
    }

    public void decrement() {
        add(-1);
    }

    public void add(long delta) {
        counter.add(delta, attributes);
    }

    public void subtract(long diff) {
        add(-diff);
    }
}
