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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;

public class InstrumentProvider {

    public static final InstrumentProvider NOOP =
            new InstrumentProvider(OpenTelemetry.noop(), "default");

    private final String namespace;
    private final Meter meter;

    public InstrumentProvider(OpenTelemetry otel, String namespace) {
        this.namespace = namespace;
        if (otel == null) {
            otel = GlobalOpenTelemetry.get();
        }

        this.meter =
                otel.getMeterProvider()
                        .meterBuilder("io.oxia.client")
                        //                .setInstrumentationVersion(OxiaClient.getVersion())
                        .build();
    }

    public Counter newCounter(String name, Unit unit, String description, Attributes attributes) {
        return new Counter(meter, name, unit, description, namespace, attributes);
    }

    public UpDownCounter newUpDownCounter(
            String name, Unit unit, String description, Attributes attributes) {
        return new UpDownCounter(meter, name, unit, description, namespace, attributes);
    }

    public LatencyHistogram newLatencyHistogram(
            String name, String description, Attributes attributes) {
        return new LatencyHistogram(meter, name, description, namespace, attributes);
    }
}
