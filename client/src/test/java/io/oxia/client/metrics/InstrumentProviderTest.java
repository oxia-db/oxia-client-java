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
package io.oxia.client.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class InstrumentProviderTest {

    private static final Attributes ATTRIBUTES = Attributes.builder().put("oxia.shard", 1L).build();

    private final InMemoryMetricReader reader = InMemoryMetricReader.create();
    private final OpenTelemetrySdk otel =
            OpenTelemetrySdk.builder()
                    .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(reader).build())
                    .build();

    @AfterEach
    void closeSdk() {
        otel.close();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "ns")
    void upDownCounter(String namespace) {
        var counter =
                new InstrumentProvider(otel, namespace)
                        .newUpDownCounter("counter", Unit.Events, "description", ATTRIBUTES);

        counter.add(5);
        counter.increment();
        counter.decrement();

        MetricData metric = metric("counter");
        assertThat(metric.getUnit()).isEqualTo("{event}");
        assertThat(metric.getLongSumData().getPoints())
                .extracting(LongPointData::getAttributes, LongPointData::getValue)
                .containsExactly(tuple(withNamespace(namespace), 5L));
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "ns")
    void latencyHistogram(String namespace) {
        var histogram =
                new InstrumentProvider(otel, namespace)
                        .newLatencyHistogram("latency", "description", ATTRIBUTES);

        histogram.recordSuccess(TimeUnit.MILLISECONDS.toNanos(500));
        histogram.recordFailure(TimeUnit.SECONDS.toNanos(2));

        MetricData metric = metric("latency");
        assertThat(metric.getUnit()).isEqualTo("s");
        assertThat(metric.getHistogramData().getPoints())
                .extracting(HistogramPointData::getAttributes, HistogramPointData::getSum)
                .containsExactlyInAnyOrder(
                        tuple(withStatus(namespace, "success"), 0.5),
                        tuple(withStatus(namespace, "failed"), 2.0));
    }

    @Test
    void defaultsToTheGlobalOpenTelemetry() {
        new InstrumentProvider(null, "ns")
                .newCounter("counter", Unit.Events, "description", ATTRIBUTES)
                .increment();

        assertThat(reader.collectAllMetrics()).isEmpty();
    }

    private MetricData metric(String name) {
        return reader.collectAllMetrics().stream()
                .filter(m -> m.getName().equals(name))
                .findFirst()
                .orElseThrow();
    }

    private static Attributes withNamespace(String namespace) {
        if (namespace == null) {
            return ATTRIBUTES;
        }
        return ATTRIBUTES.toBuilder().put("oxia.namespace", namespace).build();
    }

    private static Attributes withStatus(String namespace, String status) {
        return withNamespace(namespace).toBuilder().put("oxia.response.status", status).build();
    }
}
