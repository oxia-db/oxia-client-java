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

configurations.testImplementation {
    resolutionStrategy {
        force("org.awaitility:awaitility:3.0.0")
    }
}

tasks.test {
    maxHeapSize = "1g"
}

dependencies {
    testImplementation(project(":client"))
    testImplementation(project(":client-api"))
    testImplementation(project(":testcontainers"))
    testImplementation(libs.grpc.stub)
    testImplementation(libs.opentelemetry.exporter.otlp)
    testImplementation(libs.opentelemetry.sdk)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(libs.opentelemetry.semconv)
    testImplementation(libs.testcontainers.junit.jupiter)
}
