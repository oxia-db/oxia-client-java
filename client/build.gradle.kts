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

plugins {
    alias(libs.plugins.lightproto)
}

dependencies {
    api(project(":client-api"))

    implementation(libs.caffeine)
    implementation(libs.netty.buffer)
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.stub)
    implementation(libs.opentelemetry.api)
    implementation(libs.jakarta.annotation.api)
    implementation(libs.zero.allocation.hashing)

    testImplementation(libs.grpc.inprocess)
    testImplementation(libs.opentelemetry.sdk)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(libs.oxia.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
}

tasks.test {
    maxHeapSize = "1g"
}
