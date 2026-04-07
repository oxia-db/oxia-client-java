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
    application
    alias(libs.plugins.shadow)
}

application {
    mainClass.set("io.oxia.client.perf.PerfClient")
    applicationDefaultJvmArgs =
        listOf(
            "-Dlog4j.configurationFile=classpath:log4j2.xml",
            // Suppress Netty sun.misc.Unsafe warnings on Java 23+
            "--sun-misc-unsafe-memory-access=allow",
            // Use ZGC for low-latency garbage collection
            "-XX:+UseZGC",
            // JIT compiler optimizations
            "-XX:+TieredCompilation",
            "-XX:-OmitStackTraceInFastThrow",
            // Heap settings
            "-Xms128m",
            "-Xmx1g",
            // Direct memory for Netty buffers
            "-XX:MaxDirectMemorySize=512m",
        )
}

dependencies {
    implementation(project(":client"))
    implementation(project(":client-api"))
    implementation(libs.guava)
    implementation(libs.jcommander)
    implementation(libs.opentelemetry.exporter.otlp)
    implementation(libs.opentelemetry.exporter.prometheus)
    implementation(libs.opentelemetry.sdk)
    implementation(libs.opentelemetry.sdk.autoconfigure)
    implementation(libs.hdr.histogram)
    implementation(libs.log4j.core)
    implementation(libs.log4j.slf4j2.impl)
}

tasks.shadowJar {
    exclude("module-info.class")
    exclude("META-INF/MANIFEST.MF")
    mergeServiceFiles()
}
