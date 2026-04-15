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

// Force patched versions of Shadow plugin's transitive buildscript deps:
//   - plexus-utils 4.0.3 fixes CVE-2025-67030 (directory traversal)
//   - log4j-core 2.25.4 fixes CVE-2026-34477, CVE-2026-34478, CVE-2026-34480
buildscript {
    configurations.classpath {
        resolutionStrategy {
            force("org.codehaus.plexus:plexus-utils:4.0.3")
            force("org.apache.logging.log4j:log4j-core:2.25.4")
            force("org.apache.logging.log4j:log4j-api:2.25.4")
        }
    }
}

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
            "-Xms512m",
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
