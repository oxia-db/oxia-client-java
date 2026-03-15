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
    `java-library`
    jacoco
    alias(libs.plugins.spotless)
    alias(libs.plugins.spotbugs)
}

allprojects {
    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "jacoco")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "com.github.spotbugs")

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(17))
        }
    }

    dependencies {
        implementation(rootProject.libs.slf4j.api)

        compileOnly(rootProject.libs.lombok)
        annotationProcessor(rootProject.libs.lombok)
        testCompileOnly(rootProject.libs.lombok)
        testAnnotationProcessor(rootProject.libs.lombok)

        implementation(platform(rootProject.libs.grpc.bom))
        implementation(platform(rootProject.libs.opentelemetry.bom))
        implementation(platform(rootProject.libs.opentelemetry.bom.alpha))
        implementation(platform(rootProject.libs.junit.bom))
        constraints {
            implementation(rootProject.libs.guava)
            implementation(rootProject.libs.opentelemetry.semconv)
            implementation(rootProject.libs.byte.buddy)
            implementation(rootProject.libs.byte.buddy.agent)
            implementation(rootProject.libs.commons.compress)
            implementation(rootProject.libs.commons.lang3)
        }

        testImplementation(rootProject.libs.assertj)
        testImplementation(rootProject.libs.awaitility)
        testImplementation(rootProject.libs.junit.jupiter)
        testImplementation(rootProject.libs.mockito.junit.jupiter)
        testImplementation(rootProject.libs.slf4j.simple)
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    }

    tasks.test {
        useJUnitPlatform()
    }

    spotless {
        java {
            licenseHeader("/*\n" +
                " * Copyright \u00a9 \$YEAR The Oxia Authors\n" +
                " *\n" +
                " * Licensed under the Apache License, Version 2.0 (the \"License\");\n" +
                " * you may not use this file except in compliance with the License.\n" +
                " * You may obtain a copy of the License at\n" +
                " *\n" +
                " *     http://www.apache.org/licenses/LICENSE-2.0\n" +
                " *\n" +
                " * Unless required by applicable law or agreed to in writing, software\n" +
                " * distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
                " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
                " * See the License for the specific language governing permissions and\n" +
                " * limitations under the License.\n" +
                " */")
            googleJavaFormat()
            importOrder()
            removeUnusedImports()
            leadingSpacesToTabs(2)
            leadingTabsToSpaces(4)
            targetExclude("build/**")
        }
    }

    spotbugs {
        excludeFilter.set(rootProject.file("etc/findbugsExclude.xml"))
        onlyAnalyze.set(listOf("io.oxia.client.*"))
    }

    jacoco {
        toolVersion = "0.8.12"
    }

    tasks.jacocoTestReport {
        dependsOn(tasks.test)
        reports {
            xml.required.set(true)
        }
    }
}
