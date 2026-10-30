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
    `jacoco-report-aggregation`
    alias(libs.plugins.spotless)
    alias(libs.plugins.spotbugs)
    alias(libs.plugins.maven.publish) apply false
}

val publishedArtifactIds =
        mapOf(
                "client-api" to "oxia-client-api",
                "client" to "oxia-client",
                "perf" to "oxia-perf",
        )

val publishedNames =
        mapOf(
                "client-api" to "Oxia Client API",
                "client" to "Oxia Client",
                "perf" to "Oxia Perf Client",
        )

allprojects {
    repositories {
        mavenCentral()
    }

    // The spotbugs configuration exists in the root project too, so the forced
    // versions must apply to all projects, not just subprojects
    plugins.withId("com.github.spotbugs") {
        configurations.named("spotbugs") {
            resolutionStrategy {
                force("org.apache.logging.log4j:log4j-core:${rootProject.libs.versions.log4j.get()}")
                force("org.apache.ant:ant:1.10.15")
                force("org.apache.bcel:bcel:6.12.0")
                // spotbugs -> Saxon-HE -> xmlresolver pulls httpclient5 5.1.3; force patched versions
                force("org.apache.httpcomponents.client5:httpclient5:5.6.3")
                force("org.apache.httpcomponents.core5:httpcore5:5.4.3")
                force("org.apache.httpcomponents.core5:httpcore5-h2:5.4.3")
            }
        }
    }
}

dependencies {
    jacocoAggregation(project(":client"))
}

tasks.named<JacocoReport>("testCodeCoverageReport") {
    classDirectories.setFrom(
            classDirectories.files.map {
                fileTree(it) { exclude("io/oxia/proto/**", "io/netty/buffer/LightProto*") }
            },
    )
}

tasks.check {
    dependsOn(tasks.named("testCodeCoverageReport"))
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "jacoco")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "com.github.spotbugs")

    publishedArtifactIds[project.name]?.let { artifactId: String ->
        apply(plugin = "com.vanniktech.maven.publish")

        extensions.configure<com.vanniktech.maven.publish.MavenPublishBaseExtension>("mavenPublishing") {
            publishToMavenCentral()
            signAllPublications()

            coordinates(group.toString(), artifactId, version.toString())

            pom {
                name.set(publishedNames[project.name])
                description.set("Oxia Client SDK for Java")
                url.set("https://oxia-db.github.io")
                inceptionYear.set("2022")

                organization {
                    name.set("The Oxia Authors")
                    url.set("https://oxia-db.github.io/")
                }

                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0")
                    }
                }

                developers {
                    developer {
                        organization.set("The Oxia Authors")
                        organizationUrl.set("https://oxia-db.github.io/")
                    }
                }

                scm {
                    connection.set("scm:git:git://github.com/oxia-db/oxia-client-java.git")
                    developerConnection.set("scm:git:ssh://github.com:oxia-db/oxia-client-java.git")
                    url.set("https://github.com/oxia-db/oxia-client-java")
                }
            }
        }
    }

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(17))
        }
    }

    // Align this build's own classpaths without publishing the BOMs and constraints to consumers
    val buildVersions by configurations.creating {
        isCanBeConsumed = false
        isCanBeResolved = false
    }
    configurations.matching { it.isCanBeResolved && it.name.endsWith("Classpath") }.configureEach {
        extendsFrom(buildVersions)
    }

    if (providers.gradleProperty("dependencyLine").orNull == "latest") {
        val latest =
                mapOf(
                        "io.grpc" to rootProject.libs.versions.grpc.latest.get(),
                        "io.netty" to rootProject.libs.versions.netty.latest.get(),
                        "io.opentelemetry" to rootProject.libs.versions.opentelemetry.latest.get(),
                        "com.google.protobuf" to rootProject.libs.versions.protobuf.java.latest.get(),
                        "com.google.api.grpc" to rootProject.libs.versions.proto.google.common.protos.latest.get(),
                )
        configurations.configureEach {
            resolutionStrategy.eachDependency {
                val version = requested.version.orEmpty()
                latest[requested.group]?.takeIf { version.isNotEmpty() }?.let {
                    useVersion(if (version.endsWith("-alpha")) "$it-alpha" else it)
                }
            }
        }
    }

    dependencies {
        implementation(rootProject.libs.slog)

        compileOnly(rootProject.libs.lombok)
        annotationProcessor(rootProject.libs.lombok)
        testCompileOnly(rootProject.libs.lombok)
        testAnnotationProcessor(rootProject.libs.lombok)

        buildVersions(platform(rootProject.libs.grpc.bom))
        buildVersions(platform(rootProject.libs.netty.bom))
        buildVersions(platform(rootProject.libs.opentelemetry.bom))
        buildVersions(platform(rootProject.libs.opentelemetry.bom.alpha))
        buildVersions(platform(rootProject.libs.junit.bom))
        constraints {
            buildVersions(rootProject.libs.guava)
            buildVersions(rootProject.libs.opentelemetry.semconv)
            buildVersions(rootProject.libs.byte.buddy)
            buildVersions(rootProject.libs.byte.buddy.agent)
            buildVersions(rootProject.libs.commons.compress)
            buildVersions(rootProject.libs.commons.lang3)
            buildVersions(rootProject.libs.protobuf.java)
            buildVersions(rootProject.libs.jackson.databind)
            buildVersions(rootProject.libs.jackson.core)
            buildVersions(rootProject.libs.log4j.core)
            buildVersions(rootProject.libs.bcpkix)
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
