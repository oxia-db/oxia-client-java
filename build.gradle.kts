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

        extensions.configure<org.gradle.api.publish.PublishingExtension>("publishing") {
            repositories {
                maven {
                    name = "GitHubPackages"
                    url = uri("https://maven.pkg.github.com/oxia-db/oxia-client-java")
                    credentials {
                        username = findProperty("gpr.user") as String? ?: System.getenv("GITHUB_ACTOR")
                        password = findProperty("gpr.key") as String? ?: System.getenv("GITHUB_TOKEN")
                    }
                }
            }
        }
    }

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(17))
        }
    }

    dependencies {
        implementation(rootProject.libs.slog)

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
            implementation(rootProject.libs.protobuf.java)
            implementation(rootProject.libs.jackson.databind)
            implementation(rootProject.libs.jackson.core)
            implementation(rootProject.libs.log4j.core)
            implementation(rootProject.libs.bcpkix)
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

    configurations.named("spotbugs") {
        resolutionStrategy {
            force("org.apache.logging.log4j:log4j-core:${rootProject.libs.versions.log4j.get()}")
            force("org.apache.ant:ant:1.10.15")
            force("org.apache.bcel:bcel:6.12.0")
        }
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
