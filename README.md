# Oxia Java client SDK

[![Build](https://github.com/oxia-db/oxia-client-java/actions/workflows/pr-build-and-test.yml/badge.svg)](https://github.com/oxia-db/oxia-client-java/actions/workflows/pr-build-and-test.yml)

## Overview

This project comprises JDK language compatible modules for the [Oxia][oxia] service. It provides
the following capabilities:

- [Client](client/) for the Oxia service
- [OpenTelemetry Metrics](client-metrics-opentelemetry/) integration with the client
- [Testcontainer](testcontainers/) for integration testing with a local Oxia service
- [Performance Test Tool](perf/) for performance testing with an Oxia service.

## Build

Requirements:

* JDK 17
* Gradle 9.4+ (wrapper included)

Common build actions:

|             Action              |                 Command                   |
|---------------------------------|-------------------------------------------|
| Full build and test             | `./gradlew build`                         |
| Skip tests                      | `./gradlew build -x test`                |
| Skip Jacoco test coverage check | `./gradlew build -x jacocoTestReport`    |
| Skip Spotless formatting check  | `./gradlew build -x spotlessCheck`       |
| Format code                     | `./gradlew spotlessApply`                |

### Contributing to Oxia

Please 🌟 star the project if you like it.

Feel free to open an [issue](https://github.com/oxia-db/oxia/issues/new) or start a [discussion](https://github.com/oxia-db/oxia/discussions/new/choose). You can also follow the development [guide]() to contribute and build on it.

### License

Copyright 2022-2026 The Oxia Authors

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
