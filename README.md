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
* Maven 3.8.6+

Common build actions:

|             Action              |                 Command                  |
|---------------------------------|------------------------------------------|
| Full build and test             | `mvn clean verify`                       |
| Skip tests                      | `mvn clean verify -DskipTests`           |
| Skip Jacoco test coverage check | `mvn clean verify -Djacoco.skip`         |
| Skip Spotless formatting check  | `mvn clean verify -Dspotless.check.skip` |
| Format code                     | `mvn spotless:apply`                     |
| Generate license headers        | `mvn license:format`                     |

### Contributing to Oxia

Please 🌟 star the project if you like it.

Feel free to open an [issue](https://github.com/oxia-db/oxia/issues/new) or start a [discussion](https://github.com/oxia-db/oxia/discussions/new/choose). You can also follow the development [guide]() to contribute and build on it.

### License

Copyright 2022-2025 The Oxia Authors

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
