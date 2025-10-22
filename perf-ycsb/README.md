# Oxia YCSB

This is the tool for testing StreamNative oxia by the YCSB model.

## Getting Started

## Install

1. Compile and package the `perf-ycsb`

```shell
mvn clean package -DskipTests -P perf-ycsb -pl perf-ycsb
```

2. Run the command

```shell
java -jar ./perf-ycsb/target/oxia-perf-ycsb-<version>.jar ycsb
```

