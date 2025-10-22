# Oxia Java Client

A JDK based client for [Oxia][oxia].

## Features

- Request batching
- Asynchronous operation
- Notifications callback
- Record caching

## Usage

See the included [integration test][it] for executable examples.

### Get and Put

```java
var client = new OxiaClientBuilder("localhost:6648").asyncClient().join();
var value = client.get("key").join().getValue();
// This Put is unconditional
var version = client.put("key", "hello".getBytes(), PutOptions.none()).join().getVersion();   
```

### Put — conditional on key absence

```java
try {
  var result = client.put(
        "key", value, PutOption.IfRecordDoesNotExist
  ).join();
} catch (KeyAlreadyExistsException e) {
  ...
}
```

### Put — conditional on version ID

```java
try {
  var result = client.put(
    "key", value, PutOption.ifVersionIdEquals(1L)
  ).join();
} catch (UnexpectedVersionIdException e) {
  ...
}
```

### Put — ephemeral record

```java

var result = client.put(
  "key", value, PutOption.AsEphemeralRecord
).join();
```

### Delete — unconditional

```java
var deleted = client.delete("key").join();
```

### Delete — conditional on version ID

```java
try {
  var deleted = client.delete(
    "key", DeleteOption.ifVersionIdEquals(1L)
  ).join();
} catch (UnexpectedVersionIdException e) {
  ...
}
```

### Delete Range

Consult the [Oxia documentation][sort] for detail concerning the ordering of keys.

```java
client.deleteRange("aStartInc", "bEndExc").join();
```

### List Key Range

Consult the [Oxia documentation][sort] for detail concerning the ordering of keys.

```java
var keys = client.list("aStartInc", "bEndExc").join();
```

### Subscribe to notifications

A stream of record change notifications that can be consumed.

```java
client.notifications(
  notification -> switch (notification) {
    case KeyCreated c -> System.out.println("Created " + c.key() + ":" + c.version());
    case KeyDeleted d -> System.out.println("Deleted " + d.key());
    case KeyModified m -> System.out.println("Modified " + m.key() + ":" + m.version());
  }
);
```

## Configuration

| Option                | Default value | Description                                              |
|:----------------------|--------------:|:---------------------------------------------------------|
| `serviceAddress`      |             - | Address of Oxia cluster — `<host>:<port>`                |
| `namespace`           |       default | The Oxia namespace that this client should use           |
| `requestTimeout`      |           30s | GRPC Request timeout                                     |
| `batchLinger`         |           5ms | Max time to wait for new operations before closing batch |
| `maxRequestsPerBatch` |         1,000 | Max number of operations in a batch                      |
| `recordCacheCapacity` |        10,000 | Number or records that can be cached by the client       |
| `sessionTimeout`      |           15s | Period of inactivity after which session will be closed  |
| `clientIdentifier`    | <random UUID> | String that uniquely identifies this client instance     |

## Metrics

The Oxia Java client collects various metrics about the operations being performed. There is an
[API][metrics-api] and an OpenTelemetry [implementation][otel-metrics] is provided.

There are various categories of metrics captured:

### Operation Metrics

|             Name              |                                  Description                                   |
|-------------------------------|--------------------------------------------------------------------------------|
| `oxia_client_operation_timer` | The time an operation takes to be executed from request submission to response |
| `oxia_client_operation_size`  | The record value size. Only applicable for `put` and `get`                     |

#### Attributes

|   Name   |     Description      |                     Values                     |
|----------|----------------------|------------------------------------------------|
| `type`   | The operation type   | `put`, `delete`, `delete_range`, `get`, `list` |
| `result` | The operation result | `success`, `failure`                           |

### Batch Metrics

|              Name               |                            Description                             |
|---------------------------------|--------------------------------------------------------------------|
| `oxia_client_batch_total_timer` | The time a batch takes to be completed from creation to response   |
| `oxia_client_batch_exec_timer`  | The time a batch takes to be completed from submission to response |
| `oxia_client_batch_size`        | The size of all the record values in a batch                       |
| `oxia_client_batch_requests`    | The count of requests in a batch                                   |

#### Attributes

|   Name   |     Description      |        Values        |
|----------|----------------------|----------------------|
| `type`   | The operation type   | `write`, `read`      |
| `result` | The operation result | `success`, `failure` |

### Cache Metrics

|              Name              |                    Description                    |
|--------------------------------|---------------------------------------------------|
| `oxia_client_cache_load_timer` | The time it took to load a record in to the cache |
| `oxia_client_cache_hits`       | Counts of cache hits and misses                   |
| `oxia_client_cache_evictions`  | Counts of record evictions from the cache         |

#### Attributes

|      Name       |           Description           |                         Values                         |              Notes               |
|-----------------|---------------------------------|--------------------------------------------------------|----------------------------------|
| `type`          | The operation type              | `load`, `hit`, `eviction`                              |                                  |
| `result`        | The operation result            | `success`, `failure`                                   |                                  |
| `removal_cause` | See: [`RemovalCause`][caffeine] | `explicit`, `replaced`, `collected`, `expired`, `size` | Applies to `type: eviction` only |

### Session Metrics

|              Name               |                 Description                 |
|---------------------------------|---------------------------------------------|
| `oxia_client_session_keepalive` | Status of the session keep-alive heartbeats |

#### Attributes

|   Name   |     Description      |        Values        |
|----------|----------------------|----------------------|
| `result` | The heartbeat result | `success`, `failure` |

### Notification Metrics

|               Name               |            Description            |
|----------------------------------|-----------------------------------|
| `oxia_client_notification`       | Status of the notification stream |
| `oxia_client_notification_event` | Count of record change events     |

#### Attributes

##### `oxia_client_notification_event`

|   Name   |    Description    |        Values        |
|----------|-------------------|----------------------|
| `type`   |                   | `batch`              |
| `result` | The stream status | `success`, `failure` |

##### `oxia_client_notification`

|  Name  |      Description      |                    Values                    |
|--------|-----------------------|----------------------------------------------|
| `type` | The notification type | `key_created`, `key_deleted`, `key_modified` |

### Shard Assignment Metrics

|                 Name                  |              Description               |
|---------------------------------------|----------------------------------------|
| `oxia_client_shard_assignment`        | Status of the shard assignments stream |
| `oxia_client_shard_assignment_change` | Count of assignment changes            |

#### Attributes

##### `oxia_client_shard_assignment`

|   Name   |    Description    |        Values        |
|----------|-------------------|----------------------|
| `type`   |                   | `event`              |
| `result` | The stream status | `success`, `failure` |

##### `oxia_client_shard_assignment_change`

|  Name  |     Description     |              Values              |
|--------|---------------------|----------------------------------|
| `type` | The assignment type | `added`, `removed`, `reassigned` |

[oxia]: https://github.com/oxia-db/oxia
[it]: ../client-it/src/test/java/io/oxia/client/it/OxiaClientIT.java
[sort]: https://github.com/oxia-db/oxia/blob/main/docs/oxia-key-sorting.md
[caffeine]: https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/RemovalCause.java
[metrics-api]: client-metrics-api/src/main/java/io/oxia/client/metrics/api/Metrics.java
[otel-metrics]: client-metrics-opentelemetry/src/main/java/io/oxia/client/metrics/opentelemetry/OpenTelemetryMetrics.java

