# Oxia Java Client API

The Oxia Java client provides a high-performance interface to interact with Oxia, a distributed coordination service. The client is available in both synchronous and asynchronous variants to suit different application needs.

A simple example on how to write and read records:

```java
// Import the necessary classes
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.client.api.SyncOxiaClient;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.PutResult;

// Create a synchronous client
SyncOxiaClient client = OxiaClientBuilder.create("localhost:6648")
    .syncClient();

// Write a value
PutResult putResult = client.put("/my-key", "my-value".getBytes());
System.out.println("Inserted key: " + putResult.key());
System.out.println("Version: " + putResult.version().versionId());

// Read a value
GetResult getResult = client.get("/my-key");
if (getResult != null) {
    System.out.println("Retrieved key: " + getResult.key());
    System.out.println("Value: " + new String(getResult.value()));
    System.out.println("Version: " + getResult.version().versionId());
}
```

## Async Client

While the sync interface is easier to use, the calls are blocking, which makes it harder to submit many concurrent requests and achieve high throughput.

Oxia also provides an "async" client API that makes use of CompletableFuture to track the completions of the read/write operations.

With the async client API, a single thread can submit many concurrent requests. In addition, the Oxia client library will automatically batch the requests for better performance.

```java
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

// Create an async client with batching configuration
AsyncOxiaClient client = OxiaClientBuilder.create("localhost:6648")
    .batchLinger(Duration.ofMillis(10))
    .asyncClient()
    .get();

// Submit multiple concurrent requests
CompletableFuture<PutResult> future1 = client.put("/key-1", "hello".getBytes());
CompletableFuture<PutResult> future2 = client.put("/key-2", "hello".getBytes());
CompletableFuture<PutResult> future3 = client.put("/key-3", "hello".getBytes());

// Wait for all operations to complete
CompletableFuture.allOf(future1, future2, future3).join();

// Check results
try {
    PutResult result1 = future1.get();
    PutResult result2 = future2.get();
    PutResult result3 = future3.get();
    
    System.out.println("All operations completed successfully");
} catch (Exception e) {
    System.err.println("Operation failed: " + e.getMessage());
}
```

## Client Options

When creating a client, you can customize its behavior with various options:

```java
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.client.api.SyncOxiaClient;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Duration;

SyncOxiaClient client = OxiaClientBuilder.create("localhost:6648")
    .namespace("my-namespace")
    .batchLinger(Duration.ofMillis(10))
    .maxRequestsPerBatch(100)
    .requestTimeout(Duration.ofSeconds(5))
    .sessionTimeout(Duration.ofSeconds(30))
    .openTelemetry(openTelemetry)
    .syncClient();
```

Available client options include:

| Option | Description | Default |
|--------|-------------|---------|
| `namespace(String namespace)` | Set the Oxia namespace to use | `"default"` |
| `batchLinger(Duration duration)` | How long to wait before sending a batched request | `5ms` |
| `maxRequestsPerBatch(int count)` | Maximum number of requests in a batch | `1000` |
| `requestTimeout(Duration duration)` | How long to wait for responses before cancelling | `30s` |
| `sessionTimeout(Duration duration)` | Session timeout for ephemeral records | `15s` |
| `openTelemetry(OpenTelemetry openTelemetry)` | Custom OpenTelemetry provider | - |
| `clientIdentifier(String clientIdentifier)` | Set a custom client identity | Random UUID |
| `enableTls(boolean enableTls)` | Configure TLS for secure connections | `false` |
| `authentication(Authentication authentication)` | Configure authentication | `null` |
| `connectionBackoff(Duration minDelay, Duration maxDelay)` | Configure connection retry backoff | - |
| `maxConnectionPerNode(int connections)` | Maximum connections per node | - |
| `connectionKeepAliveTimeout(Duration timeout)` | Connection keep-alive timeout | - |
| `connectionKeepAliveTime(Duration time)` | Connection keep-alive time | - |

## Namespaces

A client can use a particular Oxia namespace, other than `default`, by specifying an option in the client instantiation:

```java
SyncOxiaClient client = OxiaClientBuilder.create("localhost:6648")
    .namespace("my-namespace")
    .syncClient();
```

All the operations will be referring to that particular namespace and there are no key conflicts across namespaces.

## Authentication

Oxia supports token-based authentication:

```java
import io.streamnative.oxia.client.api.Authentication;
import io.grpc.Metadata;

// Using a static token
Authentication auth = new Authentication() {
    @Override
    public Metadata generateCredentials() {
        Metadata metadata = new Metadata();
        Metadata.Key<String> key = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
        metadata.put(key, "Bearer my-token");
        return metadata;
    }
};

SyncOxiaClient client = OxiaClientBuilder.create("localhost:6648")
    .authentication(auth)
    .syncClient();

// Alternatively, using the authentication plugin class name and parameters
SyncOxiaClient client = OxiaClientBuilder.create("localhost:6648")
    .authentication("io.streamnative.oxia.auth.TokenAuthentication", "token=my-token")
    .syncClient();
```

## Conditional Operations

You can make operations conditional on the current version of a record:

```java
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.DeleteOption;
import java.util.Set;

// Put operation that succeeds only if the record has the expected version
PutResult result = client.put("/my-key", "new-value".getBytes(), 
    Set.of(PutOption.IfVersionIdEquals(existingVersion.versionId())));

// Put operation that succeeds only if the record doesn't exist
PutResult result = client.put("/my-key", "new-value".getBytes(), 
    Set.of(PutOption.IfRecordDoesNotExist));

// Delete operation that succeeds only if the record has the expected version
boolean deleted = client.delete("/my-key", 
    Set.of(DeleteOption.IfVersionIdEquals(existingVersion.versionId())));
```

## Partition Keys

You can ensure that records are co-located in the same Oxia shard by using partition keys:

```java
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.GetOption;
import java.util.Set;

// Put operation with a partition key
PutResult result = client.put("/my-key", "value".getBytes(), 
    Set.of(PutOption.PartitionKey("my-partition")));

// Get operation with the same partition key
GetResult result = client.get("/my-key", 
    Set.of(GetOption.PartitionKey("my-partition")));
```

## Sequential Keys

You can create records with server-assigned sequential keys:

```java
import io.streamnative.oxia.client.api.PutOption;
import java.util.Set;
import java.util.List;

// Create a record with a sequential key
// The key will be "/my-key-prefix-1" where "1" is the first available sequence number
PutResult result = client.put("/my-key-prefix-", "value".getBytes(), 
    Set.of(
        PutOption.PartitionKey("my-partition"),
        PutOption.SequenceKeysDeltas(List.of(1L))
    ));

// Create multiple records with sequential keys in a single operation
// The keys will be "/my-key-prefix-1", "/my-key-prefix-2", "/my-key-prefix-4"
PutResult result = client.put("/my-key-prefix-", "value".getBytes(), 
    Set.of(
        PutOption.PartitionKey("my-partition"),
        PutOption.SequenceKeysDeltas(List.of(1L, 1L, 2L))
    ));
```

## Secondary Indexes

You can create secondary indexes for your records to enable efficient lookups:

```java
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.ListOption;
import io.streamnative.oxia.client.api.RangeScanOption;
import java.util.Set;
import java.util.List;

// Put operation with secondary indexes
PutResult result = client.put("/user/123", 
    "{\"name\":\"John\",\"email\":\"john@example.com\"}".getBytes(), 
    Set.of(
        PutOption.SecondaryIndex("email", "john@example.com"),
        PutOption.SecondaryIndex("name", "John")
    ));

// List records using a secondary index
List<String> keys = client.list("j", "k", 
    Set.of(ListOption.UseIndex("name")));

// Range scan using a secondary index
Iterable<GetResult> results = client.rangeScan("j", "k", 
    Set.of(RangeScanOption.UseIndex("name")));
for (GetResult result : results) {
    System.out.printf("Key: %s, Value: %s%n", result.key(), new String(result.value()));
}
```

## Key Comparison Operations

Oxia supports various key comparison operations for Get requests:

```java
import io.streamnative.oxia.client.api.GetOption;
import java.util.Set;

// Get the exact key (default behavior)
GetResult result = client.get("/my-key", 
    Set.of(GetOption.ComparisonEqual));

// Get the highest key that is <= the given key
GetResult result = client.get("/my-key", 
    Set.of(GetOption.ComparisonFloor));

// Get the lowest key that is >= the given key
GetResult result = client.get("/my-key", 
    Set.of(GetOption.ComparisonCeiling));

// Get the highest key that is < the given key
GetResult result = client.get("/my-key", 
    Set.of(GetOption.ComparisonLower));

// Get the lowest key that is > the given key
GetResult result = client.get("/my-key", 
    Set.of(GetOption.ComparisonHigher));
```

## Range Operations

Oxia provides operations to work with ranges of keys:

```java
// List all keys in a range
List<String> keys = client.list("/users/", "/users/z");

// Scan all records in a range
Iterable<GetResult> results = client.rangeScan("/users/", "/users/z");
for (GetResult result : results) {
    System.out.printf("Key: %s, Value: %s%n", result.key(), new String(result.value()));
}

// Delete all records in a range
client.deleteRange("/users/", "/users/z");
```

## Notifications

Clients can subscribe to receive a feed of notifications with all the events happening in the namespace they're using.

Notifications can be used to replicate exactly the state of an Oxia namespace or to invalidate a cache.

```java
import io.streamnative.oxia.client.api.Notification;
import java.util.function.Consumer;

// Register a notification callback
client.notifications(notification -> {
    if (notification instanceof Notification.KeyCreated) {
        Notification.KeyCreated created = (Notification.KeyCreated) notification;
        System.out.printf("Key created: %s, version: %d%n", 
            created.key(), created.version());
    } else if (notification instanceof Notification.KeyModified) {
        Notification.KeyModified modified = (Notification.KeyModified) notification;
        System.out.printf("Key modified: %s, version: %d%n", 
            modified.key(), modified.version());
    } else if (notification instanceof Notification.KeyDeleted) {
        Notification.KeyDeleted deleted = (Notification.KeyDeleted) notification;
        System.out.printf("Key deleted: %s%n", deleted.key());
    } else if (notification instanceof Notification.KeyRangeDelete) {
        Notification.KeyRangeDelete rangeDelete = (Notification.KeyRangeDelete) notification;
        System.out.printf("Key range deleted: %s to %s%n", 
            rangeDelete.startKeyInclusive(), rangeDelete.endKeyExclusive());
    }
});
```

Notification types include:
- `KeyCreated`: A record that didn't exist was created
- `KeyModified`: An existing record was modified
- `KeyDeleted`: A record was deleted
- `KeyRangeDelete`: A range of keys was deleted

## Ephemeral Records

Applications can create records that will automatically be removed once the client session expires.

```java
import io.streamnative.oxia.client.api.PutOption;
import java.util.Set;

// Create an ephemeral record
PutResult result = client.put("/my-key", "my-value".getBytes(), 
    Set.of(PutOption.AsEphemeralRecord));
```

Ephemeral records have their lifecycle tied to a particular client instance, and they
are automatically deleted when the client instance is closed.

These records are also deleted if the client cannot communicate with the Oxia
service for some extended amount of time, and the session between the client and
the service "expires".

Application can control the session behavior by setting the session timeout
appropriately with the `sessionTimeout()` option when creating the client instance.

## Error Handling

Oxia defines several exception types that you should handle in your application:

```java
import io.streamnative.oxia.client.api.exceptions.*;

try {
    // Perform Oxia operations
    client.put("/my-key", "my-value".getBytes(), 
        Set.of(PutOption.IfVersionIdEquals(123L)));
} catch (UnexpectedVersionIdException e) {
    // Handle version conflict error
    System.err.println("Version conflict: " + e.getMessage());
} catch (KeyAlreadyExistsException e) {
    // Handle key already exists error
    System.err.println("Key already exists: " + e.getMessage());
} catch (SessionDoesNotExistException e) {
    // Handle session does not exist error
    System.err.println("Session does not exist: " + e.getMessage());
} catch (OxiaException e) {
    // Handle other Oxia errors
    System.err.println("Oxia error: " + e.getMessage());
}
```