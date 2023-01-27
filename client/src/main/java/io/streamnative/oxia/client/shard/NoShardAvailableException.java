package io.streamnative.oxia.client.shard;


import lombok.Getter;
import lombok.NonNull;

/** A leader has not been assigned to the shard that maps to the key. */
public class NoShardAvailableException extends RuntimeException {
    @Getter private final String key;
    @Getter private final Long shardId;

    /**
     * Creates an instance of the exception.
     *
     * @param key The key specified in the call.
     */
    public NoShardAvailableException(@NonNull String key) {
        super("No shard available to accept to key: " + key);
        this.key = key;
        this.shardId = null;
    }
    /**
     * Creates an instance of the exception.
     *
     * @param shardId The shard id specified in the call.
     */
    public NoShardAvailableException(long shardId) {
        super("Shard not available : " + shardId);
        this.shardId = shardId;
        this.key = null;
    }
}
