package io.streamnative.oxia.client.api;


import lombok.NonNull;

/**
 * The result of a client get request.
 *
 * @param payload The payload associated with the key specified in the call.
 * @param stat Metadata for the record associated with the key specified in the call.
 */
public record GetResult(byte @NonNull [] payload, @NonNull Stat stat) {}
