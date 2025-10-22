/*
 * Copyright © 2022-2025 The Oxia Authors
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
package io.oxia.client.perf.ycsb.output;

import io.oxia.client.perf.ycsb.WorkerException;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;

@Slf4j
final class PulsarOutput implements Output {
    private final PulsarClient client;
    private final Producer<BenchmarkReportSnapshot> producer;

    public PulsarOutput(PulsarOutputOptions options) {
        try {
            this.client =
                    PulsarClient.builder()
                            .serviceUrl(options.serviceURL())
                            .authentication(options.authenticationPlugin(), options.authenticationParams())
                            .build();
            this.producer =
                    client
                            .newProducer(Schema.AVRO(BenchmarkReportSnapshot.class))
                            .topic(options.targetTopic())
                            .create();
        } catch (PulsarClientException ex) {
            throw new WorkerException(ex);
        }
    }

    @Override
    public void report(BenchmarkReportSnapshot report) {
        try {
            final MessageId send = producer.send(report);
            log.info("output to pulsar success, the response message id: {}", send);
        } catch (Throwable e) {
            throw new WorkerException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
