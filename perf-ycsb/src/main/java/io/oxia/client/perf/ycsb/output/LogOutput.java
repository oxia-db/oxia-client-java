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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class LogOutput implements Output {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final boolean pretty;

    public LogOutput(boolean pretty) {
        this.pretty = pretty;
        mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    @Override
    public void report(BenchmarkReportSnapshot report) {
        final String s;
        try {
            if (pretty) {
                s = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report);
            } else {
                s = mapper.writeValueAsString(report);
            }
        } catch (Throwable ex) {
            throw new OutputException(ex.getMessage());
        }
        log.info(s);
    }

    @Override
    public void close() {}
}
