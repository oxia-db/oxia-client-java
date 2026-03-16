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
package io.oxia.client.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.oxia.client.ProtoUtil;
import io.oxia.proto.GetResponse;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class GetResultTest {

    @Test
    void fromProto() {
        var payload = "hello".getBytes(UTF_8);
        var response = new GetResponse();
        response.setValue(payload);
        response
                .setVersion()
                .setVersionId(1L)
                .setCreatedTimestamp(2L)
                .setModifiedTimestamp(3L)
                .setModificationsCount(4L);
        assertThat(ProtoUtil.getResultFromProto("original-key", response))
                .isEqualTo(
                        new GetResult(
                                "original-key",
                                payload,
                                new io.oxia.client.api.Version(
                                        1L, 2L, 3L, 4L, Optional.empty(), Optional.empty())));
    }

    @Test
    void fromProtoWithOverride() {
        var payload = "hello".getBytes(UTF_8);
        var response = new GetResponse();
        response.setKey("new-key").setValue(payload);
        response
                .setVersion()
                .setVersionId(1L)
                .setCreatedTimestamp(2L)
                .setModifiedTimestamp(3L)
                .setModificationsCount(4L);
        assertThat(ProtoUtil.getResultFromProto("original-key", response))
                .isEqualTo(
                        new GetResult(
                                "new-key",
                                payload,
                                new io.oxia.client.api.Version(
                                        1L, 2L, 3L, 4L, Optional.empty(), Optional.empty())));
    }
}
