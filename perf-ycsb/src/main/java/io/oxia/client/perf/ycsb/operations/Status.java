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
package io.oxia.client.perf.ycsb.operations;

public final class Status {
    private int code;
    private byte[] payload;
    private String errorInfo;

    private Status() {}

    public String getErrorInfo() {
        return errorInfo;
    }

    public static Status success() {
        final Status status = new Status();
        status.code = CODE_SUCCESS;
        return status;
    }

    public static Status success(byte[] payload) {
        final Status status = new Status();
        status.code = CODE_SUCCESS;
        status.payload = payload;
        return status;
    }

    public boolean isSuccess() {
        return code == CODE_SUCCESS;
    }

    public static Status failed(String errorInfo) {
        final Status status = new Status();
        status.code = CODE_UNKNOWN;
        status.errorInfo = errorInfo;
        return status;
    }

    public static final int CODE_SUCCESS = 0;
    public static final int CODE_UNKNOWN = 1;
}
