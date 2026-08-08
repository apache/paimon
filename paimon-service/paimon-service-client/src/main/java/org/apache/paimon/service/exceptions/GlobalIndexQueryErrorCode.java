/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.service.exceptions;

/** Stable machine-readable failure codes for global-index query protocol. */
public enum GlobalIndexQueryErrorCode {
    UNKNOWN_KEY_SHARD(1, true),
    STALE_GENERATION(2, true),
    NOT_READY(3, false),
    DUPLICATE_KEY(4, false),
    UNSUPPORTED_PROTOCOL(5, false),
    REQUEST_TOO_LARGE(6, false),
    INVALID_REQUEST(7, false),
    OVERLOADED(8, true),
    REQUEST_TIMEOUT(9, true),
    INTERNAL_ERROR(10, false);

    private final int wireCode;
    private final boolean retryable;

    GlobalIndexQueryErrorCode(int wireCode, boolean retryable) {
        this.wireCode = wireCode;
        this.retryable = retryable;
    }

    public int wireCode() {
        return wireCode;
    }

    public boolean retryable() {
        return retryable;
    }

    public static GlobalIndexQueryErrorCode fromWireCode(int wireCode) {
        for (GlobalIndexQueryErrorCode value : values()) {
            if (value.wireCode == wireCode) {
                return value;
            }
        }
        throw new IllegalArgumentException(
                "Unknown global-index query error code " + wireCode + '.');
    }
}
