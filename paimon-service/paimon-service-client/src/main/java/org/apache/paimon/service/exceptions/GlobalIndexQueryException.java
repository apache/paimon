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

import org.apache.paimon.utils.Preconditions;

/** Structured failure returned by the global-index query protocol. */
public class GlobalIndexQueryException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final GlobalIndexQueryErrorCode errorCode;
    private final boolean retryable;

    public GlobalIndexQueryException(GlobalIndexQueryErrorCode errorCode, String message) {
        this(errorCode, errorCode.retryable(), message);
    }

    public GlobalIndexQueryException(
            GlobalIndexQueryErrorCode errorCode, boolean retryable, String message) {
        super(message);
        this.errorCode = Preconditions.checkNotNull(errorCode);
        this.retryable = retryable;
    }

    public GlobalIndexQueryException(
            GlobalIndexQueryErrorCode errorCode, String message, Throwable cause) {
        this(errorCode, errorCode.retryable(), message, cause);
    }

    public GlobalIndexQueryException(
            GlobalIndexQueryErrorCode errorCode,
            boolean retryable,
            String message,
            Throwable cause) {
        super(message, cause);
        this.errorCode = Preconditions.checkNotNull(errorCode);
        this.retryable = retryable;
    }

    public GlobalIndexQueryErrorCode errorCode() {
        return errorCode;
    }

    public boolean retryable() {
        return retryable;
    }
}
