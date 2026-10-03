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

package org.apache.paimon.oss;

import com.aliyun.oss.ClientErrorCode;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.comm.RequestMessage;
import com.aliyun.oss.common.comm.ResponseMessage;
import com.aliyun.oss.common.comm.RetryStrategy;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Retry strategy for every OSS request of {@link OSSFileIO}, so closing a data file or deleting
 * files survives throttling (QpsLimitExceeded), 5xx and network errors instead of failing the job.
 */
class OSSRetryStrategy extends RetryStrategy {

    private static final long BASE_DELAY_MILLIS = 300;
    private static final long MAX_DELAY_MILLIS = 10_000;

    private static final Set<Integer> RETRYABLE_STATUS =
            new HashSet<>(Arrays.asList(429, 500, 502, 503, 504));

    private static final Set<String> RETRYABLE_CLIENT_ERRORS =
            new HashSet<>(
                    Arrays.asList(
                            ClientErrorCode.CONNECTION_TIMEOUT,
                            ClientErrorCode.SOCKET_TIMEOUT,
                            ClientErrorCode.CONNECTION_REFUSED,
                            ClientErrorCode.UNKNOWN_HOST,
                            ClientErrorCode.SOCKET_EXCEPTION,
                            ClientErrorCode.SSL_EXCEPTION));

    @Override
    public boolean shouldRetry(
            Exception ex, RequestMessage request, ResponseMessage response, int retries) {
        if (ex instanceof ClientException) {
            return RETRYABLE_CLIENT_ERRORS.contains(((ClientException) ex).getErrorCode());
        }
        if (ex instanceof OSSException
                && OSSErrorCode.INVALID_RESPONSE.equals(((OSSException) ex).getErrorCode())) {
            return false;
        }
        return response != null && RETRYABLE_STATUS.contains(response.getStatusCode());
    }

    /** Capped exponential backoff with jitter, so parallel writers of a table spread retries. */
    @Override
    public long getPauseDelay(int retries) {
        long delay = Math.min(MAX_DELAY_MILLIS, BASE_DELAY_MILLIS << Math.min(retries, 16));
        return delay / 2 + ThreadLocalRandom.current().nextLong(delay / 2 + 1);
    }
}
