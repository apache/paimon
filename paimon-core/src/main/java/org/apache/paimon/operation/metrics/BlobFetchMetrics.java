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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.rest.HttpClientUtils;

/** Metrics to measure blob fetches during write. */
public class BlobFetchMetrics implements BlobFetchMetricReporter {

    public static final String GROUP_NAME = "blobFetch";

    public static final String BLOB_FETCH_TOTAL = "blobFetchTotal";
    public static final String BLOB_FETCH_SUCCESS = "blobFetchSuccess";
    public static final String BLOB_FETCH_SUCCESS_BYTES = "blobFetchSuccessBytes";
    public static final String BLOB_FETCH_NULL_WRITTEN = "blobFetchNullWritten";
    public static final String BLOB_FETCH_MISSING_FILE_NULL_WRITTEN =
            "blobFetchMissingFileNullWritten";
    public static final String BLOB_FETCH_FAILURE_NULL_WRITTEN = "blobFetchFailureNullWritten";
    public static final String BLOB_FETCH_FAILURE = "blobFetchFailure";
    public static final String BLOB_FETCH_HTTP_NOT_FOUND = "blobFetchHttpNotFound";
    public static final String BLOB_FETCH_HTTP_CLIENT_ERROR = "blobFetchHttpClientError";
    public static final String BLOB_FETCH_HTTP_SERVER_ERROR = "blobFetchHttpServerError";
    public static final String BLOB_FETCH_HTTP_OTHER_ERROR = "blobFetchHttpOtherError";
    public static final String BLOB_FETCH_INVALID_URI = "blobFetchInvalidUri";
    public static final String BLOB_FETCH_OTHER_ERROR = "blobFetchOtherError";

    private final MetricGroup metricGroup;
    private final Counter totalCounter;
    private final Counter successCounter;
    private final Counter successBytesCounter;
    private final Counter nullWrittenCounter;
    private final Counter missingFileNullWrittenCounter;
    private final Counter fetchFailureNullWrittenCounter;
    private final Counter failureCounter;
    private final Counter httpNotFoundCounter;
    private final Counter httpClientErrorCounter;
    private final Counter httpServerErrorCounter;
    private final Counter httpOtherErrorCounter;
    private final Counter invalidUriCounter;
    private final Counter otherErrorCounter;

    public BlobFetchMetrics(MetricRegistry registry, String tableName) {
        this.metricGroup = registry.createTableMetricGroup(GROUP_NAME, tableName);
        this.totalCounter = metricGroup.counter(BLOB_FETCH_TOTAL);
        this.successCounter = metricGroup.counter(BLOB_FETCH_SUCCESS);
        this.successBytesCounter = metricGroup.counter(BLOB_FETCH_SUCCESS_BYTES);
        this.nullWrittenCounter = metricGroup.counter(BLOB_FETCH_NULL_WRITTEN);
        this.missingFileNullWrittenCounter =
                metricGroup.counter(BLOB_FETCH_MISSING_FILE_NULL_WRITTEN);
        this.fetchFailureNullWrittenCounter = metricGroup.counter(BLOB_FETCH_FAILURE_NULL_WRITTEN);
        this.failureCounter = metricGroup.counter(BLOB_FETCH_FAILURE);
        this.httpNotFoundCounter = metricGroup.counter(BLOB_FETCH_HTTP_NOT_FOUND);
        this.httpClientErrorCounter = metricGroup.counter(BLOB_FETCH_HTTP_CLIENT_ERROR);
        this.httpServerErrorCounter = metricGroup.counter(BLOB_FETCH_HTTP_SERVER_ERROR);
        this.httpOtherErrorCounter = metricGroup.counter(BLOB_FETCH_HTTP_OTHER_ERROR);
        this.invalidUriCounter = metricGroup.counter(BLOB_FETCH_INVALID_URI);
        this.otherErrorCounter = metricGroup.counter(BLOB_FETCH_OTHER_ERROR);
    }

    @VisibleForTesting
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    @Override
    public void recordSuccess(long bytes) {
        totalCounter.inc();
        successCounter.inc();
        if (bytes > 0) {
            successBytesCounter.inc(bytes);
        }
    }

    @Override
    public void recordMissingFileNullWritten(boolean httpNotFound) {
        totalCounter.inc();
        nullWrittenCounter.inc();
        missingFileNullWrittenCounter.inc();
        if (httpNotFound) {
            httpNotFoundCounter.inc();
        }
    }

    @Override
    public void recordFetchFailureNullWritten(Throwable throwable) {
        totalCounter.inc();
        nullWrittenCounter.inc();
        fetchFailureNullWrittenCounter.inc();
        recordFailureReason(throwable);
    }

    @Override
    public void recordFetchFailure(Throwable throwable) {
        totalCounter.inc();
        failureCounter.inc();
        recordFailureReason(throwable);
    }

    public void close() {
        metricGroup.close();
    }

    private void recordFailureReason(Throwable throwable) {
        Integer statusCode = HttpClientUtils.getHttpStatusCode(throwable);
        if (statusCode != null) {
            if (statusCode == 404) {
                httpNotFoundCounter.inc();
            } else if (statusCode >= 400 && statusCode < 500) {
                httpClientErrorCounter.inc();
            } else if (statusCode >= 500 && statusCode < 600) {
                httpServerErrorCounter.inc();
            } else {
                httpOtherErrorCounter.inc();
            }
            return;
        }

        if (HttpClientUtils.isInvalidUriException(throwable)) {
            invalidUriCounter.inc();
            return;
        }

        otherErrorCounter.inc();
    }
}
