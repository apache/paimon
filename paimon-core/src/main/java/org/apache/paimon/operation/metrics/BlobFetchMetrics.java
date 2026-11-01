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
import org.apache.paimon.data.BlobTransferSource;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.rest.HttpClientUtils;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.IOUtils;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    public static final String TRANSFER_GROUP_NAME = "blobTransfer";
    public static final String TRANSFER_READ_BYTES = "blobTransferReadBytes";
    public static final String TRANSFER_WRITTEN_BYTES = "blobTransferWrittenBytes";
    public static final String TRANSFER_DURATION_NANOS = "blobTransferDurationNanos";
    public static final String TRANSFER_LATENCY_MILLIS = "blobTransferLatencyMillis";
    public static final String TRANSFER_COMPLETED = "blobTransferCompleted";
    public static final String TRANSFER_FAILED = "blobTransferFailed";

    private final MetricRegistry registry;
    private final String tableName;
    // An enum bounds the number of dynamic groups, independently of URI or user-provided data.
    private final Map<BlobTransferSource, TransferMetrics> transferGroups =
            new EnumMap<>(BlobTransferSource.class);
    private boolean closed;

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
        this.registry = registry;
        this.tableName = tableName;
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

    @Override
    public synchronized void recordTransfer(
            BlobTransferSource source,
            long readBytes,
            long writtenBytes,
            long durationNanos,
            boolean completed) {
        if (closed) {
            return;
        }
        BlobTransferSource kind = source == null ? BlobTransferSource.UNKNOWN : source;
        TransferMetrics transfer = transferGroups.get(kind);
        if (transfer == null) {
            Map<String, String> variables = new LinkedHashMap<>();
            variables.put("table", tableName);
            variables.put("sourceKind", kind.metricValue());
            MetricGroup candidate = registry.createMetricGroup(TRANSFER_GROUP_NAME, variables);
            try {
                transfer = new TransferMetrics(candidate);
            } catch (RuntimeException | Error e) {
                try {
                    candidate.close();
                } catch (RuntimeException | Error closeFailure) {
                    ExceptionUtils.firstOrSuppressed(closeFailure, e);
                }
                throw e;
            }
            transferGroups.put(kind, transfer);
        }
        transfer.readBytes.inc(Math.max(0, readBytes));
        transfer.writtenBytes.inc(Math.max(0, writtenBytes));
        transfer.durationNanos.inc(Math.max(0, durationNanos));
        transfer.latencyMillis.update(Math.max(0, durationNanos) / 1_000_000L);
        (completed ? transfer.completed : transfer.failed).inc();
    }

    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        List<AutoCloseable> closeables = new ArrayList<>();
        closeables.add(metricGroup::close);
        transferGroups.values().forEach(transfer -> closeables.add(transfer.group::close));
        transferGroups.clear();
        try {
            IOUtils.closeAll(closeables);
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }

    private static class TransferMetrics {
        private final MetricGroup group;
        private final Counter readBytes;
        private final Counter writtenBytes;
        private final Counter durationNanos;
        private final Histogram latencyMillis;
        private final Counter completed;
        private final Counter failed;

        private TransferMetrics(MetricGroup group) {
            this.group = group;
            this.readBytes = group.counter(TRANSFER_READ_BYTES);
            this.writtenBytes = group.counter(TRANSFER_WRITTEN_BYTES);
            this.durationNanos = group.counter(TRANSFER_DURATION_NANOS);
            this.latencyMillis = group.histogram(TRANSFER_LATENCY_MILLIS, 1024);
            this.completed = group.counter(TRANSFER_COMPLETED);
            this.failed = group.counter(TRANSFER_FAILED);
        }
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
