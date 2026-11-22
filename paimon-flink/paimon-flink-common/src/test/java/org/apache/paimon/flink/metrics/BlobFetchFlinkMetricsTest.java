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

package org.apache.paimon.flink.metrics;

import org.apache.paimon.operation.metrics.BlobFetchMetrics;

import org.apache.flink.metrics.testutils.MetricListener;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies the existing table-scoped Flink registry bridge for Blob fetch counters. */
class BlobFetchFlinkMetricsTest {

    @Test
    void registersExistingAndOptionalCountersInSameTableGroup() {
        MetricListener listener = new MetricListener();
        BlobFetchMetrics metrics =
                new BlobFetchMetrics(
                        new FlinkMetricRegistry(listener.getMetricGroup()), "media", true);
        metrics.recordSuccess(7);
        metrics.recordTransfer(7, 7, 50, true);
        metrics.recordTransfer(4, 2, 30, false);
        metrics.recordFetchFailure(new RuntimeException("HTTP error code: 503"));
        assertCounter(listener, BlobFetchMetrics.BLOB_FETCH_SUCCESS_BYTES, 7);
        assertCounter(listener, BlobFetchMetrics.BLOB_FETCH_TRANSFER_TIMED_COUNT, 2);
        assertCounter(listener, BlobFetchMetrics.BLOB_FETCH_TRANSFER_DURATION_NANOS, 80);
        assertCounter(listener, BlobFetchMetrics.BLOB_FETCH_FAILED_READ_BYTES, 4);
        assertCounter(listener, BlobFetchMetrics.BLOB_FETCH_FAILED_WRITTEN_BYTES, 2);
        assertCounter(listener, BlobFetchMetrics.BLOB_FETCH_HTTP_503, 1);
        assertCounter(listener, BlobFetchMetrics.BLOB_FETCH_HTTP_SERVER_ERROR, 1);
        metrics.close();
    }

    private static void assertCounter(MetricListener listener, String name, long expected) {
        assertThat(listener.getCounter("paimon", "table", "media", "blobFetch", name))
                .hasValueSatisfying(counter -> assertThat(counter.getCount()).isEqualTo(expected));
    }
}
