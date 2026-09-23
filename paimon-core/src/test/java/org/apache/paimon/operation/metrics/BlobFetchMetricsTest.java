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

import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.TestMetricRegistry;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlobFetchMetrics}. */
public class BlobFetchMetricsTest {

    private static final String TABLE_NAME = "myTable";

    @Test
    public void testMetricRegistration() {
        BlobFetchMetrics metrics = new BlobFetchMetrics(new TestMetricRegistry(), TABLE_NAME);
        MetricGroup metricGroup = metrics.getMetricGroup();

        assertThat(metricGroup.getGroupName()).isEqualTo(BlobFetchMetrics.GROUP_NAME);
        assertThat(metricGroup.getAllVariables()).containsEntry("table", TABLE_NAME);
        assertThat(metricGroup.getMetrics().keySet())
                .containsExactlyInAnyOrder(
                        BlobFetchMetrics.BLOB_FETCH_TOTAL,
                        BlobFetchMetrics.BLOB_FETCH_SUCCESS,
                        BlobFetchMetrics.BLOB_FETCH_SUCCESS_BYTES,
                        BlobFetchMetrics.BLOB_FETCH_NULL_WRITTEN,
                        BlobFetchMetrics.BLOB_FETCH_MISSING_FILE_NULL_WRITTEN,
                        BlobFetchMetrics.BLOB_FETCH_FAILURE_NULL_WRITTEN,
                        BlobFetchMetrics.BLOB_FETCH_FAILURE,
                        BlobFetchMetrics.BLOB_FETCH_HTTP_NOT_FOUND,
                        BlobFetchMetrics.BLOB_FETCH_HTTP_CLIENT_ERROR,
                        BlobFetchMetrics.BLOB_FETCH_HTTP_SERVER_ERROR,
                        BlobFetchMetrics.BLOB_FETCH_HTTP_OTHER_ERROR,
                        BlobFetchMetrics.BLOB_FETCH_INVALID_URI,
                        BlobFetchMetrics.BLOB_FETCH_OTHER_ERROR);
    }

    @Test
    public void testMetricsAreUpdated() {
        BlobFetchMetrics metrics = new BlobFetchMetrics(new TestMetricRegistry(), TABLE_NAME);

        metrics.recordSuccess(10);
        metrics.recordMissingFileNullWritten(true);
        metrics.recordMissingFileNullWritten(false);
        metrics.recordFetchFailureNullWritten(new RuntimeException("HTTP error code: 500"));
        metrics.recordFetchFailure(new RuntimeException("HTTP error code: 429"));
        metrics.recordFetchFailure(
                new IllegalArgumentException("Illegal character in path at index 1"));
        metrics.recordFetchFailure(new RuntimeException("boom"));

        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_TOTAL, 7);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_SUCCESS, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_SUCCESS_BYTES, 10);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_NULL_WRITTEN, 3);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_MISSING_FILE_NULL_WRITTEN, 2);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_FAILURE_NULL_WRITTEN, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_FAILURE, 3);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_NOT_FOUND, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_CLIENT_ERROR, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_SERVER_ERROR, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_OTHER_ERROR, 0);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_INVALID_URI, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_OTHER_ERROR, 1);
    }

    private static void assertCounter(BlobFetchMetrics metrics, String name, long expected) {
        assertThat(counter(metrics, name)).isEqualTo(expected);
    }

    private static long counter(BlobFetchMetrics metrics, String name) {
        Map<String, Metric> registeredMetrics = metrics.getMetricGroup().getMetrics();
        return ((Counter) registeredMetrics.get(name)).getCount();
    }
}
