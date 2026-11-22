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
import org.apache.paimon.metrics.MetricGroupImpl;
import org.apache.paimon.metrics.SimpleCounter;
import org.apache.paimon.metrics.TestMetricRegistry;

import org.junit.jupiter.api.Test;

import java.io.IOException;
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

    @Test
    public void testTransferMetricRegistration() {
        BlobFetchMetrics legacy = new BlobFetchMetrics(new TestMetricRegistry(), TABLE_NAME);
        BlobFetchMetrics enabled = new BlobFetchMetrics(new TestMetricRegistry(), TABLE_NAME, true);

        assertThat(legacy.isTransferMetricsEnabled()).isFalse();
        assertThat(enabled.isTransferMetricsEnabled()).isTrue();
        assertThat(enabled.getMetricGroup().getGroupName()).isEqualTo(BlobFetchMetrics.GROUP_NAME);
        assertThat(enabled.getMetricGroup().getAllVariables()).containsEntry("table", TABLE_NAME);
        assertThat(enabled.getMetricGroup().getMetrics().keySet())
                .hasSize(18)
                .containsAll(legacy.getMetricGroup().getMetrics().keySet())
                .contains(
                        BlobFetchMetrics.BLOB_FETCH_TRANSFER_TIMED_COUNT,
                        BlobFetchMetrics.BLOB_FETCH_TRANSFER_DURATION_NANOS,
                        BlobFetchMetrics.BLOB_FETCH_FAILED_READ_BYTES,
                        BlobFetchMetrics.BLOB_FETCH_FAILED_WRITTEN_BYTES,
                        BlobFetchMetrics.BLOB_FETCH_HTTP_503);
    }

    @Test
    public void testTransferMetricsAreUpdatedWithoutChangingFetchOutcomes() {
        BlobFetchMetrics metrics = new BlobFetchMetrics(new TestMetricRegistry(), TABLE_NAME, true);

        metrics.recordTransfer(10, 10, 30, true);
        metrics.recordTransfer(7, 3, 40, false);
        metrics.recordTransfer(0, 0, -1, false);

        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_TRANSFER_TIMED_COUNT, 3);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_TRANSFER_DURATION_NANOS, 70);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_FAILED_READ_BYTES, 7);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_FAILED_WRITTEN_BYTES, 3);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_503, 0);
        BlobFetchMetrics legacy = new BlobFetchMetrics(new TestMetricRegistry(), TABLE_NAME);
        legacy.getMetricGroup()
                .getMetrics()
                .keySet()
                .forEach(name -> assertCounter(metrics, name, 0));
    }

    @Test
    public void testDisabledTransferMetricsKeepLegacyCountersOnly() {
        for (BlobFetchMetrics metrics :
                new BlobFetchMetrics[] {
                    new BlobFetchMetrics(new TestMetricRegistry(), TABLE_NAME),
                    new BlobFetchMetrics(new TestMetricRegistry(), TABLE_NAME, false)
                }) {
            metrics.recordTransfer(7, 3, 40, false);
            metrics.recordFetchFailure(new IOException("HTTP error code: 503"));
            assertThat(metrics.isTransferMetricsEnabled()).isFalse();
            assertThat(metrics.getMetricGroup().getMetrics()).hasSize(13);
            assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_TOTAL, 1);
            assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_FAILURE, 1);
            assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_SERVER_ERROR, 1);
            assertThat(metrics.getMetricGroup().getMetrics())
                    .doesNotContainKeys(
                            BlobFetchMetrics.BLOB_FETCH_TRANSFER_TIMED_COUNT,
                            BlobFetchMetrics.BLOB_FETCH_TRANSFER_DURATION_NANOS,
                            BlobFetchMetrics.BLOB_FETCH_FAILED_READ_BYTES,
                            BlobFetchMetrics.BLOB_FETCH_FAILED_WRITTEN_BYTES,
                            BlobFetchMetrics.BLOB_FETCH_HTTP_503);
        }
    }

    @Test
    public void testHttp503IsATerminalFailureSubset() {
        BlobFetchMetrics metrics = new BlobFetchMetrics(new TestMetricRegistry(), TABLE_NAME, true);

        metrics.recordFetchFailure(new IOException("HTTP error code: 503"));
        metrics.recordFetchFailureNullWritten(
                new RuntimeException("fetch failed", new IOException("HTTP error code: 503")));
        metrics.recordFetchFailure(new IOException("HTTP error code: 500"));
        metrics.recordFetchFailure(new IOException("HTTP error code: 502"));
        metrics.recordFetchFailure(new IOException("HTTP error code: 429"));
        metrics.recordFetchFailure(new IOException("no response"));

        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_503, 2);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_SERVER_ERROR, 4);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_CLIENT_ERROR, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_OTHER_ERROR, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_TOTAL, 6);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_FAILURE, 5);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_NULL_WRITTEN, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_FAILURE_NULL_WRITTEN, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_SUCCESS, 0);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_SUCCESS_BYTES, 0);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_TRANSFER_TIMED_COUNT, 0);
    }

    @Test
    public void testHttp503CounterFailurePreservesLegacyFailureMetrics() {
        BlobFetchMetrics metrics =
                new BlobFetchMetrics(
                        (name, variables) ->
                                new MetricGroupImpl(name, variables) {
                                    @Override
                                    public Counter counter(String metricName) {
                                        if (BlobFetchMetrics.BLOB_FETCH_HTTP_503.equals(
                                                metricName)) {
                                            return new SimpleCounter() {
                                                @Override
                                                public void inc() {
                                                    throw new IllegalStateException(
                                                            "counter failed");
                                                }
                                            };
                                        }
                                        return super.counter(metricName);
                                    }
                                },
                        TABLE_NAME,
                        true);

        metrics.recordFetchFailure(new IOException("HTTP error code: 503"));
        metrics.recordFetchFailureNullWritten(new IOException("HTTP error code: 503"));
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_TOTAL, 2);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_FAILURE, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_NULL_WRITTEN, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_FAILURE_NULL_WRITTEN, 1);
        assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_SERVER_ERROR, 2);
    }

    private static void assertCounter(BlobFetchMetrics metrics, String name, long expected) {
        assertThat(counter(metrics, name)).isEqualTo(expected);
    }

    private static long counter(BlobFetchMetrics metrics, String name) {
        Map<String, Metric> registeredMetrics = metrics.getMetricGroup().getMetrics();
        return ((Counter) registeredMetrics.get(name)).getCount();
    }
}
