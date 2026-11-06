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

import org.apache.paimon.data.BlobTransferSource;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.HistogramStatistics;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricGroupImpl;
import org.apache.paimon.metrics.TestMetricRegistry;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void testTransferMetricsAccumulateWithoutRegisteringAgain() {
        RecordingRegistry registry = new RecordingRegistry();
        BlobFetchMetrics metrics = new BlobFetchMetrics(registry, TABLE_NAME, true);
        assertThat(metrics.isTransferMetricsEnabled()).isTrue();
        metrics.recordTransfer(BlobTransferSource.HTTP, 10, 10, 10_000_000L, true);
        metrics.recordTransfer(BlobTransferSource.HTTP, 5, 0, 30_000_000L, false);
        metrics.recordTransfer(null, -1, -1, -1, false);

        MetricGroup group = registry.transferGroup(BlobTransferSource.HTTP);
        assertThat(group.getAllVariables())
                .containsOnlyKeys("table", "sourceKind")
                .containsEntry("table", TABLE_NAME)
                .containsEntry("sourceKind", "http");
        assertThat(counter(group, BlobFetchMetrics.TRANSFER_READ_BYTES)).isEqualTo(15);
        assertThat(counter(group, BlobFetchMetrics.TRANSFER_WRITTEN_BYTES)).isEqualTo(10);
        assertThat(counter(group, BlobFetchMetrics.TRANSFER_DURATION_NANOS)).isEqualTo(40_000_000L);
        assertThat(counter(group, BlobFetchMetrics.TRANSFER_COMPLETED)).isEqualTo(1);
        assertThat(counter(group, BlobFetchMetrics.TRANSFER_FAILED)).isEqualTo(1);
        assertThat(
                        counter(
                                registry.transferGroup(BlobTransferSource.UNKNOWN),
                                BlobFetchMetrics.TRANSFER_READ_BYTES))
                .isZero();
        assertThat(registry.groups).hasSize(3);
        Histogram latency =
                (Histogram) group.getMetrics().get(BlobFetchMetrics.TRANSFER_LATENCY_MILLIS);
        assertThat(latency.getCount()).isEqualTo(2);
        assertThat(latency.getStatistics().getMean()).isEqualTo(20.0);
        assertThat(latency.getStatistics().getMin()).isEqualTo(10);
        assertThat(latency.getStatistics().getMax()).isEqualTo(30);
    }

    @Test
    public void testTransferGroupsAndHistogramHaveFixedBounds() {
        RecordingRegistry registry = new RecordingRegistry();
        BlobFetchMetrics metrics = new BlobFetchMetrics(registry, TABLE_NAME, true);
        for (BlobTransferSource source : BlobTransferSource.values()) {
            for (int i = 0; i < 1100; i++) {
                metrics.recordTransfer(source, 0, 0, 1_000_000L, true);
            }
            Histogram latency =
                    (Histogram)
                            registry.transferGroup(source)
                                    .getMetrics()
                                    .get(BlobFetchMetrics.TRANSFER_LATENCY_MILLIS);
            assertThat(latency.getStatistics().size()).isEqualTo(1024);
        }
        assertThat(registry.groups).hasSize(1 + BlobTransferSource.values().length);
    }

    @Test
    public void testCloseDoesNotAllowLateTransferReportsToCreateOrUpdateGroups() {
        RecordingRegistry registry = new RecordingRegistry();
        BlobFetchMetrics metrics = new BlobFetchMetrics(registry, TABLE_NAME, true);
        metrics.recordTransfer(BlobTransferSource.HTTP, 10, 10, 1, true);
        metrics.close();
        metrics.close();
        metrics.recordTransfer(BlobTransferSource.HTTP, 5, 5, 1, true);
        metrics.recordTransfer(BlobTransferSource.FILE, 5, 5, 1, true);
        assertThat(registry.groups).hasSize(2);
        assertThat(registry.closeCount).isEqualTo(2);
        assertThat(
                        counter(
                                registry.transferGroup(BlobTransferSource.HTTP),
                                BlobFetchMetrics.TRANSFER_READ_BYTES))
                .isEqualTo(10);
    }

    @Test
    public void testCloseAttemptsEveryGroupAndPreservesTheFirstFailure() {
        RecordingRegistry registry = new RecordingRegistry();
        RuntimeException primaryFailure = new IllegalStateException("primary close failed");
        RuntimeException transferFailure = new IllegalStateException("transfer close failed");
        registry.closeFailures.put("blobFetch:", primaryFailure);
        registry.closeFailures.put("blobTransfer:http", transferFailure);
        BlobFetchMetrics metrics = new BlobFetchMetrics(registry, TABLE_NAME, true);
        metrics.recordTransfer(BlobTransferSource.HTTP, 1, 1, 1, true);
        metrics.recordTransfer(BlobTransferSource.FILE, 1, 1, 1, true);

        assertThatThrownBy(metrics::close).isSameAs(primaryFailure);
        assertThat(primaryFailure.getSuppressed()).containsExactly(transferFailure);
        assertThat(registry.closeCount).isEqualTo(3);
        metrics.close();
        metrics.recordTransfer(BlobTransferSource.CUSTOM, 1, 1, 1, true);
        assertThat(registry.closeCount).isEqualTo(3);
        assertThat(registry.groups).hasSize(3);
    }

    @Test
    public void testFailedMetricRegistrationClosesTheCandidateAndCanBeRetried() {
        RecordingRegistry registry = new RecordingRegistry();
        RuntimeException registrationFailure = new IllegalStateException("registration failed");
        RuntimeException closeFailure = new IllegalStateException("candidate close failed");
        registry.registrationFailure = registrationFailure;
        registry.closeFailures.put("blobTransfer:http", closeFailure);
        BlobFetchMetrics metrics = new BlobFetchMetrics(registry, TABLE_NAME, true);

        assertThatThrownBy(() -> metrics.recordTransfer(BlobTransferSource.HTTP, 1, 1, 1, true))
                .isSameAs(registrationFailure);
        assertThat(registrationFailure.getSuppressed()).containsExactly(closeFailure);
        assertThat(registry.closeCount).isEqualTo(1);
        registry.registrationFailure = null;
        registry.closeFailures.clear();
        metrics.recordTransfer(BlobTransferSource.HTTP, 1, 1, 1, true);
        assertThat(registry.groups).hasSize(3);
        metrics.close();
        assertThat(registry.closeCount).isEqualTo(3);
    }

    @Test
    public void testTransferMetricsAreDisabledByDefaultAndCanBeExplicitlyDisabled() {
        for (boolean explicit : new boolean[] {false, true}) {
            RecordingRegistry registry = new RecordingRegistry();
            BlobFetchMetrics metrics =
                    explicit
                            ? new BlobFetchMetrics(registry, TABLE_NAME, false)
                            : new BlobFetchMetrics(registry, TABLE_NAME);
            assertThat(metrics.isTransferMetricsEnabled()).isFalse();
            metrics.recordSuccess(10);
            metrics.recordFetchFailure(new RuntimeException("HTTP error code: 500"));
            for (BlobTransferSource source : BlobTransferSource.values()) {
                metrics.recordTransfer(source, 10, 10, 1_000_000L, true);
            }
            assertThat(registry.groups).hasSize(1);
            assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_TOTAL, 2);
            assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_SUCCESS, 1);
            assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_SUCCESS_BYTES, 10);
            assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_FAILURE, 1);
            assertCounter(metrics, BlobFetchMetrics.BLOB_FETCH_HTTP_SERVER_ERROR, 1);
            metrics.close();
            assertThat(registry.closeCount).isEqualTo(1);
        }
    }

    @Test
    public void testDisabledTransferReportingDoesNotAcquireTheReporterMonitor() throws Exception {
        RecordingRegistry registry = new RecordingRegistry();
        BlobFetchMetrics metrics = new BlobFetchMetrics(registry, TABLE_NAME);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            synchronized (metrics) {
                executor.submit(
                                () ->
                                        metrics.recordTransfer(
                                                BlobTransferSource.HTTP, 1, 1, 1, true))
                        .get(10, TimeUnit.SECONDS);
            }
            assertThat(registry.groups).hasSize(1);
        } finally {
            executor.shutdownNow();
            metrics.close();
        }
    }

    @Test
    public void testSharedReporterAccumulatesPreciselyWithNonAtomicCounters() throws Exception {
        RecordingRegistry registry = new RecordingRegistry();
        registry.nonAtomicCounters = true;
        BlobFetchMetrics metrics = new BlobFetchMetrics(registry, TABLE_NAME, true);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int thread = 0; thread < 4; thread++) {
                futures.add(
                        executor.submit(
                                () -> {
                                    await(start);
                                    for (int i = 0; i < 2000; i++) {
                                        metrics.recordTransfer(
                                                BlobTransferSource.HTTP, 3, 2, 9, i % 2 == 0);
                                    }
                                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
            MetricGroup group = registry.transferGroup(BlobTransferSource.HTTP);
            assertThat(registry.groups).hasSize(2);
            assertThat(counter(group, BlobFetchMetrics.TRANSFER_READ_BYTES)).isEqualTo(24_000);
            assertThat(counter(group, BlobFetchMetrics.TRANSFER_WRITTEN_BYTES)).isEqualTo(16_000);
            assertThat(counter(group, BlobFetchMetrics.TRANSFER_DURATION_NANOS)).isEqualTo(72_000);
            assertThat(counter(group, BlobFetchMetrics.TRANSFER_COMPLETED)).isEqualTo(4000);
            assertThat(counter(group, BlobFetchMetrics.TRANSFER_FAILED)).isEqualTo(4000);
            assertThat(
                            ((Histogram)
                                            group.getMetrics()
                                                    .get(BlobFetchMetrics.TRANSFER_LATENCY_MILLIS))
                                    .getCount())
                    .isEqualTo(8000);
        } finally {
            start.countDown();
            executor.shutdownNow();
            metrics.close();
        }
    }

    @Test
    public void testCloseWaitsForAnInFlightTransferAndRejectsLateReports() throws Exception {
        RecordingRegistry registry = new RecordingRegistry();
        registry.updateEntered = new CountDownLatch(1);
        registry.releaseUpdate = new CountDownLatch(1);
        BlobFetchMetrics metrics = new BlobFetchMetrics(registry, TABLE_NAME, true);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch closeStarted = new CountDownLatch(1);
        try {
            Future<?> recording =
                    executor.submit(
                            () -> metrics.recordTransfer(BlobTransferSource.HTTP, 3, 2, 9, true));
            assertThat(registry.updateEntered.await(10, TimeUnit.SECONDS)).isTrue();
            Future<?> closing =
                    executor.submit(
                            () -> {
                                closeStarted.countDown();
                                metrics.close();
                            });
            assertThat(closeStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> closing.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            registry.releaseUpdate.countDown();
            recording.get(10, TimeUnit.SECONDS);
            closing.get(10, TimeUnit.SECONDS);
            metrics.recordTransfer(BlobTransferSource.HTTP, 100, 100, 100, true);
            metrics.recordTransfer(BlobTransferSource.FILE, 100, 100, 100, true);
            MetricGroup group = registry.transferGroup(BlobTransferSource.HTTP);
            assertThat(counter(group, BlobFetchMetrics.TRANSFER_READ_BYTES)).isEqualTo(3);
            assertThat(counter(group, BlobFetchMetrics.TRANSFER_COMPLETED)).isEqualTo(1);
            assertThat(registry.groups).hasSize(2);
            assertThat(registry.closeCount).isEqualTo(2);
        } finally {
            registry.releaseUpdate.countDown();
            executor.shutdownNow();
            metrics.close();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting for test coordination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static long counter(MetricGroup group, String name) {
        return ((Counter) group.getMetrics().get(name)).getCount();
    }

    private static class RecordingRegistry extends TestMetricRegistry {
        private final List<MetricGroup> groups = new ArrayList<>();
        private int closeCount;
        private final Map<String, RuntimeException> closeFailures = new HashMap<>();
        private RuntimeException registrationFailure;
        private boolean nonAtomicCounters;
        private CountDownLatch updateEntered;
        private CountDownLatch releaseUpdate;

        @Override
        public MetricGroup createMetricGroup(String groupName, Map<String, String> variables) {
            MetricGroup group =
                    new MetricGroupImpl(groupName, variables) {
                        @Override
                        public Counter counter(String name) {
                            if (!nonAtomicCounters) {
                                return super.counter(name);
                            }
                            Counter counter = new NonAtomicCounter();
                            getMetrics().put(name, counter);
                            return counter;
                        }

                        @Override
                        public Histogram histogram(String name, int windowSize) {
                            if (BlobFetchMetrics.TRANSFER_LATENCY_MILLIS.equals(name)
                                    && registrationFailure != null) {
                                throw registrationFailure;
                            }
                            Histogram histogram = super.histogram(name, windowSize);
                            if (updateEntered == null) {
                                return histogram;
                            }
                            return new Histogram() {
                                @Override
                                public void update(long value) {
                                    updateEntered.countDown();
                                    await(releaseUpdate);
                                    histogram.update(value);
                                }

                                @Override
                                public long getCount() {
                                    return histogram.getCount();
                                }

                                @Override
                                public HistogramStatistics getStatistics() {
                                    return histogram.getStatistics();
                                }
                            };
                        }

                        @Override
                        public void close() {
                            closeCount++;
                            RuntimeException failure =
                                    closeFailures.get(
                                            groupName
                                                    + ":"
                                                    + variables.getOrDefault("sourceKind", ""));
                            if (failure != null) {
                                throw failure;
                            }
                        }
                    };
            groups.add(group);
            return group;
        }

        private MetricGroup transferGroup(BlobTransferSource source) {
            return groups.stream()
                    .filter(
                            group ->
                                    source.metricValue()
                                            .equals(group.getAllVariables().get("sourceKind")))
                    .findFirst()
                    .orElseThrow(AssertionError::new);
        }
    }

    /** Deliberately non-atomic, matching counter backends which require serialized updates. */
    private static class NonAtomicCounter implements Counter {
        private long count;

        @Override
        public void inc() {
            inc(1);
        }

        @Override
        public void inc(long n) {
            long next = count + n;
            Thread.yield();
            count = next;
        }

        @Override
        public void dec() {
            dec(1);
        }

        @Override
        public void dec(long n) {
            inc(-n);
        }

        @Override
        public long getCount() {
            return count;
        }
    }

    private static void assertCounter(BlobFetchMetrics metrics, String name, long expected) {
        assertThat(counter(metrics, name)).isEqualTo(expected);
    }

    private static long counter(BlobFetchMetrics metrics, String name) {
        Map<String, Metric> registeredMetrics = metrics.getMetricGroup().getMetrics();
        return ((Counter) registeredMetrics.get(name)).getCount();
    }
}
