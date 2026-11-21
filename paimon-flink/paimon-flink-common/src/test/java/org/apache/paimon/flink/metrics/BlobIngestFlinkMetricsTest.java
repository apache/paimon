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

import org.apache.paimon.data.BlobIngestObservations;
import org.apache.paimon.metrics.BlobIngestMetrics;

import org.apache.flink.metrics.testutils.MetricListener;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies the real Flink registry bridge used by the sink writer and committer. */
class BlobIngestFlinkMetricsTest {

    @Test
    void testNativeObservationRegistersInTableScopedFlinkGroup() {
        MetricListener listener = new MetricListener();
        BlobIngestMetrics metrics = new BlobIngestMetrics("media");
        metrics.withMetricRegistry(new FlinkMetricRegistry(listener.getMetricGroup()));
        try (BlobIngestObservations.Scope ignored = metrics.open()) {
            BlobIngestObservations.record("payload_read", "observed", 123);
            BlobIngestObservations.record("http_response", "initial_get_503", 1);
            BlobIngestObservations.record("commit_publish", "committed", 1);
        }
        assertThat(
                        listener.getCounter(
                                "paimon", "table", "media", "blobIngest", "payload_read_observed"))
                .hasValueSatisfying(counter -> assertThat(counter.getCount()).isEqualTo(123));
        assertThat(
                        listener.getCounter(
                                "paimon",
                                "table",
                                "media",
                                "blobIngest",
                                "http_response_initial_get_503"))
                .hasValueSatisfying(counter -> assertThat(counter.getCount()).isEqualTo(1));
        assertThat(
                        listener.getCounter(
                                "paimon",
                                "table",
                                "media",
                                "blobIngest",
                                "commit_publish_committed"))
                .hasValueSatisfying(counter -> assertThat(counter.getCount()).isEqualTo(1));
        metrics.close();
        try (BlobIngestObservations.Scope ignored = metrics.open()) {
            assertThat(BlobIngestObservations.enabled()).isFalse();
        }
    }
}
