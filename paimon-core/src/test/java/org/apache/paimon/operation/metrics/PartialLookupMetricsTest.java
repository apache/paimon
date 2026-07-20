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

import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.TestMetricRegistry;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PartialLookupMetrics}. */
public class PartialLookupMetricsTest {

    @Test
    public void testRegistrationAndReporting() {
        PartialLookupMetrics metrics =
                new PartialLookupMetrics(new TestMetricRegistry(), "myTable");
        MetricGroup metricGroup = metrics.metricGroup();

        assertThat(metricGroup.getGroupName()).isEqualTo(PartialLookupMetrics.GROUP_NAME);
        assertThat(metricGroup.getAllVariables()).containsEntry("table", "myTable");
        Map<String, Metric> registeredMetrics = metricGroup.getMetrics();
        assertThat(registeredMetrics.keySet())
                .containsExactlyInAnyOrder(
                        PartialLookupMetrics.PARTIAL_LOOKUP_COUNT,
                        PartialLookupMetrics.PARTIAL_LOOKUP_REMOTE_ACCESS_COUNT);

        metrics.reportLookup(false);
        assertThat(metrics.lookupCount()).isEqualTo(1);
        assertThat(metrics.remoteAccessCount()).isZero();

        metrics.reportLookup(true);
        assertThat(metrics.lookupCount()).isEqualTo(2);
        assertThat(metrics.remoteAccessCount()).isEqualTo(1);
    }
}
