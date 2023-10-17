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

package org.apache.paimon.metrics;

import org.apache.paimon.metrics.groups.GenericMetricGroup;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link MetricGroup}. */
public class MetricGroupTest {
    @Test
    public void testGroupRegisterMetrics() {
        GenericMetricGroup group = GenericMetricGroup.createGenericMetricGroup("myTable", "commit");
        assertThat(group.isClosed()).isFalse();
        // these will fail is the registration is propagated
        group.counter("testcounter");
        group.gauge("testgauge", () -> null);
        assertThat(group.getGroupName()).isEqualTo("commit");
        assertThat(group.getAllTags().size()).isEqualTo(1);
        assertThat(group.getAllTags())
                .containsExactlyEntriesOf(
                        new HashMap<String, String>() {
                            {
                                put("table", "myTable");
                            }
                        });
        assertThat(group.getMetrics().size()).isEqualTo(2);
        group.close();
        assertThat(group.isClosed()).isTrue();
    }

    @Test
    public void testTolerateMetricNameCollisions() {
        final String name = "abctestname";
        GenericMetricGroup group = GenericMetricGroup.createGenericMetricGroup("myTable", "commit");

        Counter counter1 = group.counter(name);

        // return the old one with the metric name collision
        assertThat(group.counter(name)).isSameAs(counter1);
        group.close();
    }

    @Test
    public void testAddAndRemoveMetricGroups() {
        AbstractMetricGroup metricGroup =
                GenericMetricGroup.createGenericMetricGroup("myTable", "commit");
        assertThat(Metrics.getInstance().getMetricGroups()).contains(metricGroup);
        metricGroup.close();
        assertThat(Metrics.getInstance().getMetricGroups()).doesNotContain(metricGroup);
    }
}
