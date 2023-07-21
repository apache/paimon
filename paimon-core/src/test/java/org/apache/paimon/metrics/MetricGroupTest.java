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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for the {@link MetricGroup}. */
public class MetricGroupTest {
    @Test
    public void closedGroupDoesNotRegisterMetrics() {
        GenericMetricGroup group = new GenericMetricGroup("testgroup");
        assertFalse(group.isClosed());

        group.close();
        assertTrue(group.isClosed());

        // these will fail is the registration is propagated
        group.counter("testcounter");
        group.gauge(
                "testgauge",
                new Gauge<Object>() {
                    @Override
                    public Object getValue() {
                        return null;
                    }
                });
        assertThat(group.getMetrics().size()).isEqualTo(0);
    }

    @Test
    public void tolerateMetricNameCollisions() {
        final String name = "abctestname";
        GenericMetricGroup group = new GenericMetricGroup("testgroup");

        Counter counter1 = group.counter(name);

        // return the old one with the metric name collision
        assertThat(group.counter(name)).isEqualTo(counter1);
    }
}
