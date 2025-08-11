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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OffPeakHours}. */
class OffPeakHoursTest {

    private static final int RATIO = 10;

    @Test
    void testCreateFromOptions() {
        Options options = new Options();
        options.set(CoreOptions.COMPACT_OFFPEAK_START_HOUR, 22);
        options.set(CoreOptions.COMPACT_OFFPEAK_END_HOUR, 6);
        options.set(CoreOptions.COMPACTION_OFFPEAK_RATIO, RATIO);
        CoreOptions coreOptions = new CoreOptions(options);

        OffPeakHours offPeakHours = OffPeakHours.create(coreOptions);

        assertThat(offPeakHours).isNotNull();
        assertThat(offPeakHours.currentRatio(23)).isEqualTo(RATIO);
        assertThat(offPeakHours.currentRatio(7)).isEqualTo(0);
    }

    @Test
    void testCreateFromOptionsWithDefault() {
        Options options = new Options();
        CoreOptions coreOptions = new CoreOptions(options);
        OffPeakHours offPeakHours = OffPeakHours.create(coreOptions);
        assertThat(offPeakHours).isNull();
    }

    @Test
    void testCreateFromOptionsWithSameHour() {
        Options options = new Options();
        options.set(CoreOptions.COMPACT_OFFPEAK_START_HOUR, 5);
        options.set(CoreOptions.COMPACT_OFFPEAK_END_HOUR, 5);
        options.set(CoreOptions.COMPACTION_OFFPEAK_RATIO, RATIO);
        CoreOptions coreOptions = new CoreOptions(options);

        OffPeakHours offPeakHours = OffPeakHours.create(coreOptions);

        assertThat(offPeakHours).isNull();
    }

    @Test
    void testCreateWithInvalidHours() {
        assertThat(OffPeakHours.create(-1, -1, RATIO)).isNull();
        assertThat(OffPeakHours.create(5, 5, RATIO)).isNull();
        assertThat(OffPeakHours.create(2, -1, RATIO)).isNull();
        assertThat(OffPeakHours.create(-1, 2, RATIO)).isNull();
    }

    @Test
    void testCurrentRatioNormalHours() {
        OffPeakHours offPeakHours = OffPeakHours.create(2, 8, RATIO);
        assertThat(offPeakHours).isNotNull();

        assertThat(offPeakHours.currentRatio(1)).as("Before start").isEqualTo(0);
        assertThat(offPeakHours.currentRatio(2)).as("At start").isEqualTo(RATIO);
        assertThat(offPeakHours.currentRatio(5)).as("In between").isEqualTo(RATIO);
        assertThat(offPeakHours.currentRatio(7)).as("Before end").isEqualTo(RATIO);
        assertThat(offPeakHours.currentRatio(8)).as("At end (exclusive)").isEqualTo(0);
        assertThat(offPeakHours.currentRatio(9)).as("After end").isEqualTo(0);
    }

    @Test
    void testCurrentRatioOvernightHours() {
        OffPeakHours offPeakHours = OffPeakHours.create(22, 6, RATIO);
        assertThat(offPeakHours).isNotNull();

        assertThat(offPeakHours.currentRatio(21)).as("Before start").isEqualTo(0);
        assertThat(offPeakHours.currentRatio(22)).as("At start").isEqualTo(RATIO);
        assertThat(offPeakHours.currentRatio(23)).as("After start").isEqualTo(RATIO);
        assertThat(offPeakHours.currentRatio(0)).as("After midnight, before end").isEqualTo(RATIO);
        assertThat(offPeakHours.currentRatio(5)).as("Before end").isEqualTo(RATIO);
        assertThat(offPeakHours.currentRatio(6)).as("At end (exclusive)").isEqualTo(0);
        assertThat(offPeakHours.currentRatio(10)).as("After end, before next start").isEqualTo(0);
    }
}
