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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OffPeakHours}. */
public class OffPeakHoursTest {

    @Test
    public void testNormalRangeOffPeakHours() {
        // Test normal range: 9 AM to 5 PM (9-17)
        OffPeakHours offPeakHours = OffPeakHours.create(9, 17, 0);

        // Hours before start should not be off-peak
        for (int hour = 0; hour < 9; hour++) {
            assertThat(offPeakHours.isOffPeak(hour))
                    .as("Hour %d should not be off-peak", hour)
                    .isFalse();
        }

        // Hours in range should be off-peak (start inclusive, end exclusive)
        for (int hour = 9; hour < 17; hour++) {
            assertThat(offPeakHours.isOffPeak(hour))
                    .as("Hour %d should be off-peak", hour)
                    .isTrue();
        }

        // Hours after end should not be off-peak
        for (int hour = 17; hour < 24; hour++) {
            assertThat(offPeakHours.isOffPeak(hour))
                    .as("Hour %d should not be off-peak", hour)
                    .isFalse();
        }
    }

    @Test
    public void testWrapAroundRangeOffPeakHours() {
        OffPeakHours offPeakHours = OffPeakHours.create(22, 6, 0);

        // Hours before end (0-5) should be off-peak
        for (int hour = 0; hour < 6; hour++) {
            assertThat(offPeakHours.isOffPeak(hour))
                    .as("Hour %d should be off-peak", hour)
                    .isTrue();
        }

        // Hours between end and start (6-21) should not be off-peak
        for (int hour = 6; hour < 22; hour++) {
            assertThat(offPeakHours.isOffPeak(hour))
                    .as("Hour %d should not be off-peak", hour)
                    .isFalse();
        }

        // Hours from start to end of day (22-23) should be off-peak
        for (int hour = 22; hour < 24; hour++) {
            assertThat(offPeakHours.isOffPeak(hour))
                    .as("Hour %d should be off-peak", hour)
                    .isTrue();
        }
    }

    @Test
    public void testSingleHourRange() {
        // Test single hour range: 12 to 13
        OffPeakHours offPeakHours = OffPeakHours.create(12, 13, 0);

        // Only hour 12 should be off-peak
        for (int hour = 0; hour < 24; hour++) {
            if (hour == 12) {
                assertThat(offPeakHours.isOffPeak(hour))
                        .as("Hour %d should be off-peak", hour)
                        .isTrue();
            } else {
                assertThat(offPeakHours.isOffPeak(hour))
                        .as("Hour %d should not be off-peak", hour)
                        .isFalse();
            }
        }
    }
}
