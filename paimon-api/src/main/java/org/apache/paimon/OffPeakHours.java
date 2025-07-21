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

package org.apache.paimon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;

/** OffPeakHours. */
public abstract class OffPeakHours {
    private static final Logger LOG = LoggerFactory.getLogger(OffPeakHours.class);

    public static final OffPeakHours DISABLED =
            new OffPeakHours() {
                @Override
                public boolean isOffPeak() {
                    return false;
                }

                @Override
                public boolean isOffPeak(int targetHour) {
                    return false;
                }
            };

    /**
     * @param startHour inclusive
     * @param endHour exclusive
     */
    public static OffPeakHours getInstance(int startHour, int endHour) {
        if (startHour == -1 && endHour == -1) {
            return DISABLED;
        }

        if (!isValidHour(startHour) || !isValidHour(endHour)) {
            if (LOG.isWarnEnabled()) {
                LOG.warn(
                        "Ignoring invalid start/end hour for peak hour : start = "
                                + startHour
                                + " end = "
                                + endHour
                                + ". Valid numbers are [0-23]");
            }
            return DISABLED;
        }

        if (startHour == endHour) {
            return DISABLED;
        }

        return new OffPeakHoursImpl(startHour, endHour);
    }

    private static boolean isValidHour(int hour) {
        return 0 <= hour && hour <= 23;
    }

    /** Returns whether {@code targetHour} is off-peak hour. */
    public abstract boolean isOffPeak(int targetHour);

    /** Returns whether it is off-peak hour. */
    public abstract boolean isOffPeak();

    private static class OffPeakHoursImpl extends OffPeakHours {
        final int startHour;
        final int endHour;

        /**
         * @param startHour inclusive
         * @param endHour exclusive
         */
        OffPeakHoursImpl(int startHour, int endHour) {
            this.startHour = startHour;
            this.endHour = endHour;
        }

        @Override
        public boolean isOffPeak() {
            return isOffPeak(ZonedDateTime.now(ZoneId.systemDefault()).getHour());
        }

        @Override
        public boolean isOffPeak(int targetHour) {
            if (startHour <= endHour) {
                return startHour <= targetHour && targetHour < endHour;
            }
            return targetHour < endHour || startHour <= targetHour;
        }
    }
}
