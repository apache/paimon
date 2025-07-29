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

import java.time.LocalDateTime;

/** OffPeakHours. */
public abstract class OffPeakHours {

    public abstract boolean isOffPeak();

    public abstract boolean isOffPeak(int targetHour);

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

    public static OffPeakHours create(int startHour, int endHour) {
        if (startHour == -1 && endHour == -1) {
            return DISABLED;
        }

        if (startHour == endHour) {
            return DISABLED;
        }

        return new OffPeakHoursImpl(startHour, endHour);
    }

    private static class OffPeakHoursImpl extends OffPeakHours {

        private final int startHour;
        private final int endHour;

        OffPeakHoursImpl(int startHour, int endHour) {
            this.startHour = startHour;
            this.endHour = endHour;
        }

        @Override
        public boolean isOffPeak() {
            return isOffPeak(LocalDateTime.now().getHour());
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
