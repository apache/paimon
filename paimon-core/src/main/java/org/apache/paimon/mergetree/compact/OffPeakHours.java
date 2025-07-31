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

import javax.annotation.Nullable;

import java.time.LocalDateTime;

/** OffPeakHours to control compaction ratio by hours. */
public abstract class OffPeakHours {

    public abstract boolean isOffPeak(int targetHour);

    public abstract int currentRatio();

    @Nullable
    public static OffPeakHours create(CoreOptions options) {
        return create(
                options.compactOffPeakStartHour(),
                options.compactOffPeakEndHour(),
                options.compactOffPeakRatio());
    }

    @Nullable
    public static OffPeakHours create(int startHour, int endHour, int compactOffPeakRatio) {
        if (startHour == -1 && endHour == -1) {
            return null;
        }

        if (startHour == endHour) {
            return null;
        }

        return new OffPeakHoursImpl(startHour, endHour, compactOffPeakRatio);
    }

    private static class OffPeakHoursImpl extends OffPeakHours {

        private final int startHour;
        private final int endHour;
        private final int compactOffPeakRatio;

        OffPeakHoursImpl(int startHour, int endHour, int compactOffPeakRatio) {
            this.startHour = startHour;
            this.endHour = endHour;
            this.compactOffPeakRatio = compactOffPeakRatio;
        }

        @Override
        public boolean isOffPeak(int targetHour) {
            if (startHour <= endHour) {
                return startHour <= targetHour && targetHour < endHour;
            }
            return targetHour < endHour || startHour <= targetHour;
        }

        @Override
        public int currentRatio() {
            return isOffPeak(LocalDateTime.now().getHour()) ? compactOffPeakRatio : 0;
        }
    }
}
