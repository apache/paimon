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

/** OffPeakHours to control compaction ratio by hours. */
public class OffPeakHours {

    private final int startHour;
    private final int endHour;
    private final int compactOffPeakRatio;

    private OffPeakHours(int startHour, int endHour, int compactOffPeakRatio) {
        this.startHour = startHour;
        this.endHour = endHour;
        this.compactOffPeakRatio = compactOffPeakRatio;
    }

    public int currentRatio(int targetHour) {
        boolean isOffPeak;
        if (startHour <= endHour) {
            isOffPeak = startHour <= targetHour && targetHour < endHour;
        } else {
            isOffPeak = targetHour < endHour || startHour <= targetHour;
        }
        return isOffPeak ? compactOffPeakRatio : 0;
    }

    @Nullable
    public static OffPeakHours create(CoreOptions options) {
        return create(
                options.compactOffPeakStartHour(),
                options.compactOffPeakEndHour(),
                options.compactOffPeakRatio());
    }

    public static OffPeakHours create(int startHour, int endHour, int compactOffPeakRatio) {
        if (startHour == -1 || endHour == -1) {
            return null;
        }

        if (startHour == endHour) {
            return null;
        }

        return new OffPeakHours(startHour, endHour, compactOffPeakRatio);
    }
}
