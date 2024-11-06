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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

/** Refresh black list for {@link FileStoreLookupFunction}. */
public class RefreshBlacklist {

    private static final Logger LOG = LoggerFactory.getLogger(RefreshBlacklist.class);

    private final List<Pair<Long, Long>> timePeriodsBlacklist;

    private long nextBlacklistCheckTime;

    public RefreshBlacklist(List<Pair<Long, Long>> timePeriodsBlacklist) {
        this.timePeriodsBlacklist = timePeriodsBlacklist;
        this.nextBlacklistCheckTime = -1;
    }

    @Nullable
    public static RefreshBlacklist create(String blacklist) {
        List<Pair<Long, Long>> timePeriodsBlacklist = parseTimePeriodsBlacklist(blacklist);
        if (timePeriodsBlacklist.isEmpty()) {
            return null;
        }

        return new RefreshBlacklist(timePeriodsBlacklist);
    }

    private static List<Pair<Long, Long>> parseTimePeriodsBlacklist(String blacklist) {
        if (StringUtils.isNullOrWhitespaceOnly(blacklist)) {
            return Collections.emptyList();
        }
        String[] timePeriods = blacklist.split(",");
        List<Pair<Long, Long>> result = new ArrayList<>();
        for (String period : timePeriods) {
            String[] times = period.split("->");
            if (times.length != 2) {
                throw new IllegalArgumentException(
                        String.format("Incorrect time periods format: [%s].", blacklist));
            }

            long left = parseToMillis(times[0]);
            long right = parseToMillis(times[1]);
            if (left > right) {
                throw new IllegalArgumentException(
                        String.format("Incorrect time period: [%s->%s].", times[0], times[1]));
            }
            result.add(Pair.of(left, right));
        }
        return result;
    }

    private static long parseToMillis(String dateTime) {
        try {
            return DateTimeUtils.parseTimestampData(dateTime + ":00", 3, TimeZone.getDefault())
                    .getMillisecond();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    String.format("Date time format error: [%s].", dateTime), e);
        }
    }

    public boolean canRefresh() {
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis < nextBlacklistCheckTime) {
            return false;
        }

        Pair<Long, Long> selectedPeriod = null;
        for (Pair<Long, Long> period : timePeriodsBlacklist) {
            if (period.getLeft() <= currentTimeMillis && currentTimeMillis <= period.getRight()) {
                selectedPeriod = period;
                break;
            }
        }

        if (selectedPeriod != null) {
            LOG.info(
                    "Current time {} is in black list {}-{}, so try to refresh cache next time.",
                    currentTimeMillis,
                    selectedPeriod.getLeft(),
                    selectedPeriod.getRight());
            nextBlacklistCheckTime = selectedPeriod.getRight() + 1;
            return false;
        }

        return true;
    }

    public long nextBlacklistCheckTime() {
        return nextBlacklistCheckTime;
    }
}
