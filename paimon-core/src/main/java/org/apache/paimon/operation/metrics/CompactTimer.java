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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

import java.util.LinkedList;

/**
 * A timer which supports the following operations in O(1) amortized time complexity.
 *
 * <ul>
 *   <li>Start the timer.
 *   <li>Stop the timer.
 *   <li>Query how long the timer is running in the last <code>queryLengthMillis</code>
 *       milliseconds, where <code>queryLengthMillis</code> is a constant.
 * </ul>
 */
@ThreadSafe
public class CompactTimer {

    private final long queryLengthMillis;
    private final LinkedList<TimeInterval> intervals;
    // innerSum is the total length of intervals, except the first and the last one
    private long innerSum;
    private long lastCallMillis;

    public CompactTimer(long queryLengthMillis) {
        this.queryLengthMillis = queryLengthMillis;
        this.intervals = new LinkedList<>();
        this.innerSum = 0;
        this.lastCallMillis = -1;
    }

    public void start() {
        start(System.currentTimeMillis());
    }

    @VisibleForTesting
    void start(long millis) {
        synchronized (intervals) {
            removeExpiredIntervals(millis - queryLengthMillis);
            Preconditions.checkArgument(
                    intervals.isEmpty() || intervals.getLast().finished(),
                    "There is an unfinished interval. This is unexpected.");
            Preconditions.checkArgument(lastCallMillis <= millis, "millis must not decrease.");
            lastCallMillis = millis;

            if (intervals.size() > 1) {
                innerSum += intervals.getLast().totalLength();
            }
            intervals.add(new TimeInterval(millis));
        }
    }

    public void finish() {
        finish(System.currentTimeMillis());
    }

    @VisibleForTesting
    void finish(long millis) {
        synchronized (intervals) {
            removeExpiredIntervals(millis - queryLengthMillis);
            Preconditions.checkArgument(
                    intervals.size() > 0 && !intervals.getLast().finished(),
                    "There is no unfinished interval. This is unexpected.");
            Preconditions.checkArgument(lastCallMillis <= millis, "millis must not decrease.");
            lastCallMillis = millis;

            intervals.getLast().finish(millis);
        }
    }

    public long calculateLength() {
        return calculateLength(System.currentTimeMillis());
    }

    @VisibleForTesting
    long calculateLength(long toMillis) {
        synchronized (intervals) {
            Preconditions.checkArgument(lastCallMillis <= toMillis, "millis must not decrease.");
            lastCallMillis = toMillis;

            long fromMillis = toMillis - queryLengthMillis;
            removeExpiredIntervals(fromMillis);

            if (intervals.isEmpty()) {
                return 0;
            } else if (intervals.size() == 1) {
                return intervals.getFirst().calculateLength(fromMillis, toMillis);
            } else {
                // only the first and the last interval may not be complete,
                // so we calculate them separately
                return innerSum
                        + intervals.getFirst().calculateLength(fromMillis, toMillis)
                        + intervals.getLast().calculateLength(fromMillis, toMillis);
            }
        }
    }

    private void removeExpiredIntervals(long expireMillis) {
        while (intervals.size() > 0
                && intervals.getFirst().finished()
                && intervals.getFirst().finishMillis <= expireMillis) {
            intervals.removeFirst();
            if (intervals.size() > 1) {
                innerSum -= intervals.getFirst().totalLength();
            }
        }
    }

    private static class TimeInterval {

        private final long startMillis;
        private Long finishMillis;

        private TimeInterval(long startMillis) {
            this.startMillis = startMillis;
            this.finishMillis = null;
        }

        private void finish(long finishMillis) {
            this.finishMillis = finishMillis;
        }

        private boolean finished() {
            return finishMillis != null;
        }

        private long totalLength() {
            return finishMillis - startMillis;
        }

        private long calculateLength(long fromMillis, long toMillis) {
            if (finishMillis == null) {
                return toMillis - Math.min(Math.max(startMillis, fromMillis), toMillis);
            } else {
                long l = Math.max(fromMillis, startMillis);
                long r = Math.min(toMillis, finishMillis);
                return Math.max(0, r - l);
            }
        }
    }
}
