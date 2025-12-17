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
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.paimon.CoreOptions.COMPACTION_OPTIMIZATION_INTERVAL;
import static org.apache.paimon.CoreOptions.COMPACTION_TOTAL_SIZE_THRESHOLD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link EarlyFullCompaction}. */
public class EarlyFullCompactionTest {

    @Test
    public void testCreateNoOptions() {
        CoreOptions options = new CoreOptions(new Options());
        assertThat(EarlyFullCompaction.create(options)).isNull();
    }

    @Test
    public void testCreateWithInterval() {
        Options options = new Options();
        options.set(COMPACTION_OPTIMIZATION_INTERVAL, Duration.ofHours(1));
        EarlyFullCompaction trigger = EarlyFullCompaction.create(new CoreOptions(options));
        assertThat(trigger).isNotNull();
    }

    @Test
    public void testCreateWithThreshold() {
        Options options = new Options();
        options.set(COMPACTION_TOTAL_SIZE_THRESHOLD, MemorySize.ofMebiBytes(100));
        EarlyFullCompaction trigger = EarlyFullCompaction.create(new CoreOptions(options));
        assertThat(trigger).isNotNull();
    }

    @Test
    public void testCreateWithBoth() {
        Options options = new Options();
        options.set(COMPACTION_OPTIMIZATION_INTERVAL, Duration.ofHours(1));
        options.set(COMPACTION_TOTAL_SIZE_THRESHOLD, MemorySize.ofMebiBytes(100));
        EarlyFullCompaction trigger = EarlyFullCompaction.create(new CoreOptions(options));
        assertThat(trigger).isNotNull();
    }

    @Test
    public void testSingleRun() {
        EarlyFullCompaction trigger = new EarlyFullCompaction(1000L, 1000L, null);
        assertThat(trigger.tryFullCompact(5, createRuns(100))).isEmpty();
    }

    @Test
    public void testNoOptions() {
        EarlyFullCompaction trigger = new EarlyFullCompaction(null, null, null);
        assertThat(trigger.tryFullCompact(5, createRuns(100, 200))).isEmpty();
    }

    @Test
    public void testInterval() {
        AtomicLong time = new AtomicLong(10_000);
        TestableEarlyFullCompaction trigger =
                new TestableEarlyFullCompaction(1000L, null, null, time);

        // First time, should trigger
        Optional<CompactUnit> compactUnit = trigger.tryFullCompact(5, createRuns(100, 200));
        assertThat(compactUnit).isPresent();
        assertThat(compactUnit.get().outputLevel()).isEqualTo(4);
        assertThat(compactUnit.get().files()).hasSize(2);

        // Last compaction time is now 10_000.
        // Advance time, but not enough for interval to trigger.
        time.addAndGet(500); // now 10_500
        assertThat(trigger.tryFullCompact(5, createRuns(100, 200))).isEmpty();

        // Advance time to be greater than interval.
        time.addAndGet(501); // now 11_001, diff is 1001 > 1000
        Optional<CompactUnit> compactUnit2 = trigger.tryFullCompact(5, createRuns(100, 200));
        assertThat(compactUnit2).isPresent();
        assertThat(compactUnit2.get().outputLevel()).isEqualTo(4);
        assertThat(compactUnit2.get().files()).hasSize(2);
    }

    @Test
    public void testTotalSizeThreshold() {
        EarlyFullCompaction trigger = new EarlyFullCompaction(null, 1000L, null);

        // total size 300 < 1000, should trigger
        Optional<CompactUnit> compactUnit = trigger.tryFullCompact(5, createRuns(100, 200));
        assertThat(compactUnit).isPresent();
        assertThat(compactUnit.get().outputLevel()).isEqualTo(4);
        assertThat(compactUnit.get().files()).hasSize(2);

        // total size 1000 == 1000, should not trigger
        assertThat(trigger.tryFullCompact(5, createRuns(500, 500))).isEmpty();

        // total size 1500 > 1000, should not trigger
        assertThat(trigger.tryFullCompact(5, createRuns(500, 1000))).isEmpty();
    }

    @Test
    public void testIncrementalSizeThreshold() {
        EarlyFullCompaction trigger = new EarlyFullCompaction(null, null, 500L);

        // trigger, no max level
        Optional<CompactUnit> compactUnit = trigger.tryFullCompact(5, createRuns(400, 200));
        assertThat(compactUnit).isPresent();
        assertThat(compactUnit.get().outputLevel()).isEqualTo(4);
        assertThat(compactUnit.get().files()).hasSize(2);

        // no trigger, no max level
        compactUnit = trigger.tryFullCompact(5, createRuns(100, 200));
        assertThat(compactUnit).isEmpty();

        // no trigger, with max level
        List<LevelSortedRun> runs =
                Arrays.asList(
                        createLevelSortedRun(100),
                        createLevelSortedRun(300),
                        createLevelSortedRun(4, 500));
        compactUnit = trigger.tryFullCompact(5, runs);
        assertThat(compactUnit).isEmpty();

        // trigger, with max level
        runs =
                Arrays.asList(
                        createLevelSortedRun(100),
                        createLevelSortedRun(300),
                        createLevelSortedRun(300),
                        createLevelSortedRun(4, 500));
        compactUnit = trigger.tryFullCompact(5, runs);
        assertThat(compactUnit).isPresent();
        assertThat(compactUnit.get().outputLevel()).isEqualTo(4);
        assertThat(compactUnit.get().files()).hasSize(4);
    }

    @Test
    public void testIntervalTriggersFirst() {
        AtomicLong time = new AtomicLong(10_000);
        // Interval will trigger, but size is > threshold
        TestableEarlyFullCompaction trigger =
                new TestableEarlyFullCompaction(1000L, 500L, null, time);

        // First time, interval should trigger even if size (600) > threshold (500)
        Optional<CompactUnit> compactUnit = trigger.tryFullCompact(5, createRuns(300, 300));
        assertThat(compactUnit).isPresent();
        assertThat(compactUnit.get().outputLevel()).isEqualTo(4);
    }

    @Test
    public void testThresholdTriggersWhenIntervalFails() {
        AtomicLong time = new AtomicLong(10_000);
        TestableEarlyFullCompaction trigger =
                new TestableEarlyFullCompaction(1000L, 500L, null, time);

        // Trigger once to set last compaction time
        assertThat(trigger.tryFullCompact(5, createRuns(10, 20))).isPresent();

        // Advance time, but not enough for interval to trigger
        time.addAndGet(500); // now 10_500

        // Size (60) < threshold (500), should trigger
        Optional<CompactUnit> compactUnit = trigger.tryFullCompact(5, createRuns(30, 30));
        assertThat(compactUnit).isPresent();
        assertThat(compactUnit.get().outputLevel()).isEqualTo(4);

        // Size (600) > threshold (500), should not trigger
        assertThat(trigger.tryFullCompact(5, createRuns(300, 300))).isEmpty();
    }

    private LevelSortedRun createLevelSortedRun(long size) {
        return createLevelSortedRun(0, size);
    }

    private LevelSortedRun createLevelSortedRun(int level, long size) {
        SortedRun run = mock(SortedRun.class);
        when(run.totalSize()).thenReturn(size);
        DataFileMeta file = mock(DataFileMeta.class);
        when(run.files()).thenReturn(singletonList(file));
        return new LevelSortedRun(level, run);
    }

    private List<LevelSortedRun> createRuns(long... sizes) {
        return Arrays.stream(sizes)
                .mapToObj(this::createLevelSortedRun)
                .collect(Collectors.toList());
    }

    /** A {@link EarlyFullCompaction} that allows controlling time for tests. */
    private static class TestableEarlyFullCompaction extends EarlyFullCompaction {

        private final AtomicLong currentTime;

        public TestableEarlyFullCompaction(
                @Nullable Long fullCompactionInterval,
                @Nullable Long totalSizeThreshold,
                @Nullable Long incrementalSizeThreshold,
                AtomicLong currentTime) {
            super(fullCompactionInterval, totalSizeThreshold, incrementalSizeThreshold);
            this.currentTime = currentTime;
        }

        @Override
        long currentTimeMillis() {
            return currentTime.get();
        }
    }
}
