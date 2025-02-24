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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.Levels;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MergeTreeCompactManager}. */
public class DelayedLookupCompactManagerTest extends MergeTreeCompactManagerTest {

    @Test
    public void testDelayedLookupCompactionBeforeThreshold()
            throws ExecutionException, InterruptedException {
        List<LevelMinMax> inputs =
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 3, 6),
                        new LevelMinMax(0, 6, 8),
                        new LevelMinMax(1, 1, 4),
                        new LevelMinMax(1, 6, 8),
                        new LevelMinMax(1, 10, 10),
                        new LevelMinMax(2, 1, 3),
                        new LevelMinMax(2, 4, 6));
        List<DataFileMeta> files = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i++) {
            LevelMinMax minMax = inputs.get(i);
            files.add(minMax.toFile(i));
        }

        Levels levels = new Levels(comparator, files, 3);

        CompactStrategy strategy = new ForceUpLevel0Compaction(new UniversalCompaction(0, 0, 5));

        DelayedLookupCompactManager oldManager =
                new DelayedLookupCompactManager(
                        service,
                        levels,
                        strategy,
                        comparator,
                        2,
                        Integer.MAX_VALUE,
                        new TestRewriter(true),
                        null,
                        null,
                        false,
                        LocalDateTime.now().minusDays(2),
                        Duration.ofDays(2),
                        5,
                        5);

        // delayed lookup compaction not triggered
        assertThat(oldManager.isDelayedCompactPartition()).isTrue();
        assertThat(oldManager.shouldTriggerDelayedLookupCompact()).isFalse();
        assertThat(oldManager.hasDelayedCompact()).isTrue();

        // trigger delayed lookup compaction by stop trigger
        oldManager.triggerCompaction(false);
        assertThat(oldManager.getCompactionResult(true)).isEmpty();

        oldManager.triggerCompaction(false);
        oldManager.triggerCompaction(false);
        oldManager.triggerCompaction(false);
        oldManager.triggerCompaction(false);

        assertThat(oldManager.shouldTriggerDelayedLookupCompact()).isTrue();
        oldManager.triggerCompaction(false);
        assertThat(oldManager.getCompactionResult(true)).isPresent();
        assertThat(oldManager.hasDelayedCompact()).isFalse();

        // trigger delayed lookup compaction by l0 file count
        oldManager.addNewFile((new LevelMinMax(0, 11, 12)).toFile(inputs.size()));
        oldManager.addNewFile((new LevelMinMax(0, 13, 14)).toFile(inputs.size() + 1));
        oldManager.addNewFile((new LevelMinMax(0, 15, 16)).toFile(inputs.size() + 2));
        oldManager.addNewFile((new LevelMinMax(0, 17, 18)).toFile(inputs.size() + 3));
        assertThat(oldManager.shouldTriggerDelayedLookupCompact()).isFalse();
        oldManager.addNewFile((new LevelMinMax(0, 15, 16)).toFile(inputs.size() + 4));
        oldManager.addNewFile((new LevelMinMax(0, 17, 18)).toFile(inputs.size() + 6));
        assertThat(oldManager.shouldTriggerDelayedLookupCompact()).isTrue();
        oldManager.triggerCompaction(false);
        assertThat(oldManager.getCompactionResult(true)).isPresent();
    }

    @Test
    public void testDelayedLookupCompactionAfterThreshold()
            throws ExecutionException, InterruptedException {
        List<LevelMinMax> inputs =
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 3, 6),
                        new LevelMinMax(0, 6, 8),
                        new LevelMinMax(1, 1, 4),
                        new LevelMinMax(1, 6, 8),
                        new LevelMinMax(1, 10, 10),
                        new LevelMinMax(2, 1, 3),
                        new LevelMinMax(2, 4, 6));
        List<DataFileMeta> files = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i++) {
            LevelMinMax minMax = inputs.get(i);
            files.add(minMax.toFile(i));
        }

        Levels levels = new Levels(comparator, files, 3);

        CompactStrategy strategy = new ForceUpLevel0Compaction(new UniversalCompaction(0, 0, 5));

        DelayedLookupCompactManager newManager =
                new DelayedLookupCompactManager(
                        service,
                        levels,
                        strategy,
                        comparator,
                        2,
                        Integer.MAX_VALUE,
                        new TestRewriter(true),
                        null,
                        null,
                        false,
                        LocalDateTime.now().minusDays(1),
                        Duration.ofDays(2),
                        5,
                        5);

        // compaction will trigger immediately for new partitions
        assertThat(newManager.isDelayedCompactPartition()).isFalse();
        assertThat(newManager.shouldTriggerDelayedLookupCompact()).isFalse();
        newManager.triggerCompaction(false);
        assertThat(newManager.getCompactionResult(true)).isPresent();
        assertThat(newManager.hasDelayedCompact()).isFalse();

        // stop-trigger not working for new partitions
        newManager.triggerCompaction(false);
        newManager.triggerCompaction(false);
        newManager.triggerCompaction(false);
        newManager.triggerCompaction(false);
        newManager.triggerCompaction(false);
        newManager.triggerCompaction(false);
        assertThat(newManager.shouldTriggerDelayedLookupCompact()).isFalse();
    }
}
