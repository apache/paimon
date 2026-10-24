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

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.SortedRun;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests that a single {@link MergeTreeCompactManager} serializes compaction per bucket. */
class MergeTreeCompactManagerBucketSerializationTest {

    private ExecutorService executorService;

    @AfterEach
    void tearDown() {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
    }

    @Test
    void testSecondTriggerIgnoredWhileCompactionRunning() throws Exception {
        executorService = Executors.newSingleThreadExecutor();
        AtomicInteger rewriteCalls = new AtomicInteger();
        CountDownLatch rewriteStarted = new CountDownLatch(1);
        CountDownLatch unblockRewrite = new CountDownLatch(1);

        DataFileMeta file1 = DataFileTestUtils.newFile(0, 1, 3, 3L);
        DataFileMeta file2 = DataFileTestUtils.newFile(0, 4, 6, 6L);
        LevelSortedRun run1 = new LevelSortedRun(0, SortedRun.fromSingle(file1));
        LevelSortedRun run2 = new LevelSortedRun(0, SortedRun.fromSingle(file2));
        List<LevelSortedRun> runs = Arrays.asList(run1, run2);

        Levels levels = mock(Levels.class);
        when(levels.levelSortedRuns()).thenReturn(runs);
        when(levels.numberOfLevels()).thenReturn(3);
        when(levels.nonEmptyHighestLevel()).thenReturn(0);

        CompactStrategy strategy = mock(CompactStrategy.class);
        when(strategy.pick(anyInt(), any()))
                .thenReturn(Optional.of(CompactUnit.fromLevelRuns(1, runs)));

        CompactRewriter rewriter = mock(CompactRewriter.class);
        when(rewriter.rewrite(anyInt(), anyBoolean(), any()))
                .thenAnswer(
                        invocation -> {
                            rewriteCalls.incrementAndGet();
                            rewriteStarted.countDown();
                            assertThat(unblockRewrite.await(30, TimeUnit.SECONDS)).isTrue();
                            return new CompactResult(
                                    Collections.emptyList(), Collections.emptyList());
                        });

        MergeTreeCompactManager manager =
                new MergeTreeCompactManager(
                        executorService,
                        levels,
                        strategy,
                        Comparator.comparingInt(row -> row.getInt(0)),
                        1024 * 1024,
                        5,
                        rewriter,
                        null,
                        null,
                        false,
                        false,
                        null,
                        false,
                        false,
                        "");

        manager.triggerCompaction(false);
        assertThat(rewriteStarted.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(manager.compactNotCompleted()).isTrue();

        manager.triggerCompaction(false);
        assertThat(rewriteCalls.get()).isEqualTo(1);

        unblockRewrite.countDown();
        manager.getCompactionResult(true);
        assertThat(manager.compactNotCompleted()).isFalse();
    }
}
