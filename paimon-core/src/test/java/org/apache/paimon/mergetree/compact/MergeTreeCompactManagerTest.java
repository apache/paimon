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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.SortedRun;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MergeTreeCompactManager}. */
public class MergeTreeCompactManagerTest {

    private final Comparator<InternalRow> comparator = Comparator.comparingInt(o -> o.getInt(0));

    private static ExecutorService service;

    @BeforeAll
    public static void before() {
        service = Executors.newSingleThreadExecutor();
    }

    @AfterAll
    public static void after() {
        service.shutdownNow();
        service = null;
    }

    @Test
    public void testOutputToZeroLevel() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 3),
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 1, 8)),
                Arrays.asList(new LevelMinMax(0, 1, 8), new LevelMinMax(0, 1, 3)),
                (numLevels, runs) -> Optional.of(CompactUnit.fromLevelRuns(0, runs.subList(0, 2))),
                false);
    }

    @Test
    public void testCompactToPenultimateLayer() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 3),
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(2, 1, 7)),
                Arrays.asList(new LevelMinMax(1, 1, 5), new LevelMinMax(2, 1, 7)),
                (numLevels, runs) -> Optional.of(CompactUnit.fromLevelRuns(1, runs.subList(0, 2))),
                false);
    }

    @Test
    public void testNoCompaction() throws ExecutionException, InterruptedException {
        innerTest(
                Collections.singletonList(new LevelMinMax(3, 1, 3)),
                Collections.singletonList(new LevelMinMax(3, 1, 3)));
    }

    @Test
    public void testNormal() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 3),
                        new LevelMinMax(1, 1, 5),
                        new LevelMinMax(1, 6, 7)),
                Arrays.asList(new LevelMinMax(2, 1, 5), new LevelMinMax(2, 6, 7)));
    }

    @Test
    public void testUpgrade() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 3),
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 8)),
                Arrays.asList(new LevelMinMax(2, 1, 5), new LevelMinMax(2, 6, 8)));
    }

    @Test
    public void testSmallFiles() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(new LevelMinMax(0, 1, 1), new LevelMinMax(0, 2, 2)),
                Collections.singletonList(new LevelMinMax(2, 1, 2)));
    }

    @Test
    public void testSmallFilesNoCompact() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 6),
                        new LevelMinMax(1, 7, 8),
                        new LevelMinMax(1, 9, 10)),
                Arrays.asList(
                        new LevelMinMax(2, 1, 5),
                        new LevelMinMax(2, 6, 6),
                        new LevelMinMax(2, 7, 8),
                        new LevelMinMax(2, 9, 10)));
    }

    @Test
    public void testSmallFilesCrossLevel() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 6),
                        new LevelMinMax(1, 7, 7),
                        new LevelMinMax(1, 9, 10)),
                Arrays.asList(
                        new LevelMinMax(2, 1, 5),
                        new LevelMinMax(2, 6, 7),
                        new LevelMinMax(2, 9, 10)));
    }

    @Test
    public void testComplex() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 6),
                        new LevelMinMax(1, 1, 4),
                        new LevelMinMax(1, 6, 8),
                        new LevelMinMax(1, 10, 11),
                        new LevelMinMax(2, 1, 3),
                        new LevelMinMax(2, 4, 6)),
                Arrays.asList(new LevelMinMax(2, 1, 8), new LevelMinMax(2, 10, 11)));
    }

    @Test
    public void testSmallInComplex() throws ExecutionException, InterruptedException {
        innerTest(
                Arrays.asList(
                        new LevelMinMax(0, 1, 5),
                        new LevelMinMax(0, 6, 6),
                        new LevelMinMax(1, 1, 4),
                        new LevelMinMax(1, 6, 8),
                        new LevelMinMax(1, 10, 10),
                        new LevelMinMax(2, 1, 3),
                        new LevelMinMax(2, 4, 6)),
                Collections.singletonList(new LevelMinMax(2, 1, 10)));
    }

    private void innerTest(List<LevelMinMax> inputs, List<LevelMinMax> expected)
            throws ExecutionException, InterruptedException {
        innerTest(inputs, expected, testStrategy(), true);
    }

    private void innerTest(
            List<LevelMinMax> inputs,
            List<LevelMinMax> expected,
            CompactStrategy strategy,
            boolean expectedDropDelete)
            throws ExecutionException, InterruptedException {
        List<DataFileMeta> files = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i++) {
            LevelMinMax minMax = inputs.get(i);
            files.add(minMax.toFile(i));
        }
        Levels levels = new Levels(comparator, files, 3);
        MergeTreeCompactManager manager =
                new MergeTreeCompactManager(
                        service,
                        levels,
                        strategy,
                        comparator,
                        2,
                        Integer.MAX_VALUE,
                        new TestRewriter(expectedDropDelete),
                        null,
                        null,
                        false);
        manager.triggerCompaction(false);
        manager.getCompactionResult(true);
        List<LevelMinMax> outputs =
                levels.allFiles().stream().map(LevelMinMax::new).collect(Collectors.toList());
        assertThat(outputs).isEqualTo(expected);
    }

    public static BinaryRow row(int i) {
        return DataFileTestUtils.row(i);
    }

    private CompactStrategy testStrategy() {
        return (numLevels, runs) -> Optional.of(CompactUnit.fromLevelRuns(numLevels - 1, runs));
    }

    private static class TestRewriter extends AbstractCompactRewriter {

        private final boolean expectedDropDelete;

        private TestRewriter(boolean expectedDropDelete) {
            this.expectedDropDelete = expectedDropDelete;
        }

        @Override
        public CompactResult rewrite(
                int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
                throws Exception {
            assertThat(dropDelete).isEqualTo(expectedDropDelete);
            int minKey = Integer.MAX_VALUE;
            int maxKey = Integer.MIN_VALUE;
            long maxSequence = 0;
            for (List<SortedRun> section : sections) {
                for (SortedRun run : section) {
                    for (DataFileMeta file : run.files()) {
                        int min = file.minKey().getInt(0);
                        int max = file.maxKey().getInt(0);
                        if (min < minKey) {
                            minKey = min;
                        }
                        if (max > maxKey) {
                            maxKey = max;
                        }
                        if (file.maxSequenceNumber() > maxSequence) {
                            maxSequence = file.maxSequenceNumber();
                        }
                    }
                }
            }
            return new CompactResult(
                    extractFilesFromSections(sections),
                    Collections.singletonList(newFile(outputLevel, minKey, maxKey, maxSequence)));
        }
    }

    private static class LevelMinMax {

        private final int level;
        private final int min;
        private final int max;

        private LevelMinMax(DataFileMeta file) {
            this.level = file.level();
            this.min = file.minKey().getInt(0);
            this.max = file.maxKey().getInt(0);
        }

        private LevelMinMax(int level, int min, int max) {
            this.level = level;
            this.min = min;
            this.max = max;
        }

        private DataFileMeta toFile(long maxSequence) {
            return newFile(level, min, max, maxSequence);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LevelMinMax that = (LevelMinMax) o;
            return level == that.level && min == that.min && max == that.max;
        }

        @Override
        public int hashCode() {
            return Objects.hash(level, min, max);
        }

        @Override
        public String toString() {
            return "LevelMinMax{" + "level=" + level + ", min=" + min + ", max=" + max + '}';
        }
    }
}
