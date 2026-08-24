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

package org.apache.paimon.index.pk;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PrimaryKeyIndexLevels}. */
class PrimaryKeyIndexLevelsTest {

    @Test
    void testPlansCompleteMissingDataLevel() {
        PrimaryKeyIndexLevels<TestUnit> levels =
                new PrimaryKeyIndexLevels<>(TestUnit::dataLevel, TestUnit::sources);
        DataFileMeta dataB = dataFile("data-b", 20, 2);
        DataFileMeta dataA = dataFile("data-a", 10, 2);

        PrimaryKeyIndexLevels.Plan<TestUnit> plan =
                levels.pick(Collections.emptyList(), active(dataB, dataA)).get();

        assertThat(plan.dataLevel()).isEqualTo(2);
        assertThat(plan.inputUnits()).isEmpty();
        assertThat(plan.sourceFiles()).containsExactly(dataA, dataB);
    }

    @Test
    void testValidatesPlanAgainstCurrentCompleteLevel() {
        PrimaryKeyIndexLevels<TestUnit> levels =
                new PrimaryKeyIndexLevels<>(TestUnit::dataLevel, TestUnit::sources);
        DataFileMeta dataA = dataFile("data-a", 10, 2);
        PrimaryKeyIndexLevels.Plan<TestUnit> plan =
                levels.pick(Collections.emptyList(), active(dataA)).get();

        assertThat(levels.isCurrent(plan, active(dataA))).isTrue();
        assertThat(levels.isCurrent(plan, active(dataA, dataFile("data-b", 20, 2)))).isFalse();
    }

    @Test
    void testRebuildsMismatchedCompleteLevel() {
        PrimaryKeyIndexLevels<TestUnit> levels =
                new PrimaryKeyIndexLevels<>(TestUnit::dataLevel, TestUnit::sources);
        DataFileMeta dataA = dataFile("data-a", 30, 2);
        DataFileMeta dataB = dataFile("data-b", 40, 2);
        TestUnit partial = unit("partial", 2, dataA);

        PrimaryKeyIndexLevels.Plan<TestUnit> plan =
                levels.pick(Collections.singletonList(partial), active(dataA, dataB)).get();

        assertThat(plan.dataLevel()).isEqualTo(2);
        assertThat(plan.inputUnits()).containsExactly(partial);
        assertThat(plan.sourceFiles()).containsExactly(dataA, dataB);
    }

    @Test
    void testDropsLevelWithoutActiveData() {
        PrimaryKeyIndexLevels<TestUnit> levels =
                new PrimaryKeyIndexLevels<>(TestUnit::dataLevel, TestUnit::sources);
        TestUnit retired = unit("retired", 3, dataFile("data-a", 40, 3));

        PrimaryKeyIndexLevels.Plan<TestUnit> plan =
                levels.pick(Collections.singletonList(retired), Collections.emptyMap()).get();

        assertThat(plan.dataLevel()).isEqualTo(3);
        assertThat(plan.inputUnits()).containsExactly(retired);
        assertThat(plan.sourceFiles()).isEmpty();
    }

    @Test
    void testExactLevelsNeedNoWork() {
        PrimaryKeyIndexLevels<TestUnit> levels =
                new PrimaryKeyIndexLevels<>(TestUnit::dataLevel, TestUnit::sources);
        DataFileMeta dataA = dataFile("data-a", 50, 2);
        TestUnit current = unit("current", 2, dataA);

        assertThat(levels.pick(Collections.singletonList(current), active(dataA))).isEmpty();
    }

    @Test
    void testRejectsDuplicateUnitsForOneDataLevel() {
        PrimaryKeyIndexLevels<TestUnit> levels =
                new PrimaryKeyIndexLevels<>(TestUnit::dataLevel, TestUnit::sources);
        DataFileMeta dataA = dataFile("data-a", 10, 2);
        TestUnit unitA = unit("unit-a", 2, dataA);
        TestUnit unitB = unit("unit-b", 2, dataA);

        assertThatThrownBy(() -> levels.pick(Arrays.asList(unitA, unitB), active(dataA)))
                .hasMessageContaining("data level 2");
    }

    private static TestUnit unit(String id, int dataLevel, DataFileMeta... files) {
        return new TestUnit(
                id,
                dataLevel,
                Arrays.stream(files)
                        .map(
                                file ->
                                        new PrimaryKeyIndexSourceFile(
                                                file.fileName(), file.rowCount()))
                        .collect(java.util.stream.Collectors.toList()));
    }

    private static Map<String, DataFileMeta> active(DataFileMeta... files) {
        Map<String, DataFileMeta> active = new LinkedHashMap<>();
        for (DataFileMeta file : files) {
            active.put(file.fileName(), file);
        }
        return active;
    }

    private static DataFileMeta dataFile(String fileName, long rowCount) {
        return dataFile(fileName, rowCount, 1);
    }

    private static DataFileMeta dataFile(String fileName, long rowCount, int level) {
        return DataFileMeta.forAppend(
                        fileName,
                        100,
                        rowCount,
                        SimpleStats.EMPTY_STATS,
                        0,
                        0,
                        1,
                        Collections.emptyList(),
                        null,
                        FileSource.COMPACT,
                        null,
                        null,
                        null,
                        null)
                .upgrade(level);
    }

    private static final class TestUnit {

        private final String id;
        private final int dataLevel;
        private final List<PrimaryKeyIndexSourceFile> sources;

        private TestUnit(String id, int dataLevel, List<PrimaryKeyIndexSourceFile> sources) {
            this.id = id;
            this.dataLevel = dataLevel;
            this.sources = sources;
        }

        private int dataLevel() {
            return dataLevel;
        }

        private List<PrimaryKeyIndexSourceFile> sources() {
            return sources;
        }
    }
}
