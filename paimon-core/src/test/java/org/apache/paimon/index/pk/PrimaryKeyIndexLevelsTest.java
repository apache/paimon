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

/** Tests for {@link PrimaryKeyIndexLevels}. */
class PrimaryKeyIndexLevelsTest {

    @Test
    void testPicksSimilarLogicalUnitsAtFanout() {
        PrimaryKeyIndexLevels<TestUnit> levels =
                new PrimaryKeyIndexLevels<>(3, 0.2, TestUnit::id, TestUnit::sources);
        DataFileMeta dataA = dataFile("data-a", 30);
        DataFileMeta dataB = dataFile("data-b", 40);
        DataFileMeta dataC = dataFile("data-c", 50);
        TestUnit unitA = unit("unit-a", dataA);
        TestUnit unitB = unit("unit-b", dataB);
        TestUnit unitC = unit("unit-c", dataC);
        Map<String, DataFileMeta> active = active(dataA, dataB, dataC);

        PrimaryKeyIndexLevels.Plan<TestUnit> plan =
                levels.pick(Arrays.asList(unitC, unitA, unitB), active).get();

        assertThat(plan.inputUnits()).containsExactly(unitA, unitB, unitC);
        assertThat(plan.sourceFiles()).containsExactly(dataA, dataB, dataC);
    }

    @Test
    void testPicksUnitAtStaleRatioThreshold() {
        PrimaryKeyIndexLevels<TestUnit> levels =
                new PrimaryKeyIndexLevels<>(5, 0.4, TestUnit::id, TestUnit::sources);
        DataFileMeta retired = dataFile("retired", 40);
        DataFileMeta activeData = dataFile("active", 60);
        TestUnit unit = unit("unit", retired, activeData);

        PrimaryKeyIndexLevels.Plan<TestUnit> plan =
                levels.pick(Collections.singletonList(unit), active(activeData)).get();

        assertThat(plan.inputUnits()).containsExactly(unit);
        assertThat(plan.sourceFiles()).containsExactly(activeData);
    }

    @Test
    void testPicksUnitWithHighestStaleRatio() {
        PrimaryKeyIndexLevels<TestUnit> levels =
                new PrimaryKeyIndexLevels<>(5, 0.2, TestUnit::id, TestUnit::sources);
        DataFileMeta activeA = dataFile("active-a", 50);
        DataFileMeta activeB = dataFile("active-b", 20);
        TestUnit halfStale = unit("unit-a", dataFile("retired-a", 50), activeA);
        TestUnit mostlyStale = unit("unit-b", dataFile("retired-b", 80), activeB);

        PrimaryKeyIndexLevels.Plan<TestUnit> plan =
                levels.pick(Arrays.asList(halfStale, mostlyStale), active(activeA, activeB)).get();

        assertThat(plan.inputUnits()).containsExactly(mostlyStale);
    }

    @Test
    void testBreaksEqualStaleRatioByIdentity() {
        PrimaryKeyIndexLevels<TestUnit> levels =
                new PrimaryKeyIndexLevels<>(5, 0.2, TestUnit::id, TestUnit::sources);
        TestUnit unitA = unit("unit-a", dataFile("retired-a", 10));
        TestUnit unitB = unit("unit-b", dataFile("retired-b", 10));

        PrimaryKeyIndexLevels.Plan<TestUnit> plan =
                levels.pick(Arrays.asList(unitB, unitA), Collections.emptyMap()).get();

        assertThat(plan.inputUnits()).containsExactly(unitA);
        assertThat(plan.sourceFiles()).isEmpty();
    }

    @Test
    void testSaturatesFanoutSizeComparison() {
        PrimaryKeyIndexLevels<TestUnit> levels =
                new PrimaryKeyIndexLevels<>(2, 1.0, TestUnit::id, TestUnit::sources);
        DataFileMeta smaller = dataFile("data-a", Long.MAX_VALUE / 2 + 1);
        DataFileMeta larger = dataFile("data-b", Long.MAX_VALUE - 1);
        TestUnit unitA = unit("unit-a", smaller);
        TestUnit unitB = unit("unit-b", larger);

        PrimaryKeyIndexLevels.Plan<TestUnit> plan =
                levels.pick(Arrays.asList(unitB, unitA), active(smaller, larger)).get();

        assertThat(plan.inputUnits()).containsExactly(unitA, unitB);
    }

    private static TestUnit unit(String id, DataFileMeta... files) {
        return new TestUnit(
                id,
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
                null);
    }

    private static final class TestUnit {

        private final String id;
        private final List<PrimaryKeyIndexSourceFile> sources;

        private TestUnit(String id, List<PrimaryKeyIndexSourceFile> sources) {
            this.id = id;
            this.sources = sources;
        }

        private String id() {
            return id;
        }

        private List<PrimaryKeyIndexSourceFile> sources() {
            return sources;
        }
    }
}
