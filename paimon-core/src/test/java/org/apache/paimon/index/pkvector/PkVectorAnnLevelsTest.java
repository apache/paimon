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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for derived primary-key vector ANN levels. */
class PkVectorAnnLevelsTest {

    @Test
    void testPicksSimilarSegmentsAcrossAbsoluteBoundary() {
        PkVectorAnnLevels levels = new PkVectorAnnLevels(3, 0.2);
        DataFileMeta dataA = dataFile("data-a", 99);
        DataFileMeta dataB = dataFile("data-b", 100);
        DataFileMeta dataC = dataFile("data-c", 101);
        IndexFileMeta annA = segment("ann-a", dataA);
        IndexFileMeta annB = segment("ann-b", dataB);
        IndexFileMeta annC = segment("ann-c", dataC);
        Map<String, DataFileMeta> active = new LinkedHashMap<>();
        active.put(dataA.fileName(), dataA);
        active.put(dataB.fileName(), dataB);
        active.put(dataC.fileName(), dataC);

        PkVectorAnnLevels.Plan plan = levels.pick(Arrays.asList(annC, annA, annB), active).get();

        assertThat(plan.inputSegments()).containsExactly(annA, annB, annC);
    }

    @Test
    void testPicksLevelZeroSegmentsAtFanout() {
        PkVectorAnnLevels levels = new PkVectorAnnLevels(3, 0.2);
        DataFileMeta dataA = dataFile("data-a", 30);
        DataFileMeta dataB = dataFile("data-b", 40);
        DataFileMeta dataC = dataFile("data-c", 50);
        IndexFileMeta annA = segment("ann-a", dataA);
        IndexFileMeta annB = segment("ann-b", dataB);
        IndexFileMeta annC = segment("ann-c", dataC);
        Map<String, DataFileMeta> active = new LinkedHashMap<>();
        active.put(dataA.fileName(), dataA);
        active.put(dataB.fileName(), dataB);
        active.put(dataC.fileName(), dataC);

        PkVectorAnnLevels.Plan plan = levels.pick(Arrays.asList(annC, annA, annB), active).get();

        assertThat(plan.inputSegments()).containsExactly(annA, annB, annC);
        assertThat(plan.sourceFiles()).containsExactly(dataA, dataB, dataC);
    }

    @Test
    void testPicksSegmentAboveStaleRatio() {
        PkVectorAnnLevels levels = new PkVectorAnnLevels(5, 0.2);
        DataFileMeta retired = dataFile("retired", 60);
        DataFileMeta activeData = dataFile("active", 60);
        IndexFileMeta ann = segment("ann", retired, activeData);
        Map<String, DataFileMeta> active =
                Collections.singletonMap(activeData.fileName(), activeData);

        PkVectorAnnLevels.Plan plan = levels.pick(Collections.singletonList(ann), active).get();

        assertThat(plan.inputSegments()).containsExactly(ann);
        assertThat(plan.sourceFiles()).containsExactly(activeData);
    }

    private static IndexFileMeta segment(String fileName, long rowCount) {
        return segment(fileName, dataFile(fileName + "-data", rowCount));
    }

    private static IndexFileMeta segment(String fileName, DataFileMeta source) {
        return segment(fileName, new DataFileMeta[] {source});
    }

    private static IndexFileMeta segment(String fileName, DataFileMeta... sources) {
        long rowCount = Arrays.stream(sources).mapToLong(DataFileMeta::rowCount).sum();
        byte[] sourceMeta =
                new PrimaryKeyIndexSourceMeta(
                                Arrays.stream(sources)
                                        .map(
                                                source ->
                                                        new PrimaryKeyIndexSourceFile(
                                                                source.fileName(),
                                                                source.rowCount()))
                                        .collect(java.util.stream.Collectors.toList()))
                        .serialize();
        return new IndexFileMeta(
                "test-vector-ann",
                fileName,
                100,
                rowCount,
                new GlobalIndexMeta(0, rowCount - 1, 7, null, new byte[] {1}, sourceMeta),
                null);
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
}
