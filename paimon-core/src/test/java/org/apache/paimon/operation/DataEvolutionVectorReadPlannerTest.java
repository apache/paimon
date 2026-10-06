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

package org.apache.paimon.operation;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataEvolutionVectorReadPlanner}. */
public class DataEvolutionVectorReadPlannerTest {

    private static final RowType NORMAL_TYPE = RowType.of(new DataField(0, "id", DataTypes.INT()));
    private static final RowType VECTOR_TYPE =
            RowType.of(new DataField(1, "embedding", DataTypes.BYTES()));
    private static final RowType E1_TYPE = RowType.of(new DataField(2, "e1", DataTypes.BYTES()));
    private static final RowType E2_TYPE = RowType.of(new DataField(3, "e2", DataTypes.BYTES()));

    @Test
    public void testRolledVectorFilesCoveringTheGroupKeepSequentialRead() {
        // A rolled vector column: two files that together cover the normal file's [0, 9].
        List<DataFileMeta> files =
                Arrays.asList(
                        file("data-0.parquet", "id", 0L, 10L, 1L),
                        file("data-1.vector.lance", "embedding", 0L, 5L, 2L),
                        file("data-2.vector.lance", "embedding", 5L, 5L, 2L));

        assertThat(DataEvolutionVectorReadPlanner.plan(files, VECTOR_TYPE, this::rowType, null))
                .isNull();
    }

    @Test
    public void testVectorColumnCoveringPartOfTheGroupPlansRanges() {
        List<DataFileMeta> files =
                Arrays.asList(
                        file("data-0.parquet", "id", 0L, 10L, 1L),
                        file("data-1.vector.lance", "embedding", 0L, 5L, 2L));

        List<DataEvolutionVectorReadPlanner.ReadRange> ranges =
                DataEvolutionVectorReadPlanner.plan(files, VECTOR_TYPE, this::rowType, null);

        assertThat(ranges).isNotNull();
        assertThat(ranges)
                .extracting(r -> r.range)
                .containsExactly(new Range(0, 4), new Range(5, 9));
        assertThat(ranges.get(0).files)
                .extracting(DataFileMeta::fileName)
                .containsExactly("data-1.vector.lance");
        assertThat(ranges.get(1).files).isEmpty();
    }

    @Test
    public void testNewerVectorFileOverPartOfTheColumnPlansRanges() {
        // [0, 9] then a newer update of [0, 4]: the sequential bunch would keep only [0, 4].
        List<DataFileMeta> files =
                Arrays.asList(
                        file("data-0.parquet", "id", 0L, 10L, 1L),
                        file("data-1.vector.lance", "embedding", 0L, 10L, 2L),
                        file("data-2.vector.lance", "embedding", 0L, 5L, 3L));

        List<DataEvolutionVectorReadPlanner.ReadRange> ranges =
                DataEvolutionVectorReadPlanner.plan(files, VECTOR_TYPE, this::rowType, null);

        assertThat(ranges).isNotNull();
        assertThat(ranges)
                .extracting(r -> r.range)
                .containsExactly(new Range(0, 4), new Range(5, 9));
        assertThat(ranges.get(0).files)
                .extracting(DataFileMeta::fileName)
                .containsExactly("data-2.vector.lance");
        assertThat(ranges.get(1).files)
                .extracting(DataFileMeta::fileName)
                .containsExactly("data-1.vector.lance");
    }

    @Test
    public void testNewerVectorFileOverlappingTheTailPlansRanges() {
        // [0, 6] then a newer update of [3, 9]: the sequential bunch would reject the overlap.
        List<DataFileMeta> files =
                Arrays.asList(
                        file("data-0.parquet", "id", 0L, 10L, 1L),
                        file("data-1.vector.lance", "embedding", 0L, 7L, 2L),
                        file("data-2.vector.lance", "embedding", 3L, 7L, 3L));

        List<DataEvolutionVectorReadPlanner.ReadRange> ranges =
                DataEvolutionVectorReadPlanner.plan(files, VECTOR_TYPE, this::rowType, null);

        assertThat(ranges).isNotNull();
        assertThat(ranges)
                .extracting(r -> r.range)
                .containsExactly(new Range(0, 2), new Range(3, 9));
    }

    @Test
    public void testSkippedFileNewerThanTheNextKeptFilePlansRanges() {
        // [0, 4] s4 and [5, 9] s2 tile the group, but [2, 7] s3 is newer than [5, 9] at [5, 7].
        List<DataFileMeta> files =
                Arrays.asList(
                        file("data-0.parquet", "id", 0L, 10L, 1L),
                        file("data-1.vector.lance", "embedding", 0L, 10L, 1L),
                        file("data-2.vector.lance", "embedding", 5L, 5L, 2L),
                        file("data-3.vector.lance", "embedding", 2L, 6L, 3L),
                        file("data-4.vector.lance", "embedding", 0L, 5L, 4L));

        List<DataEvolutionVectorReadPlanner.ReadRange> ranges =
                DataEvolutionVectorReadPlanner.plan(files, VECTOR_TYPE, this::rowType, null);

        assertThat(ranges).isNotNull();
        assertThat(ranges)
                .extracting(r -> r.range)
                .containsExactly(new Range(0, 4), new Range(5, 7), new Range(8, 9));
        assertThat(ranges.get(1).files)
                .extracting(DataFileMeta::fileName)
                .containsExactly("data-3.vector.lance");
    }

    @Test
    public void testFullRewriteRolledOverAnOlderFileKeepsSequentialRead() {
        // A newer full rewrite rolled into [0, 6] and [7, 9] fully shadows the older [0, 9].
        List<DataFileMeta> files =
                Arrays.asList(
                        file("data-0.parquet", "id", 0L, 10L, 1L),
                        file("data-1.vector.lance", "embedding", 0L, 10L, 1L),
                        file("data-2.vector.lance", "embedding", 0L, 7L, 2L),
                        file("data-3.vector.lance", "embedding", 7L, 3L, 2L));

        assertThat(DataEvolutionVectorReadPlanner.plan(files, VECTOR_TYPE, this::rowType, null))
                .isNull();
    }

    @Test
    public void testCoverageIsJudgedPerVectorColumn() {
        // e1 covers [0, 9] and e2 only [0, 4]: their union spans the group, but e2's rows [5, 9]
        // still need NULL-filling.
        RowType readType = RowType.of(E1_TYPE.getFields().get(0), E2_TYPE.getFields().get(0));
        List<DataFileMeta> files =
                Arrays.asList(
                        file("data-0.parquet", "id", 0L, 10L, 1L),
                        file("data-1.vector.lance", "e1", 0L, 10L, 3L),
                        file("data-2.vector.lance", "e2", 0L, 5L, 2L));

        List<DataEvolutionVectorReadPlanner.ReadRange> ranges =
                DataEvolutionVectorReadPlanner.plan(files, readType, this::rowType, null);

        assertThat(ranges).isNotNull();
        assertThat(ranges)
                .extracting(r -> r.range)
                .containsExactly(new Range(0, 4), new Range(5, 9));
    }

    @Test
    public void testRolledVectorFilesPrunedByRowRangesKeepSequentialRead() {
        // Rolled [0, 3], [4, 6], [7, 9]; the scan drops [4, 6] because no selected row is in it.
        List<DataFileMeta> files =
                Arrays.asList(
                        file("data-0.parquet", "id", 0L, 10L, 1L),
                        file("data-1.vector.lance", "embedding", 0L, 4L, 1L),
                        file("data-3.vector.lance", "embedding", 7L, 3L, 1L));
        List<Range> rowRanges = Arrays.asList(new Range(1, 1), new Range(8, 8));

        assertThat(
                        DataEvolutionVectorReadPlanner.plan(
                                files, VECTOR_TYPE, this::rowType, rowRanges))
                .isNull();
    }

    @Test
    public void testSelectedRowsOutsideTheVectorColumnPlanRanges() {
        List<DataFileMeta> files =
                Arrays.asList(
                        file("data-0.parquet", "id", 0L, 10L, 1L),
                        file("data-1.vector.lance", "embedding", 0L, 5L, 2L));

        // No selected row falls outside [0, 4], so the sequential bunch reads every one of them.
        assertThat(
                        DataEvolutionVectorReadPlanner.plan(
                                files,
                                VECTOR_TYPE,
                                this::rowType,
                                Collections.singletonList(new Range(1, 1))))
                .isNull();

        List<DataEvolutionVectorReadPlanner.ReadRange> ranges =
                DataEvolutionVectorReadPlanner.plan(
                        files,
                        VECTOR_TYPE,
                        this::rowType,
                        Arrays.asList(new Range(1, 1), new Range(7, 7)));
        assertThat(ranges).isNotNull();
        assertThat(ranges)
                .extracting(r -> r.range)
                .containsExactly(new Range(0, 4), new Range(5, 9));
        assertThat(ranges.get(1).files).isEmpty();
    }

    @Test
    public void testSelectedRowBeforeTheVectorColumnPlansRanges() {
        // embedding covers only [5, 9].
        List<DataFileMeta> files =
                Arrays.asList(
                        file("data-0.parquet", "id", 0L, 10L, 1L),
                        file("data-1.vector.lance", "embedding", 5L, 5L, 2L));

        assertThat(plan(files, new Range(5, 5), new Range(7, 7))).isNull();
        assertThat(plan(files, new Range(1, 1), new Range(7, 7))).isNotNull();
        assertThat(plan(files, new Range(4, 4), new Range(7, 7))).isNotNull();
    }

    @Test
    public void testSelectedRowInAGapOrTheTailPlansRanges() {
        // embedding covers [0, 2] and [6, 8]; [3, 5] and [9, 9] are uncovered.
        List<DataFileMeta> files =
                Arrays.asList(
                        file("data-0.parquet", "id", 0L, 10L, 1L),
                        file("data-1.vector.lance", "embedding", 0L, 3L, 2L),
                        file("data-2.vector.lance", "embedding", 6L, 3L, 3L));

        assertThat(plan(files, new Range(1, 1), new Range(7, 7))).isNull();
        assertThat(plan(files, new Range(1, 1), new Range(3, 3))).isNotNull();
        assertThat(plan(files, new Range(1, 1), new Range(5, 5))).isNotNull();
        assertThat(plan(files, new Range(1, 1), new Range(9, 9))).isNotNull();
    }

    private List<DataEvolutionVectorReadPlanner.ReadRange> plan(
            List<DataFileMeta> files, Range... rowRanges) {
        return DataEvolutionVectorReadPlanner.plan(
                files, VECTOR_TYPE, this::rowType, Arrays.asList(rowRanges));
    }

    private RowType rowType(DataFileMeta file) {
        if (file.writeCols().contains("embedding")) {
            return VECTOR_TYPE;
        } else if (file.writeCols().contains("e1")) {
            return E1_TYPE;
        } else if (file.writeCols().contains("e2")) {
            return E2_TYPE;
        }
        return NORMAL_TYPE;
    }

    private static DataFileMeta file(
            String fileName, String column, long firstRowId, long rowCount, long sequence) {
        return DataFileMeta.create(
                fileName,
                100L,
                rowCount,
                row(0),
                row(0),
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                sequence,
                sequence,
                0L,
                0,
                null,
                null,
                FileSource.APPEND,
                null,
                firstRowId,
                Collections.singletonList(column));
    }
}
