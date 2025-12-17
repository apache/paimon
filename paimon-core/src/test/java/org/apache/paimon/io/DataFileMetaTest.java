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

package org.apache.paimon.io;

import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap32;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;

/** Test for {@link DataFileMeta}. */
public class DataFileMetaTest {

    private DataFileMeta createFileWithRowId(long rowCount, long firstRowId) {
        return DataFileMeta.create(
                "test.parquet",
                1024L,
                rowCount,
                DataFileMeta.EMPTY_MIN_KEY,
                DataFileMeta.EMPTY_MAX_KEY,
                EMPTY_STATS,
                EMPTY_STATS,
                0L,
                0L,
                0L,
                0,
                Collections.emptyList(),
                0L,
                null,
                FileSource.APPEND,
                null,
                null,
                firstRowId,
                null);
    }

    @Test
    public void testToFileSelectionBasic() {
        // File has rows [1000, 1999] (1000 rows total, firstRowId = 1000)
        DataFileMeta file = createFileWithRowId(1000, 1000L);

        // Select ranges [1100, 1200], [1300, 1400]
        List<Range> ranges =
                Arrays.asList(new Range(1100, 1200), new Range(1300, 1400), new Range(1500, 1600));

        RoaringBitmap32 selection = file.toFileSelection(ranges);

        Assertions.assertThat(selection).isNotNull();
        // Should select rows at positions [100-200], [300-400], [500-600] (0-based within file)
        Assertions.assertThat(selection.getCardinality()).isEqualTo(303); // 101 + 101 + 101

        // Verify specific positions
        Assertions.assertThat(selection.contains(100)).isTrue(); // row 1100
        Assertions.assertThat(selection.contains(200)).isTrue(); // row 1200
        Assertions.assertThat(selection.contains(250)).isFalse(); // not selected
        Assertions.assertThat(selection.contains(300)).isTrue(); // row 1300
        Assertions.assertThat(selection.contains(400)).isTrue(); // row 1400
    }

    @Test
    public void testToFileSelectionPartialOverlap() {
        // File has rows [100, 199] (100 rows total, firstRowId = 100)
        DataFileMeta file = createFileWithRowId(100, 100L);

        // Select range [50, 150] which partially overlaps
        List<Range> ranges = Collections.singletonList(new Range(50, 150));

        RoaringBitmap32 selection = file.toFileSelection(ranges);

        Assertions.assertThat(selection).isNotNull();
        // Should select rows [100, 150] -> positions [0, 50] within file
        Assertions.assertThat(selection.getCardinality()).isEqualTo(51);
        Assertions.assertThat(selection.contains(0)).isTrue();
        Assertions.assertThat(selection.contains(50)).isTrue();
        Assertions.assertThat(selection.contains(51)).isFalse();
    }

    @Test
    public void testToFileSelectionNoOverlap() {
        // File has rows [100, 199]
        DataFileMeta file = createFileWithRowId(100, 100L);

        // Select range [300, 400] which doesn't overlap
        List<Range> ranges = Collections.singletonList(new Range(300, 400));

        RoaringBitmap32 selection = file.toFileSelection(ranges);

        Assertions.assertThat(selection).isNotNull();
        Assertions.assertThat(selection.isEmpty()).isTrue();
    }

    @Test
    public void testToFileSelectionSelectAll() {
        // File has rows [1000, 1099] (100 rows total)
        DataFileMeta file = createFileWithRowId(100, 1000L);

        // Select the entire file range [1000, 1099]
        List<Range> ranges = Collections.singletonList(new Range(1000, 1099));

        RoaringBitmap32 selection = file.toFileSelection(ranges);

        // When all rows are selected, should return null
        Assertions.assertThat(selection).isNull();
    }

    @Test
    public void testToFileSelectionSelectAllWithLargerRange() {
        // File has rows [1000, 1099] (100 rows total)
        DataFileMeta file = createFileWithRowId(100, 1000L);

        // Select range that covers entire file and more [900, 1200]
        List<Range> ranges = Collections.singletonList(new Range(900, 1200));

        RoaringBitmap32 selection = file.toFileSelection(ranges);

        // When all rows are selected, should return null
        Assertions.assertThat(selection).isNull();
    }

    @Test
    public void testToFileSelectionEmptyRanges() {
        DataFileMeta file = createFileWithRowId(100, 1000L);

        RoaringBitmap32 selection = file.toFileSelection(Collections.emptyList());

        Assertions.assertThat(selection).isNotNull();
        Assertions.assertThat(selection.isEmpty()).isTrue();
    }

    @Test
    public void testToFileSelectionNullRanges() {
        DataFileMeta file = createFileWithRowId(100, 1000L);

        RoaringBitmap32 selection = file.toFileSelection(null);

        Assertions.assertThat(selection).isNull();
    }

    @Test
    public void testToFileSelectionWithoutFirstRowId() {
        // Create file without firstRowId
        DataFileMeta file =
                DataFileMeta.create(
                        "test.parquet",
                        1024L,
                        100L,
                        DataFileMeta.EMPTY_MIN_KEY,
                        DataFileMeta.EMPTY_MAX_KEY,
                        EMPTY_STATS,
                        EMPTY_STATS,
                        0L,
                        0L,
                        0L,
                        0,
                        Collections.emptyList(),
                        0L,
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        null, // firstRowId is null
                        null);

        List<Range> ranges = Collections.singletonList(new Range(0, 10));

        Assertions.assertThatThrownBy(() -> file.toFileSelection(ranges))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("firstRowId is null");
    }

    @Test
    public void testToFileSelectionMultipleRangesUnsorted() {
        // File has rows [0, 999]
        DataFileMeta file = createFileWithRowId(1000, 0L);

        // Multiple unsorted ranges
        List<Range> ranges =
                Arrays.asList(
                        new Range(500, 600),
                        new Range(100, 200),
                        new Range(800, 850),
                        new Range(300, 400));

        RoaringBitmap32 selection = file.toFileSelection(ranges);

        Assertions.assertThat(selection).isNotNull();
        // 101 + 101 + 51 + 101 = 354
        Assertions.assertThat(selection.getCardinality()).isEqualTo(354);
    }

    @Test
    public void testToFileSelectionOverlappingRanges() {
        // File has rows [0, 999]
        DataFileMeta file = createFileWithRowId(1000, 0L);

        // Overlapping ranges
        List<Range> ranges = Arrays.asList(new Range(100, 300), new Range(200, 400));

        RoaringBitmap32 selection = file.toFileSelection(ranges);

        Assertions.assertThat(selection).isNotNull();
        // Union of [100, 300] and [200, 400] is [100, 400] = 301 rows
        Assertions.assertThat(selection.getCardinality()).isEqualTo(301);
    }

    @Test
    public void testToFileSelectionAtStart() {
        // File has rows [1000, 1999]
        DataFileMeta file = createFileWithRowId(1000, 1000L);

        // Select at the start
        List<Range> ranges = Collections.singletonList(new Range(1000, 1050));

        RoaringBitmap32 selection = file.toFileSelection(ranges);

        Assertions.assertThat(selection).isNotNull();
        Assertions.assertThat(selection.getCardinality()).isEqualTo(51);
        Assertions.assertThat(selection.contains(0)).isTrue();
        Assertions.assertThat(selection.contains(50)).isTrue();
        Assertions.assertThat(selection.contains(51)).isFalse();
    }

    @Test
    public void testToFileSelectionAtEnd() {
        // File has rows [1000, 1999]
        DataFileMeta file = createFileWithRowId(1000, 1000L);

        // Select at the end
        List<Range> ranges = Collections.singletonList(new Range(1950, 1999));

        RoaringBitmap32 selection = file.toFileSelection(ranges);

        Assertions.assertThat(selection).isNotNull();
        Assertions.assertThat(selection.getCardinality()).isEqualTo(50);
        Assertions.assertThat(selection.contains(950)).isTrue();
        Assertions.assertThat(selection.contains(999)).isTrue();
        Assertions.assertThat(selection.contains(949)).isFalse();
    }

    @Test
    public void testToFileSelectionSingleRow() {
        // File has rows [1000, 1999]
        DataFileMeta file = createFileWithRowId(1000, 1000L);

        // Select single row
        List<Range> ranges = Collections.singletonList(new Range(1500, 1500));

        RoaringBitmap32 selection = file.toFileSelection(ranges);

        Assertions.assertThat(selection).isNotNull();
        Assertions.assertThat(selection.getCardinality()).isEqualTo(1);
        Assertions.assertThat(selection.contains(500)).isTrue();
    }

    @Test
    public void testToFileSelectionSingleRowFile() {
        // File has only 1 row at position 1000
        DataFileMeta file = createFileWithRowId(1, 1000L);

        // Select that row
        List<Range> ranges = Collections.singletonList(new Range(1000, 1000));

        RoaringBitmap32 selection = file.toFileSelection(ranges);

        // Selecting entire single-row file should return null
        Assertions.assertThat(selection).isNull();
    }

    @Test
    public void testToFileSelectionAdjacentRanges() {
        // File has rows [0, 999]
        DataFileMeta file = createFileWithRowId(1000, 0L);

        // Adjacent ranges
        List<Range> ranges = Arrays.asList(new Range(100, 200), new Range(201, 300));

        RoaringBitmap32 selection = file.toFileSelection(ranges);

        Assertions.assertThat(selection).isNotNull();
        // 101 + 100 = 201
        Assertions.assertThat(selection.getCardinality()).isEqualTo(201);
        Assertions.assertThat(selection.contains(200)).isTrue();
        Assertions.assertThat(selection.contains(201)).isTrue();
    }
}
