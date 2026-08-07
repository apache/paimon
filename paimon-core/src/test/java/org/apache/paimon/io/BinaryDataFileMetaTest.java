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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BinaryDataFileMeta}. */
public class BinaryDataFileMetaTest {

    @Test
    void testImplementsProjectedDataFileMeta() {
        DataFileMeta expected =
                DataFileMeta.forAppend(
                        "data.parquet",
                        123L,
                        5L,
                        SimpleStats.EMPTY_STATS,
                        2L,
                        3L,
                        4L,
                        Arrays.asList("extra-1", "extra-2"),
                        new byte[] {1, 2},
                        FileSource.COMPACT,
                        Collections.singletonList("value_col"),
                        "external/dir/data.parquet",
                        10L,
                        Collections.singletonList("write_col"));
        BinaryDataFileMeta actual =
                BinaryDataFileMeta.Projection.create(DataFileMeta.SCHEMA)
                        .createDataFile()
                        .replace(new DataFileMetaSerializer().toRow(expected));

        assertThat(actual.fileName()).isEqualTo(expected.fileName());
        assertThat(actual.fileNameBinary()).isEqualTo(BinaryString.fromString(expected.fileName()));
        assertThat(actual.fileSize()).isEqualTo(expected.fileSize());
        assertThat(actual.rowCount()).isEqualTo(expected.rowCount());
        assertThat(actual.deleteRowCount()).isEqualTo(expected.deleteRowCount());
        assertThat(actual.minKey()).isEqualTo(expected.minKey());
        assertThat(actual.maxKey()).isEqualTo(expected.maxKey());
        assertThat(actual.keyStats()).isEqualTo(expected.keyStats());
        assertThat(actual.valueStats()).isEqualTo(expected.valueStats());
        assertThat(actual.minSequenceNumber()).isEqualTo(expected.minSequenceNumber());
        assertThat(actual.maxSequenceNumber()).isEqualTo(expected.maxSequenceNumber());
        assertThat(actual.schemaId()).isEqualTo(expected.schemaId());
        assertThat(actual.level()).isEqualTo(expected.level());
        assertThat(actual.extraFiles()).isEqualTo(expected.extraFiles());
        assertThat(actual.extraFileCount()).isEqualTo(2);
        assertThat(actual.extraFile(1).toString()).isEqualTo("extra-2");
        assertThat(actual.creationTime()).isEqualTo(expected.creationTime());
        assertThat(actual.creationTimeEpochMillis()).isEqualTo(expected.creationTimeEpochMillis());
        assertThat(actual.fileFormat()).isEqualTo("parquet");
        assertThat(actual.hasEmbeddedIndex()).isTrue();
        assertThat(actual.embeddedIndex()).containsExactly(1, 2);
        assertThat(actual.fileSource()).contains(FileSource.COMPACT);
        assertThat(actual.valueStatsCols()).containsExactly("value_col");
        assertThat(actual.hasExternalPath()).isTrue();
        assertThat(actual.externalPath()).isEqualTo(expected.externalPath());
        assertThat(actual.externalPathDir()).isEqualTo(expected.externalPathDir());
        assertThat(actual.hasFirstRowId()).isTrue();
        assertThat(actual.firstRowId()).isEqualTo(10L);
        assertThat(actual.nonNullFirstRowId()).isEqualTo(10L);
        assertThat(actual.writeCols()).containsExactly("write_col");
        assertThat(actual.containsWriteColumn(BinaryString.fromString("write_col"))).isTrue();
        assertThat(actual.containsWriteColumn(BinaryString.fromString("other"))).isFalse();
        assertThat(actual.toFileSelection(Collections.singletonList(new Range(11L, 12L))))
                .isEqualTo(RoaringBitmap32.bitmapOf(1, 2));

        assertUnsupported(() -> actual.upgrade(1), "upgrade(int)");
        assertUnsupported(() -> actual.rename("renamed.parquet"), "rename(String)");
        assertUnsupported(actual::copyWithoutStats, "copyWithoutStats()");
        assertUnsupported(
                () -> actual.assignSequenceNumber(4L, 5L), "assignSequenceNumber(long, long)");
        assertUnsupported(() -> actual.assignFirstRowId(20L), "assignFirstRowId(long)");
        assertUnsupported(() -> actual.newFirstRowId(20L), "newFirstRowId(Long)");
        assertUnsupported(() -> actual.copy(Collections.emptyList()), "copy(List)");
        assertUnsupported(() -> actual.newExternalPath("new/path"), "newExternalPath(String)");
        assertUnsupported(() -> actual.copy(new byte[] {3}), "copy(byte[])");
    }

    @Test
    void testBindsArbitraryProjectedSchema() {
        RowType projectedType =
                DataFileMeta.SCHEMA.project(
                        DataFileMeta.FIRST_ROW_ID,
                        DataFileMeta.FILE_NAME,
                        DataFileMeta.ROW_COUNT,
                        DataFileMeta.WRITE_COLS);
        BinaryDataFileMeta file =
                BinaryDataFileMeta.Projection.create(projectedType)
                        .createDataFile()
                        .replace(
                                GenericRow.of(null, BinaryString.fromString("data.orc"), 7L, null));

        assertThat(file.fileName()).isEqualTo("data.orc");
        assertThat(file.fileFormat()).isEqualTo("orc");
        assertThat(file.rowCount()).isEqualTo(7L);
        assertThat(file.firstRowId()).isNull();
        assertThat(file.writeCols()).isNull();
        assertUnsupported(file::fileSize, DataFileMeta.FILE_SIZE);
    }

    @Test
    void testReusesAndClearsView() {
        RowType projectedType = DataFileMeta.SCHEMA.project(DataFileMeta.FILE_NAME);
        BinaryDataFileMeta file =
                BinaryDataFileMeta.Projection.create(projectedType).createDataFile();

        file.replace(GenericRow.of(BinaryString.fromString("first.parquet")));
        assertThat(file.fileName()).isEqualTo("first.parquet");
        file.replace(GenericRow.of(BinaryString.fromString("second.parquet")));
        assertThat(file.fileName()).isEqualTo("second.parquet");

        file.clear();
        assertThatThrownBy(file::fileName)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not backed by a row");
    }

    @Test
    void testRejectsPartiallyProjectedNestedField() {
        DataField partialKeyStats =
                DataFileMeta.SCHEMA
                        .getField(DataFileMeta.KEY_STATS)
                        .newType(SimpleStats.SCHEMA.project("_MIN_VALUES"));
        RowType projectedType = new RowType(false, Collections.singletonList(partialKeyStats));

        assertThatThrownBy(() -> BinaryDataFileMeta.Projection.create(projectedType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DataFileMeta.KEY_STATS);
    }

    private static void assertUnsupported(ThrowingSupplier call, String value) {
        assertThatThrownBy(call::get)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(value);
    }

    private interface ThrowingSupplier {

        Object get();
    }
}
