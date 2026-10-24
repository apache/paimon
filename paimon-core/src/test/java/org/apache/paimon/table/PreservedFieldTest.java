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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataFormatTestUtil;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for preserved retract fields in persisted lookup changelogs. */
class PreservedFieldTest {

    @TempDir java.nio.file.Path tempDir;

    private final String commitUser = UUID.randomUUID().toString();

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "avro"})
    void testDropUnrelatedColumn(String format) throws Exception {
        FileStoreTable table = createTable(format);
        try (IOManager ioManager = IOManager.create(tempDir.resolve("io").toString())) {
            long insertSnapshot =
                    write(
                            table,
                            ioManager,
                            0,
                            GenericRow.of(1, 10, 777L, 50L),
                            GenericRow.of(2, 30, 555L, 50L));
            long updateSnapshot = write(table, ioManager, 1, GenericRow.of(1, 20, 888L, 100L));

            // Removing a preceding column changes the preserved field's position in new rows.
            table.schemaManager().commitChanges(SchemaChange.dropColumn("extra"));
            table = table.copyWithLatestSchema();

            assertThat(table.rowType().getFieldNames()).containsExactly("id", "data", "event_ts");
            assertHistoricalData(table, insertSnapshot, "1, 10, 50", "2, 30, 50");
            assertChangelog(table, insertSnapshot, "+I 1, 10, 50", "+I 2, 30, 50");
            assertChangelog(table, updateSnapshot, "-U 1, 10, 100", "+U 1, 20, 100");
            assertData(table, "1, 20, 100", "2, 30, 50");

            long changedSnapshot =
                    write(
                            table,
                            ioManager,
                            2,
                            GenericRow.of(1, 40, 150L),
                            GenericRow.ofKind(RowKind.DELETE, 2, null, 175L));
            assertChangelog(
                    table, changedSnapshot, "-U 1, 20, 150", "+U 1, 40, 150", "-D 2, 30, 175");
            assertData(table, "1, 40, 150");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "avro"})
    void testChangePreservedFieldType(String format) throws Exception {
        FileStoreTable table = createTable(format);
        try (IOManager ioManager = IOManager.create(tempDir.resolve("io").toString())) {
            long insertSnapshot =
                    write(
                            table,
                            ioManager,
                            0,
                            GenericRow.of(1, 10, 777L, 50L),
                            GenericRow.of(2, 30, 555L, 50L));
            long updateSnapshot = write(table, ioManager, 1, GenericRow.of(1, 20, 888L, 100L));

            table.schemaManager()
                    .commitChanges(
                            SchemaChange.updateColumnType("event_ts", DataTypes.DECIMAL(20, 0)));
            table = table.copyWithLatestSchema();

            assertThat(table.rowType().getTypeAt(3)).isEqualTo(DataTypes.DECIMAL(20, 0));
            assertHistoricalData(table, insertSnapshot, "1, 10, 777, 50", "2, 30, 555, 50");
            assertChangelog(table, insertSnapshot, "+I 1, 10, 777, 50", "+I 2, 30, 555, 50");
            assertChangelog(table, updateSnapshot, "-U 1, 10, 777, 100", "+U 1, 20, 888, 100");
            assertData(table, "1, 20, 888, 100", "2, 30, 555, 50");

            long changedSnapshot =
                    write(
                            table,
                            ioManager,
                            2,
                            GenericRow.of(1, 40, 999L, decimal(150)),
                            GenericRow.ofKind(RowKind.DELETE, 2, null, null, decimal(175)));
            assertChangelog(
                    table,
                    changedSnapshot,
                    "-U 1, 20, 888, 150",
                    "+U 1, 40, 999, 150",
                    "-D 2, 30, 555, 175");
            assertData(table, "1, 40, 999, 150");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "avro"})
    void testPreservedComplexFieldsAndNestedProjections(String format) throws Exception {
        FileStoreTable table =
                createTable(
                        format,
                        "metadata,items,attributes",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("data", DataTypes.INT())
                                .column("event_ts", DataTypes.BIGINT())
                                .column(
                                        "metadata",
                                        DataTypes.ROW(
                                                DataTypes.FIELD(0, "a", DataTypes.INT()),
                                                DataTypes.FIELD(1, "b", DataTypes.INT())))
                                .column("items", DataTypes.ARRAY(DataTypes.INT()))
                                .column(
                                        "attributes",
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        Path tablePath = table.location();
        long insertSnapshot;
        long updateSnapshot;
        try (IOManager ioManager = IOManager.create(tempDir.resolve("io").toString())) {
            insertSnapshot =
                    write(
                            table,
                            ioManager,
                            0,
                            rowWithComplexFields(RowKind.INSERT, 1, 10, 50, 11, 22),
                            rowWithComplexFields(RowKind.INSERT, 2, 20, 50, 13, 24),
                            rowWithComplexFields(RowKind.INSERT, 3, 30, 50, 15, 26),
                            GenericRow.of(4, 40, 50L, null, null, null));

            // Reopening ensures lookup obtains before-images from persisted files.
            table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
            updateSnapshot =
                    write(
                            table,
                            ioManager,
                            1,
                            rowWithComplexFields(RowKind.INSERT, 1, 11, 100, 33, 44),
                            rowWithComplexFields(RowKind.DELETE, 2, null, 150, 55, 66),
                            GenericRow.of(3, 31, 200L, null, null, null),
                            rowWithComplexFields(RowKind.INSERT, 4, 41, 250, 77, 88));
        }
        table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
        List<DataSplit> changelog = splits(table, updateSnapshot, ScanMode.CHANGELOG);

        // Only complex fields are preserved: data and event_ts in retractions remain before-images.
        assertThat(read(table, changelog, table.rowType(), false, true))
                .containsExactlyInAnyOrder(
                        "-U 1, 10, 50, (33, 44), [33, null, 44], {v=44}",
                        "+U 1, 11, 100, (33, 44), [33, null, 44], {v=44}",
                        "-D 2, 20, 50, (55, 66), [55, null, 66], {v=66}",
                        "-U 3, 30, 50, NULL, NULL, NULL",
                        "+U 3, 31, 200, NULL, NULL, NULL",
                        "-U 4, 40, 50, (77, 88), [77, null, 88], {v=88}",
                        "+U 4, 41, 250, (77, 88), [77, null, 88], {v=88}");

        List<DataSplit> currentData = splits(table, updateSnapshot, ScanMode.ALL);
        assertThat(read(table, currentData, table.rowType(), false, false))
                .containsExactlyInAnyOrder(
                        "1, 11, 100, (33, 44), [33, null, 44], {v=44}",
                        "3, 31, 200, NULL, NULL, NULL",
                        "4, 41, 250, (77, 88), [77, null, 88], {v=88}");

        RowType secondChild =
                table.rowType().projectByPaths(Arrays.asList("metadata.b", "id", "data"));
        assertThat(read(table, changelog, secondChild, false, true))
                .containsExactlyInAnyOrder(
                        "-U (44), 1, 10",
                        "+U (44), 1, 11",
                        "-D (66), 2, 20",
                        "-U NULL, 3, 30",
                        "+U NULL, 3, 31",
                        "-U (88), 4, 40",
                        "+U (88), 4, 41");
        assertThat(read(table, currentData, secondChild, false, false))
                .containsExactlyInAnyOrder("(44), 1, 11", "NULL, 3, 31", "(88), 4, 41");

        RowType reorderedChildren =
                table.rowType()
                        .projectByPaths(Arrays.asList("metadata.b", "metadata.a", "id", "data"));
        assertThat(read(table, changelog, reorderedChildren, false, true))
                .containsExactlyInAnyOrder(
                        "-U (44, 33), 1, 10",
                        "+U (44, 33), 1, 11",
                        "-D (66, 55), 2, 20",
                        "-U NULL, 3, 30",
                        "+U NULL, 3, 31",
                        "-U (88, 77), 4, 40",
                        "+U (88, 77), 4, 41");

        List<DataSplit> initialData = splits(table, insertSnapshot, ScanMode.ALL);
        assertThat(initialData)
                .allSatisfy(
                        split -> {
                            assertThat(split.isStreaming()).isFalse();
                            assertThat(split.rawConvertible()).isTrue();
                            assertThat(split.dataFiles())
                                    .allSatisfy(
                                            file -> assertThat(file.deleteRowCount()).contains(0L));
                        });
        // The insert snapshot can use the raw reader; forceKeepDelete selects the merged reader.
        for (boolean forceMerge : new boolean[] {false, true}) {
            assertThat(read(table, initialData, secondChild, forceMerge, false))
                    .containsExactlyInAnyOrder(
                            "(22), 1, 10", "(24), 2, 20", "(26), 3, 30", "NULL, 4, 40");
            assertThat(read(table, initialData, reorderedChildren, forceMerge, false))
                    .containsExactlyInAnyOrder(
                            "(22, 11), 1, 10", "(24, 13), 2, 20", "(26, 15), 3, 30", "NULL, 4, 40");
        }
    }

    private FileStoreTable createTable(String format) throws Exception {
        return createTable(
                format,
                "event_ts",
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("data", DataTypes.INT())
                        .column("extra", DataTypes.BIGINT())
                        .column("event_ts", DataTypes.BIGINT()));
    }

    private FileStoreTable createTable(String format, String preserveFields, Schema.Builder schema)
            throws Exception {
        Path tablePath = new Path(tempDir.resolve("table").toUri());
        new FileSystemSchemaManager(LocalFileIO.create(), tablePath)
                .createTable(
                        schema.primaryKey("id")
                                .option(CoreOptions.BUCKET.key(), "1")
                                .option(CoreOptions.FILE_FORMAT.key(), format)
                                .option(CoreOptions.MERGE_ENGINE.key(), "deduplicate")
                                .option(CoreOptions.CHANGELOG_PRODUCER.key(), "lookup")
                                .option(CoreOptions.SEQUENCE_FIELD.key(), "event_ts")
                                .option(
                                        CoreOptions.CHANGELOG_PRODUCER_PRESERVE_FIELD_ON_RETRACT
                                                .key(),
                                        preserveFields)
                                .build());
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
    }

    private GenericRow rowWithComplexFields(
            RowKind kind, int id, Integer data, long eventTs, int a, int b) {
        return GenericRow.ofKind(
                kind,
                id,
                data,
                eventTs,
                GenericRow.of(a, b),
                new GenericArray(new Integer[] {a, null, b}),
                new GenericMap(Collections.singletonMap(BinaryString.fromString("v"), b)));
    }

    private long write(
            FileStoreTable table, IOManager ioManager, long checkpoint, InternalRow... rows)
            throws Exception {
        try (StreamTableWrite write = table.newWrite(commitUser).withIOManager(ioManager);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            commit.commit(checkpoint, write.prepareCommit(true, checkpoint));
        }
        return table.snapshotManager().latestSnapshotId();
    }

    private void assertHistoricalData(FileStoreTable table, long snapshot, String... expected)
            throws Exception {
        List<DataSplit> splits =
                table.newSnapshotReader().withSnapshot(snapshot).read().dataSplits();
        assertThat(splits)
                .isNotEmpty()
                .allSatisfy(
                        split -> {
                            assertThat(split.isStreaming()).isFalse();
                            assertThat(split.rawConvertible()).isTrue();
                            assertThat(split.dataFiles())
                                    .isNotEmpty()
                                    .allSatisfy(
                                            file -> {
                                                assertThat(file.schemaId())
                                                        .isLessThan(table.schema().id());
                                                assertThat(file.deleteRowCount()).contains(0L);
                                            });
                        });
        assertThat(read(table, splits, table.rowType(), false, false))
                .containsExactlyInAnyOrder(expected);
        // Keeping deletes bypasses the raw provider, exercising the key-value merge reader too.
        // This snapshot contains only inserts, so both readers must return the same values.
        assertThat(read(table, splits, table.rowType(), true, false))
                .containsExactlyInAnyOrder(expected);
    }

    private void assertData(FileStoreTable table, String... expected) throws Exception {
        assertThat(
                        read(
                                table,
                                table.newSnapshotReader().read().dataSplits(),
                                table.rowType(),
                                false,
                                false))
                .containsExactlyInAnyOrder(expected);
    }

    private void assertChangelog(FileStoreTable table, long snapshot, String... expected)
            throws Exception {
        List<DataSplit> splits = splits(table, snapshot, ScanMode.CHANGELOG);
        assertThat(read(table, splits, table.rowType(), false, true))
                .containsExactlyInAnyOrder(expected);
    }

    private List<DataSplit> splits(FileStoreTable table, long snapshot, ScanMode mode) {
        List<DataSplit> splits =
                table.newSnapshotReader().withSnapshot(snapshot).withMode(mode).read().dataSplits();
        String format = table.options().get(CoreOptions.FILE_FORMAT.key());
        assertThat(splits)
                .isNotEmpty()
                .allSatisfy(
                        split ->
                                assertThat(split.dataFiles())
                                        .isNotEmpty()
                                        .allSatisfy(
                                                file ->
                                                        assertThat(file.fileName())
                                                                .endsWith("." + format)));
        return splits;
    }

    private List<String> read(
            FileStoreTable table,
            List<DataSplit> splits,
            RowType readType,
            boolean forceMerge,
            boolean includeRowKind)
            throws Exception {
        InnerTableRead read = table.newRead().withReadType(readType);
        if (forceMerge) {
            read.forceKeepDelete();
        }
        List<String> rows = new ArrayList<>();
        for (DataSplit split : splits) {
            try (RecordReader<InternalRow> reader = read.createReader(split)) {
                reader.forEachRemaining(
                        row ->
                                rows.add(
                                        (includeRowKind ? row.getRowKind().shortString() + " " : "")
                                                + DataFormatTestUtil.toStringWithRowKind(
                                                        row, readType)));
            }
        }
        return rows;
    }

    private Decimal decimal(long value) {
        return Decimal.fromBigDecimal(BigDecimal.valueOf(value), 20, 0);
    }
}
