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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.shredding.MapSharedShreddingDefine;
import org.apache.paimon.data.shredding.MapSharedShreddingFieldMeta;
import org.apache.paimon.data.shredding.MapSharedShreddingUtils;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.SupportsFieldMetadata;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Table-level tests for MAP shared-shredding. */
public class MapSharedShreddingTableTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testAppendOnlyTableReadWrite(String format) throws Exception {
        Table table = createTable(format, "metrics");

        write(
                table,
                GenericRow.of(1, mapOf("a", 11L, "b", 12L, "c", 13L)),
                GenericRow.of(2, mapOf()),
                GenericRow.of(3, null),
                GenericRow.of(4, mapOf("a", null, "b", 42L, "c", null)));

        Map<Integer, Map<String, Long>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), row.isNullAt(1) ? null : toJavaMap(row.getMap(1)));
        }

        assertThat(actual)
                .containsEntry(1, javaMapOf("a", 11L, "b", 12L, "c", 13L))
                .containsEntry(2, javaMapOf())
                .containsEntry(3, null)
                .containsEntry(4, javaMapOf("a", null, "b", 42L, "c", null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testAppendOnlyTableReadWriteWithTwoMapFields(String format) throws Exception {
        Table table = createTable(format, "metrics", "labels");

        write(
                table,
                GenericRow.of(
                        1,
                        mapOf("a", 11L, "b", 12L, "c", 13L),
                        mapOf("x", 21L, "y", 22L, "z", 23L)),
                GenericRow.of(2, mapOf("a", 31L), mapOf()),
                GenericRow.of(3, null, mapOf("x", 41L)));

        Map<Integer, List<Map<String, Long>>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(
                    row.getInt(0),
                    Arrays.asList(
                            row.isNullAt(1) ? null : toJavaMap(row.getMap(1)),
                            row.isNullAt(2) ? null : toJavaMap(row.getMap(2))));
        }

        assertThat(actual)
                .containsEntry(
                        1,
                        Arrays.asList(
                                javaMapOf("a", 11L, "b", 12L, "c", 13L),
                                javaMapOf("x", 21L, "y", 22L, "z", 23L)))
                .containsEntry(2, Arrays.asList(javaMapOf("a", 31L), javaMapOf()))
                .containsEntry(3, Arrays.asList(null, javaMapOf("x", 41L)));

        Map<Integer, Map<String, Long>> projected = new LinkedHashMap<>();
        for (InternalRow row : read(table, new int[] {0, 2})) {
            projected.put(row.getInt(0), row.isNullAt(1) ? null : toJavaMap(row.getMap(1)));
        }

        assertThat(projected)
                .containsEntry(1, javaMapOf("x", 21L, "y", 22L, "z", 23L))
                .containsEntry(2, javaMapOf())
                .containsEntry(3, javaMapOf("x", 41L));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testRestoreAdaptiveColumnCountFromFileMetadata(String format) throws Exception {
        Table table = createTableWithBucket(format, 8, "1", "metrics");

        write(table, GenericRow.of(1, mapOf("a", 11L)));
        write(table, GenericRow.of(2, mapOf("b", 22L)));

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<DataFileWithSplit> files = currentDataFiles(fileStoreTable);
        files.sort(Comparator.comparingLong(file -> file.dataFile.minSequenceNumber()));
        assertThat(files).hasSize(2);

        MapSharedShreddingFieldMeta firstFileMeta =
                readSharedShreddingFieldMeta(fileStoreTable, files.get(0), "metrics");
        assertThat(firstFileMeta.numColumns()).isEqualTo(8);
        assertThat(firstFileMeta.maxRowWidth()).isEqualTo(1);

        MapSharedShreddingFieldMeta secondFileMeta =
                readSharedShreddingFieldMeta(fileStoreTable, files.get(1), "metrics");
        assertThat(secondFileMeta.numColumns()).isEqualTo(1);
        assertThat(secondFileMeta.maxRowWidth()).isEqualTo(1);

        Map<Integer, Map<String, Long>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), row.isNullAt(1) ? null : toJavaMap(row.getMap(1)));
        }

        assertThat(actual)
                .containsEntry(1, javaMapOf("a", 11L))
                .containsEntry(2, javaMapOf("b", 22L));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testSwitchMapLayoutAndUseMaxColumnsWithoutMetadata(String format) throws Exception {
        Table table =
                createTableWithBucket(
                        format,
                        4,
                        "1",
                        Arrays.asList("metrics", "labels"),
                        Arrays.asList("labels"));

        write(table, GenericRow.of(1, mapOf("a", 11L, "b", 12L), mapOf("x", 21L)));

        catalog.alterTable(
                identifier(format),
                Arrays.asList(
                        SchemaChange.setOption(
                                "fields.metrics.map.storage-layout", "shared-shredding"),
                        SchemaChange.setOption(
                                "fields.metrics.map.shared-shredding.max-columns", "3"),
                        SchemaChange.setOption("fields.labels.map.storage-layout", "default")),
                false);
        table = catalog.getTable(identifier(format));

        write(table, GenericRow.of(2, mapOf("c", 31L), mapOf("y", 41L, "z", 42L)));

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        List<DataFileWithSplit> files = currentDataFiles(fileStoreTable);
        files.sort(Comparator.comparingLong(file -> file.dataFile.minSequenceNumber()));
        assertThat(files).hasSize(2);

        MapSharedShreddingFieldMeta metricsMeta =
                readSharedShreddingFieldMeta(fileStoreTable, files.get(1), "metrics");
        assertThat(metricsMeta.numColumns()).isEqualTo(3);
        assertThat(metricsMeta.maxRowWidth()).isEqualTo(1);

        Map<Integer, List<Map<String, Long>>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(
                    row.getInt(0),
                    Arrays.asList(
                            row.isNullAt(1) ? null : toJavaMap(row.getMap(1)),
                            row.isNullAt(2) ? null : toJavaMap(row.getMap(2))));
        }

        assertThat(actual)
                .containsEntry(1, Arrays.asList(javaMapOf("a", 11L, "b", 12L), javaMapOf("x", 21L)))
                .containsEntry(
                        2, Arrays.asList(javaMapOf("c", 31L), javaMapOf("y", 41L, "z", 42L)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testReadSharedShreddingMapAfterRenameColumn(String format) throws Exception {
        Table table = createTable(format, "metrics");

        write(
                table,
                GenericRow.of(1, mapOf("a", 11L, "b", 12L)),
                GenericRow.of(2, mapOf("c", 21L)));

        catalog.alterTable(
                identifier(format),
                Arrays.asList(
                        SchemaChange.renameColumn("metrics", "renamed_metrics"),
                        SchemaChange.removeOption("fields.metrics.map.storage-layout"),
                        SchemaChange.removeOption(
                                "fields.metrics.map.shared-shredding.max-columns"),
                        SchemaChange.setOption(
                                "fields.renamed_metrics.map.storage-layout", "shared-shredding"),
                        SchemaChange.setOption(
                                "fields.renamed_metrics.map.shared-shredding.max-columns", "2")),
                false);
        table = catalog.getTable(identifier(format));

        assertThat(table.rowType().getFieldNames()).containsExactly("id", "renamed_metrics");

        Map<Integer, Map<String, Long>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), row.isNullAt(1) ? null : toJavaMap(row.getMap(1)));
        }

        assertThat(actual)
                .containsEntry(1, javaMapOf("a", 11L, "b", 12L))
                .containsEntry(2, javaMapOf("c", 21L));
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testDataEvolutionMergeWithOverwrittenSharedShreddingMaps(String format)
            throws Exception {
        Table table = createDataEvolutionTable(format, "metrics", "labels");
        RowType rowType = table.rowType();

        writeWithWriteType(
                table,
                rowType.project(Arrays.asList("id", "metrics")),
                GenericRow.of(1, mapOf("old-a", 11L)),
                GenericRow.of(2, mapOf("old-b", 21L)));
        writeWithWriteType(
                table,
                rowType.project(Arrays.asList("id", "labels")),
                0L,
                GenericRow.of(101, mapOf("label-a", 101L)),
                GenericRow.of(102, mapOf("label-b", 102L)));
        writeWithWriteType(
                table,
                rowType.project(Collections.singletonList("metrics")),
                0L,
                GenericRow.of(mapOf("new-a", 111L)),
                GenericRow.of(mapOf("new-b", 222L)));

        Map<Integer, List<Map<String, Long>>> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(
                    row.getInt(0),
                    Arrays.asList(toJavaMap(row.getMap(1)), toJavaMap(row.getMap(2))));
        }

        assertThat(actual)
                .containsEntry(
                        101, Arrays.asList(javaMapOf("new-a", 111L), javaMapOf("label-a", 101L)))
                .containsEntry(
                        102, Arrays.asList(javaMapOf("new-b", 222L), javaMapOf("label-b", 102L)));

        Map<Integer, List<Map<String, Long>>> partialActual = new LinkedHashMap<>();
        ReadBuilder readBuilder =
                table.newReadBuilder().withRowRanges(Collections.singletonList(new Range(1L, 1L)));
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(
                        row ->
                                partialActual.put(
                                        row.getInt(0),
                                        Arrays.asList(
                                                toJavaMap(row.getMap(1)),
                                                toJavaMap(row.getMap(2)))));

        assertThat(partialActual)
                .containsOnlyKeys(102)
                .containsEntry(
                        102, Arrays.asList(javaMapOf("new-b", 222L), javaMapOf("label-b", 102L)));
    }

    private Table createTable(String format, String... sharedShreddingFields) throws Exception {
        return createTable(format, 2, sharedShreddingFields);
    }

    private Table createTable(String format, int maxColumns, String... sharedShreddingFields)
            throws Exception {
        return createTableWithBucket(format, maxColumns, "-1", sharedShreddingFields);
    }

    private Table createTableWithBucket(
            String format, int maxColumns, String bucket, String... sharedShreddingFields)
            throws Exception {
        return createTableWithBucket(
                format,
                maxColumns,
                bucket,
                Arrays.asList(sharedShreddingFields),
                Arrays.asList(sharedShreddingFields));
    }

    private Table createTableWithBucket(
            String format,
            int maxColumns,
            String bucket,
            List<String> mapFields,
            List<String> sharedShreddingFields)
            throws Exception {
        catalog.createTable(
                identifier(format),
                schemaWithBucket(format, maxColumns, bucket, mapFields, sharedShreddingFields),
                true);
        return catalog.getTable(identifier(format));
    }

    private Table createDataEvolutionTable(String format, String... sharedShreddingFields)
            throws Exception {
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT());
        for (String field : sharedShreddingFields) {
            builder.column(field, DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()));
        }
        builder.option("bucket", "-1")
                .option("file.format", format)
                .option(CoreOptions.WRITE_ONLY.key(), "true")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        for (String field : sharedShreddingFields) {
            builder.option("fields." + field + ".map.storage-layout", "shared-shredding")
                    .option("fields." + field + ".map.shared-shredding.max-columns", "2");
        }
        catalog.createTable(identifier(format), builder.build(), true);
        return catalog.getTable(identifier(format));
    }

    private void writeWithWriteType(Table table, RowType writeType, InternalRow... rows)
            throws Exception {
        writeWithWriteType(table, writeType, null, rows);
    }

    private void writeWithWriteType(
            Table table, RowType writeType, Long firstRowId, InternalRow... rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(writeType);
                BatchTableCommit commit = builder.newCommit()) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            List<CommitMessage> commitMessages = write.prepareCommit();
            if (firstRowId != null) {
                setFirstRowId(commitMessages, firstRowId);
            }
            commit.commit(commitMessages);
        }
    }

    private void setFirstRowId(List<CommitMessage> commitMessages, long firstRowId) {
        for (CommitMessage message : commitMessages) {
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            List<DataFileMeta> newFiles =
                    new ArrayList<>(commitMessage.newFilesIncrement().newFiles());
            commitMessage.newFilesIncrement().newFiles().clear();
            for (DataFileMeta newFile : newFiles) {
                commitMessage
                        .newFilesIncrement()
                        .newFiles()
                        .add(newFile.assignFirstRowId(firstRowId));
            }
        }
    }

    private Schema schema(String format, String... sharedShreddingFields) {
        return schema(format, 2, sharedShreddingFields);
    }

    private Schema schema(String format, int maxColumns, String... sharedShreddingFields) {
        return schemaWithBucket(format, maxColumns, "-1", sharedShreddingFields);
    }

    private Schema schemaWithBucket(
            String format, int maxColumns, String bucket, String... sharedShreddingFields) {
        return schemaWithBucket(
                format,
                maxColumns,
                bucket,
                Arrays.asList(sharedShreddingFields),
                Arrays.asList(sharedShreddingFields));
    }

    private Schema schemaWithBucket(
            String format,
            int maxColumns,
            String bucket,
            List<String> mapFields,
            List<String> sharedShreddingFields) {
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT());
        for (String field : mapFields) {
            builder.column(field, DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()));
        }
        builder.option("bucket", bucket)
                .option("file.format", format)
                .option(CoreOptions.WRITE_ONLY.key(), "true");
        if (!"-1".equals(bucket)) {
            builder.option("bucket-key", "id");
        }
        for (String field : sharedShreddingFields) {
            builder.option("fields." + field + ".map.storage-layout", "shared-shredding")
                    .option(
                            "fields." + field + ".map.shared-shredding.max-columns",
                            String.valueOf(maxColumns));
        }
        return builder.build();
    }

    private List<DataFileWithSplit> currentDataFiles(FileStoreTable table) throws Exception {
        List<DataFileWithSplit> files = new ArrayList<>();
        for (DataSplit split : table.newSnapshotReader().read().dataSplits()) {
            for (DataFileMeta dataFile : split.dataFiles()) {
                files.add(new DataFileWithSplit(split.partition(), split.bucket(), dataFile));
            }
        }
        return files;
    }

    private MapSharedShreddingFieldMeta readSharedShreddingFieldMeta(
            FileStoreTable table, DataFileWithSplit file, String fieldName) throws Exception {
        DataFilePathFactory pathFactory =
                table.store().pathFactory().createDataFilePathFactory(file.partition, file.bucket);
        FileFormat fileFormat =
                FileFormatDiscover.of(new CoreOptions(table.options()))
                        .discover(file.dataFile.fileFormat());
        Map<String, Map<String, String>> fieldMetadata =
                ((SupportsFieldMetadata) fileFormat)
                        .readFieldMetadata(
                                new FormatReaderContext(
                                        table.fileIO(),
                                        pathFactory.toPath(file.dataFile),
                                        file.dataFile.fileSize()));
        return MapSharedShreddingUtils.deserializeMetadata(
                fieldMetadata.get(fieldName), MapSharedShreddingDefine.DEFAULT_DICT_COMPRESSION);
    }

    private GenericMap mapOf(Object... entries) {
        Map<BinaryString, Long> map = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put(BinaryString.fromString((String) entries[i]), (Long) entries[i + 1]);
        }
        return new GenericMap(map);
    }

    private Map<String, Long> toJavaMap(InternalMap map) {
        Map<String, Long> result = new LinkedHashMap<>();
        for (int i = 0; i < map.size(); i++) {
            result.put(
                    map.keyArray().getString(i).toString(),
                    map.valueArray().isNullAt(i) ? null : map.valueArray().getLong(i));
        }
        return result;
    }

    private Map<String, Long> javaMapOf(Object... entries) {
        Map<String, Long> map = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put((String) entries[i], (Long) entries[i + 1]);
        }
        return map;
    }

    @Override
    protected Schema schemaDefault() {
        return schema("parquet", "metrics");
    }

    private static class DataFileWithSplit {

        private final BinaryRow partition;
        private final int bucket;
        private final DataFileMeta dataFile;

        private DataFileWithSplit(BinaryRow partition, int bucket, DataFileMeta dataFile) {
            this.partition = partition;
            this.bucket = bucket;
            this.dataFile = dataFile;
        }
    }
}
