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

package org.apache.paimon.migrate;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** Tests external file registration with extracted counts and unchanged source files. */
class ExternalFileImporterTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path source;

    @BeforeEach
    void before() throws IOException {
        fileIO = spy(LocalFileIO.create());
        source = new Path(tempDir.resolve("external").toUri());
        fileIO.mkdirs(source);
    }

    @ParameterizedTest
    @ValueSource(strings = {"parquet", "orc", "avro"})
    void testImportExtractsCountsAndPreservesSources(String format) throws Exception {
        FileStoreTable table = createTable(format, Collections.emptyMap());
        Path first = externalFile(format, "first." + format, 1, 2);
        Path second = externalFile(format, "second." + format, 3);
        invalidFile("_hidden." + format);
        invalidFile(".hidden." + format);
        invalidFile("unrelated.txt");
        invalidFile("nested/ignored." + format);

        assertThat(
                        ExternalFileImporter.importFiles(
                                table, source.toString(), "hour=12,dt=2026-10-08"))
                .isEqualTo(2);

        DataSplit split = splits(table).get(0);
        assertThat(split.partition().getString(0)).isEqualTo(fromString("2026-10-08"));
        assertThat(split.partition().getInt(1)).isEqualTo(12);
        assertThat(split.bucket()).isZero();
        assertThat(split.rowCount()).isEqualTo(3);
        assertThat(split.mergedRowCount()).hasValue(3);
        List<DataFileMeta> metas = split.dataFiles();
        assertThat(metas).hasSize(2);
        assertThat(metas.stream().map(f -> f.externalPath().get()))
                .containsExactlyInAnyOrder(first.toString(), second.toString());
        for (DataFileMeta meta : metas) {
            Path external = new Path(meta.externalPath().get());
            assertThat(meta.fileSize()).isEqualTo(fileIO.getFileStatus(external).getLen());
            assertThat(meta.schemaId()).isEqualTo(table.schema().id());
            assertThat(meta.fileFormat()).isEqualTo(format);
            assertThat(fileIO.exists(external)).isTrue();
            boolean isFirst = external.equals(first);
            assertThat(meta.rowCount()).isEqualTo(isFirst ? 2 : 1);
            if (!format.equals("avro")) {
                assertThat(meta.valueStats().minValues().getInt(0)).isEqualTo(isFirst ? 1 : 3);
                assertThat(meta.valueStats().maxValues().getInt(0)).isEqualTo(isFirst ? 2 : 3);
            }
        }
        assertThat(table.snapshotManager().latestSnapshot().totalRecordCount()).isEqualTo(3);
        assertThat(table.snapshotManager().latestSnapshot().deltaRecordCount()).isEqualTo(3);
        assertThat(table.newScan().listPartitionEntries().get(0).recordCount()).isEqualTo(3);
        verify(fileIO, atLeastOnce())
                .newInputStream(argThat(path -> path.toString().startsWith(source.toString())));
        verify(fileIO, never())
                .rename(argThat(path -> path.toString().startsWith(source.toString())), any());
        verify(fileIO, never())
                .copyFile(
                        argThat(path -> path.toString().startsWith(source.toString())),
                        any(),
                        anyBoolean());
    }

    @Test
    void testStatsModeNoneStillExtractsRowCount() throws Exception {
        FileStoreTable table =
                createTable("parquet", Collections.singletonMap("metadata.stats-mode", "none"));
        externalFile("parquet", "data.parquet", 1, 2);
        ExternalFileImporter.importFiles(table, source.toString(), "dt=p1,hour=1");
        DataFileMeta meta = splits(table).get(0).dataFiles().get(0);
        assertThat(meta.rowCount()).isEqualTo(2);
        assertThat(meta.valueStats().minValues().getFieldCount()).isZero();
        assertThat(meta.valueStats().nullCounts().size()).isZero();
    }

    @Test
    void testUnreadableFileDoesNotCommit() throws Exception {
        FileStoreTable table = createTable("parquet", Collections.emptyMap());
        write(table, GenericRow.of(9, fromString("p1"), 1));
        long snapshotId = table.snapshotManager().latestSnapshotId();
        Path first = externalFile("parquet", "first.parquet", 1);
        Path unreadable = externalFile("parquet", "unreadable.parquet", 2);
        doThrow(new IOException("Source file is unavailable"))
                .when(fileIO)
                .newInputStream(unreadable);

        assertThatThrownBy(
                        () ->
                                ExternalFileImporter.importFiles(
                                        table, source.toString(), "dt=p1,hour=1"))
                .hasStackTraceContaining("Source file is unavailable");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshotId);
        assertThat(splits(table).get(0).dataFiles()).hasSize(1);
        assertThat(table.snapshotManager().latestSnapshot().totalRecordCount()).isEqualTo(1);
        assertThat(fileIO.exists(first)).isTrue();
        assertThat(fileIO.exists(unreadable)).isTrue();
    }

    @Test
    void testCorruptFileDoesNotCommit() throws Exception {
        FileStoreTable table = createTable("parquet", Collections.emptyMap());
        externalFile("parquet", "first.parquet", 1);
        Path corrupt = invalidFile("corrupt.parquet");
        assertThatThrownBy(
                        () ->
                                ExternalFileImporter.importFiles(
                                        table, source.toString(), "dt=p1,hour=1"))
                .hasStackTraceContaining("corrupt.parquet");
        assertThat(table.snapshotManager().latestSnapshot()).isNull();
        assertThat(fileIO.exists(corrupt)).isTrue();
    }

    @Test
    void testDuplicateImportDoesNotCommit() throws Exception {
        FileStoreTable table = createTable("parquet", Collections.emptyMap());
        externalFile("parquet", "first.parquet", 1);
        ExternalFileImporter.importFiles(table, source.toString(), "dt=p1,hour=1");
        long snapshotId = table.snapshotManager().latestSnapshotId();
        externalFile("parquet", "second.parquet", 2);

        assertThatThrownBy(
                        () ->
                                ExternalFileImporter.importFiles(
                                        table, source.toString(), "dt=p1,hour=1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already been imported");
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshotId);
        assertThat(splits(table).get(0).dataFiles()).hasSize(1);
    }

    @Test
    void testSameSourceNameInDifferentDirectories() throws Exception {
        FileStoreTable table = createTable("parquet", Collections.emptyMap());
        externalFile("parquet", "data.parquet", 1);
        externalFile("parquet", "nested/data.parquet", 2);
        ExternalFileImporter.importFiles(table, source.toString(), "dt=p1,hour=1");
        ExternalFileImporter.importFiles(
                table, new Path(source, "nested").toString(), "dt=p1,hour=1");

        List<DataFileMeta> metas = splits(table).get(0).dataFiles();
        assertThat(metas).hasSize(2);
        assertThat(metas.stream().map(DataFileMeta::fileName).distinct()).hasSize(2);
        assertThat(metas.stream().map(f -> f.externalPath().get()).distinct()).hasSize(2);
        assertThat(table.snapshotManager().latestSnapshot().totalRecordCount()).isEqualTo(2);
    }

    @Test
    void testCountsWithExistingData() throws Exception {
        FileStoreTable table = createTable("parquet", Collections.emptyMap());
        write(table, GenericRow.of(1, fromString("p1"), 1));
        externalFile("parquet", "imported.parquet", 3, 4);
        ExternalFileImporter.importFiles(table, source.toString(), "dt=p1,hour=1");
        write(table, GenericRow.of(2, fromString("p1"), 1));

        DataSplit split = splits(table).get(0);
        assertThat(split.dataFiles()).hasSize(3);
        assertThat(split.rowCount()).isEqualTo(4);
        assertThat(split.mergedRowCount()).hasValue(4);
        assertThat(table.snapshotManager().latestSnapshot().totalRecordCount()).isEqualTo(4);
        assertThat(table.newScan().listPartitionEntries().get(0).recordCount()).isEqualTo(4);
    }

    @Test
    void testEmptyDirectoryDoesNotCommit() throws Exception {
        FileStoreTable table = createTable("parquet", Collections.emptyMap());
        assertThat(ExternalFileImporter.importFiles(table, source.toString(), "dt=p1,hour=1"))
                .isZero();
        assertThat(table.snapshotManager().latestSnapshot()).isNull();
    }

    @Test
    void testCompletePartitionIsRequired() throws Exception {
        FileStoreTable table = createTable("parquet", Collections.emptyMap());
        for (String partition : Arrays.asList(null, "", "dt=p1", "dt=p1,hour=1,extra=x")) {
            assertThatThrownBy(
                            () ->
                                    ExternalFileImporter.importFiles(
                                            table, source.toString(), partition))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("exactly the table partition keys");
        }
        assertThatThrownBy(
                        () ->
                                ExternalFileImporter.importFiles(
                                        table, source.toString(), "dt=p1,hour=invalid"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(table.snapshotManager().latestSnapshot()).isNull();
    }

    @Test
    void testUnsupportedTables() throws Exception {
        Map<String, String> bucketOptions = new HashMap<>();
        bucketOptions.put("bucket", "1");
        bucketOptions.put("bucket-key", "id");
        FileStoreTable bucketed = createTable("parquet", bucketOptions);
        assertThatThrownBy(
                        () ->
                                ExternalFileImporter.importFiles(
                                        bucketed, source.toString(), "dt=p1,hour=1"))
                .hasMessageContaining("bucket = -1");
        FileStoreTable tracked =
                createTable("parquet", Collections.singletonMap("row-tracking.enabled", "true"));
        assertThatThrownBy(
                        () ->
                                ExternalFileImporter.importFiles(
                                        tracked, source.toString(), "dt=p1,hour=1"))
                .hasMessageContaining("row tracking");
    }

    private Path externalFile(String format, String name, int... ids) throws Exception {
        FileStoreTable producer = createTable(format, Collections.emptyMap());
        GenericRow[] rows =
                Arrays.stream(ids)
                        .mapToObj(id -> GenericRow.of(id, fromString("source"), 0))
                        .toArray(GenericRow[]::new);
        write(producer, rows);
        DataSplit split = splits(producer).get(0);
        DataFileMeta meta = split.dataFiles().get(0);
        Path dataFile =
                producer.store()
                        .pathFactory()
                        .createDataFilePathFactory(split.partition(), split.bucket())
                        .toPath(meta);
        Path external = new Path(source, name);
        fileIO.mkdirs(external.getParent());
        fileIO.copyFile(dataFile, external, true);
        return external;
    }

    private Path invalidFile(String name) throws IOException {
        java.nio.file.Path path = tempDir.resolve("external").resolve(name);
        Files.createDirectories(path.getParent());
        Files.write(path, new byte[] {1, 2, 3});
        return new Path(path.toUri());
    }

    private FileStoreTable createTable(String format, Map<String, String> overrides)
            throws Exception {
        Path tablePath = new Path(tempDir.resolve("table-" + System.nanoTime()).toUri());
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.FILE_FORMAT.key(), format);
        options.putAll(overrides);
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("dt", DataTypes.STRING())
                        .field("hour", DataTypes.INT())
                        .build();
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(fileIO, tablePath),
                        new Schema(
                                rowType.getFields(),
                                Arrays.asList("dt", "hour"),
                                Collections.emptyList(),
                                options,
                                ""));
        return FileStoreTableFactory.create(fileIO, tablePath, schema);
    }

    private List<DataSplit> splits(FileStoreTable table) {
        return table.newScan().plan().splits().stream()
                .map(s -> (DataSplit) s)
                .collect(Collectors.toList());
    }

    private void write(FileStoreTable table, GenericRow... rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }
}
