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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.ContextRecordingFileIndexerFactory.RecordedWriter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataFileIndexWriter}. */
public class DataFileIndexWriterTest {

    @TempDir java.nio.file.Path tempFile;

    FileIO fileIO = LocalFileIO.create();

    boolean bitmapExist = false;
    boolean bsiExist = false;
    boolean bloomExists = false;

    @Test
    public void testCreatingMultipleIndexesOnOneColumn() throws Exception {

        String tableName = "test";
        String col1 = "f0";
        String col2 = "f1";
        Identifier identifier = Identifier.create(tableName, tableName);

        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("file-index.bitmap.columns", col1);
        optionsMap.put("file-index.bsi.columns", col1);
        optionsMap.put("file-index.bloom-filter.columns", col2);
        optionsMap.put("file-index.read.enabled", "true");
        optionsMap.put("file-index.in-manifest-threshold", "1B");

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.options(optionsMap);
        schemaBuilder.column(col1, DataTypes.INT());
        schemaBuilder.column(col2, DataTypes.INT());
        Schema schema = schemaBuilder.build();

        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, tempFile.toUri().toString());
        catalogOptions.set(CACHE_ENABLED, false);
        CatalogContext context = CatalogContext.create(catalogOptions);
        FileSystemCatalog catalog = (FileSystemCatalog) CatalogFactory.createCatalog(context);
        catalog.createDatabase(tableName, false);
        catalog.createTable(identifier, schema, false);
        Table table = catalog.getTable(identifier);

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        IOManager ioManager = new IOManagerImpl("/tmp");
        BatchTableWrite write = writeBuilder.newWrite();
        write.withIOManager(ioManager);
        write.write(GenericRow.of(1, 1));
        write.write(GenericRow.of(1, 2));
        write.write(GenericRow.of(2, 3));
        List<CommitMessage> commitMessages = write.prepareCommit();
        writeBuilder.newCommit().commit(commitMessages);

        foreachIndexReader(
                catalog,
                tableName,
                col1,
                fileIndexReader -> {
                    String className = fileIndexReader.getClass().getName();
                    if (className.endsWith(".BitmapFileIndex$Reader")) {
                        bitmapExist = true;
                    } else if (className.endsWith(".BitSliceIndexBitmapFileIndex$Reader")) {
                        bsiExist = true;
                    } else {
                        throw new RuntimeException("unknown file index reader: " + className);
                    }
                    BitmapIndexResult result =
                            (BitmapIndexResult)
                                    fileIndexReader.visitEqual(
                                            new FieldRef(0, col1, DataTypes.INT()), 1);
                    assert result.get().equals(RoaringBitmap32.bitmapOf(0, 1));
                });

        foreachIndexReader(
                catalog,
                tableName,
                col2,
                fileIndexReader -> {
                    String className = fileIndexReader.getClass().getName();
                    if (className.endsWith(".BloomFilterFileIndex$Reader")) {
                        bloomExists = true;
                    }
                });

        assert bitmapExist;
        assert bsiExist;
        assert bloomExists;
    }

    @Test
    public void testIndexWritersReceiveDataFileOfAppendTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(contextRecordingColumns(), "k");
        options.put(CoreOptions.TARGET_FILE_ROW_NUM.key(), "10");
        Identifier identifier = Identifier.create("db", "append_table");

        try (FileSystemCatalog catalog =
                new FileSystemCatalog(fileIO, new Path(tempFile.toString()))) {
            catalog.createDatabase("db", false);
            catalog.createTable(
                    identifier,
                    Schema.newBuilder()
                            .column("k", DataTypes.INT())
                            .column("v", DataTypes.INT())
                            .options(options)
                            .build(),
                    false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            // three data files of schema 0 because of the row number limit
            writeRows(table, 25, i -> GenericRow.of(i, i));

            catalog.alterTable(identifier, SchemaChange.addColumn("w", DataTypes.INT()), false);
            table = (FileStoreTable) catalog.getTable(identifier);
            writeRows(table, 5, i -> GenericRow.of(100 + i, i, i));

            List<ManifestEntry> entries = assertIndexWritersMatchDataFiles(table, 0);
            assertThat(entries).hasSize(4);
            assertThat(entries.stream().map(entry -> entry.file().schemaId()))
                    .containsExactlyInAnyOrder(0L, 0L, 0L, 1L);
        }
    }

    @Test
    public void testIndexWritersReceiveDataFileOfPrimaryKeyTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(contextRecordingColumns(), "v");
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        Identifier identifier = Identifier.create("db", "pk_table");

        try (FileSystemCatalog catalog =
                new FileSystemCatalog(fileIO, new Path(tempFile.toString()))) {
            catalog.createDatabase("db", false);
            catalog.createTable(
                    identifier,
                    Schema.newBuilder()
                            .column("k", DataTypes.INT())
                            .column("v", DataTypes.INT())
                            .primaryKey("k")
                            .options(options)
                            .build(),
                    false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            // written in descending key order, stored in ascending key order
            writeRows(table, 20, i -> GenericRow.of(19 - i, (19 - i) * 10));

            assertThat(assertIndexWritersMatchDataFiles(table, 1)).isNotEmpty();
        }
    }

    @Test
    public void testNestedMapIndexWritersReceiveDataFile() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(contextRecordingColumns(), "m[k1],m[k2]");
        Identifier identifier = Identifier.create("db", "map_table");

        try (FileSystemCatalog catalog =
                new FileSystemCatalog(fileIO, new Path(tempFile.toString()))) {
            catalog.createDatabase("db", false);
            catalog.createTable(
                    identifier,
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                            .options(options)
                            .build(),
                    false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            List<GenericMap> maps =
                    Arrays.asList(
                            stringIntMap("k1", 1, "k2", 2),
                            stringIntMap("k2", 3),
                            null,
                            stringIntMap("k1", 4));
            writeRows(table, maps.size(), i -> GenericRow.of(i, maps.get(i)));

            List<ManifestEntry> entries = table.store().newScan().plan().files();
            assertThat(entries).hasSize(1);
            ManifestEntry entry = entries.get(0);
            Path path =
                    table.store()
                            .pathFactory()
                            .createDataFilePathFactory(entry.partition(), entry.bucket())
                            .toPath(entry.file());

            // one writer per nested key, each counting missing keys and null maps as nulls
            List<RecordedWriter> writers =
                    ContextRecordingFileIndexerFactory.recorded().get(path.toString());
            assertThat(writers).hasSize(2);
            assertThat(writers)
                    .allSatisfy(
                            writer ->
                                    assertThat(writer.schemaId).isEqualTo(entry.file().schemaId()));
            assertThat(writers)
                    .extracting(writer -> writer.values)
                    .containsExactlyInAnyOrder(
                            Arrays.asList(1, null, null, 4), Arrays.asList(2, 3, null, null));
        }
    }

    private static GenericMap stringIntMap(Object... keyValues) {
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(BinaryString.fromString((String) keyValues[i]), keyValues[i + 1]);
        }
        return new GenericMap(map);
    }

    private static String contextRecordingColumns() {
        return CoreOptions.FILE_INDEX
                + "."
                + ContextRecordingFileIndexerFactory.IDENTIFIER
                + "."
                + CoreOptions.COLUMNS;
    }

    private static void writeRows(FileStoreTable table, int rowCount, IntFunction<InternalRow> row)
            throws Exception {
        String commitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            for (int i = 0; i < rowCount; i++) {
                write.write(row.apply(i));
            }
            commit.commit(0, write.prepareCommit(true, 0));
        }
    }

    /**
     * Checks that every data file of the table had one index writer, which received the path and
     * schema id of that file and the indexed values in file order.
     */
    private static List<ManifestEntry> assertIndexWritersMatchDataFiles(
            FileStoreTable table, int indexedField) throws IOException {
        String tableLocation = table.location().toString();
        Map<String, List<RecordedWriter>> recorded =
                ContextRecordingFileIndexerFactory.recorded().entrySet().stream()
                        .filter(e -> e.getKey().startsWith(tableLocation))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        List<ManifestEntry> entries = table.store().newScan().plan().files();
        assertThat(recorded).hasSameSizeAs(entries);
        for (ManifestEntry entry : entries) {
            DataFileMeta file = entry.file();
            Path path =
                    table.store()
                            .pathFactory()
                            .createDataFilePathFactory(entry.partition(), entry.bucket())
                            .toPath(file);
            List<RecordedWriter> writers = recorded.get(path.toString());
            assertThat(writers).as("index writers of %s", path).hasSize(1);
            RecordedWriter writer = writers.get(0);
            assertThat(writer.schemaId).isEqualTo(file.schemaId());
            assertThat(writer.values)
                    .containsExactlyElementsOf(readIntField(table, entry, indexedField));
        }
        return entries;
    }

    private static List<Object> readIntField(FileStoreTable table, ManifestEntry entry, int field)
            throws IOException {
        DataSplit split =
                DataSplit.builder()
                        .withPartition(entry.partition())
                        .withBucket(entry.bucket())
                        .withBucketPath(
                                table.store()
                                        .pathFactory()
                                        .bucketPath(entry.partition(), entry.bucket())
                                        .toString())
                        .withTotalBuckets(entry.totalBuckets())
                        .withDataFiles(Collections.singletonList(entry.file()))
                        .rawConvertible(true)
                        .build();
        List<Object> values = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                table.newReadBuilder().newRead().createReader(split)) {
            reader.forEachRemaining(row -> values.add(row.getInt(field)));
        }
        return values;
    }

    protected void foreachIndexReader(
            FileSystemCatalog fileSystemCatalog,
            String tableName,
            String columnName,
            Consumer<FileIndexReader> consumer)
            throws Catalog.TableNotExistException {
        Path tableRoot =
                fileSystemCatalog.getTableLocation(Identifier.create(tableName, tableName));
        SchemaManager schemaManager = new FileSystemSchemaManager(fileIO, tableRoot);
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        tableRoot,
                        RowType.of(),
                        new CoreOptions(new Options()).partitionDefaultName(),
                        CoreOptions.FILE_FORMAT.defaultValue(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.PARTITION_GENERATE_LEGACY_NAME.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null,
                        null,
                        CoreOptions.ExternalPathStrategy.NONE,
                        null,
                        false,
                        null);

        Table table = fileSystemCatalog.getTable(Identifier.create(tableName, tableName));
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            DataFilePathFactory dataFilePathFactory =
                    pathFactory.createDataFilePathFactory(
                            dataSplit.partition(), dataSplit.bucket());
            for (DataFileMeta dataFileMeta : dataSplit.dataFiles()) {
                TableSchema tableSchema = schemaManager.schema(dataFileMeta.schemaId());
                List<String> indexFiles =
                        dataFileMeta.extraFiles().stream()
                                .filter(
                                        name ->
                                                name.endsWith(
                                                        DataFilePathFactory.INDEX_PATH_SUFFIX))
                                .collect(Collectors.toList());
                // assert index file exist and only one index file
                assert indexFiles.size() == 1;
                try (FileIndexFormat.Reader reader =
                        FileIndexFormat.createReader(
                                fileIO.newInputStream(
                                        dataFilePathFactory.toAlignedPath(
                                                indexFiles.get(0), dataFileMeta)),
                                tableSchema.logicalRowType())) {
                    Set<FileIndexReader> fileIndexReaders = reader.readColumnIndex(columnName);
                    for (FileIndexReader fileIndexReader : fileIndexReaders) {
                        consumer.accept(fileIndexReader);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
