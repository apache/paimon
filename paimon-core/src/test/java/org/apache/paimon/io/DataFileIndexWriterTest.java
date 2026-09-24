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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests for {@link DataFileIndexWriter}. */
public class DataFileIndexWriterTest {

    @TempDir java.nio.file.Path tempFile;

    FileIO fileIO = LocalFileIO.create();

    boolean bitmapExist = false;
    boolean bsiExist = false;
    boolean bloomExists = false;

    @Test
    public void testSpillableIndexOutputStream() throws Exception {
        Path path = new Path(tempFile.resolve("index.idx").toUri());
        SpillableIndexOutputStream embedded = new SpillableIndexOutputStream(fileIO, path, 4);
        embedded.write(new byte[] {1, 2, 3, 4});
        embedded.close();
        assertThat(embedded.spilled()).isFalse();
        assertThat(embedded.embeddedBytes()).containsExactly(1, 2, 3, 4);

        SpillableIndexOutputStream external = new SpillableIndexOutputStream(fileIO, path, 4);
        external.write(new byte[] {1, 2, 3, 4});
        external.write(5);
        external.close();
        assertThat(external.spilled()).isTrue();
        try (org.apache.paimon.fs.SeekableInputStream input = fileIO.newInputStream(path)) {
            byte[] bytes = new byte[5];
            input.read(bytes);
            assertThat(bytes).containsExactly(1, 2, 3, 4, 5);
        }
        external.abort();
        assertThat(fileIO.exists(path)).isFalse();
    }

    @Test
    public void testV2TableWriterStreamsPayloadOverTwoGiB() throws Exception {
        Options options = new Options();
        options.setString("file-index.format.version", "2");
        options.setString("file-index.in-manifest-threshold", "1B");
        options.setString("file-index.stream-test.columns", "large,small");
        options.setString("file-index.stream-test.large.large", "true");
        Path path = new Path(tempFile.resolve("large.index").toUri());
        FileIO sparseFileIO = new SparseFileIndexIO();
        RowType rowType =
                RowType.builder()
                        .field("large", DataTypes.INT())
                        .field("small", DataTypes.INT())
                        .build();
        DataFileIndexWriter writer =
                new DataFileIndexWriter(
                        sparseFileIO,
                        path,
                        rowType,
                        new FileIndexOptions(new CoreOptions(options)),
                        null);
        writer.write(GenericRow.of(1, 2));
        writer.close();
        assertThat(writer.result().independentIndexFile()).isEqualTo(path.getName());

        long length = sparseFileIO.getFileStatus(path).getLen();
        assertThat(length).isGreaterThan(Integer.MAX_VALUE);
        try (FileIndexFormat.Reader reader =
                FileIndexFormat.createReader(sparseFileIO.newInputStream(path), rowType, length)) {
            assertThat(reader.indexMetas())
                    .filteredOn(meta -> meta.columnName().equals("large"))
                    .extracting(FileIndexFormat.FileIndexMeta::sizeInBytes)
                    .containsExactly(2049L * 1024 * 1024 + 1);
            assertThat(reader.readColumnIndex("large")).hasSize(1);
            assertThat(reader.readColumnIndex("small")).hasSize(1);
        }
    }

    @Test
    public void testV2FailedWriteDeletesPartialIndexFile() throws Exception {
        Options options = new Options();
        options.setString("file-index.format.version", "2");
        options.setString("file-index.in-manifest-threshold", "1B");
        options.setString("file-index.stream-test.columns", "a");
        options.setString("file-index.stream-test.a.fail", "true");
        Path path = new Path(tempFile.resolve("failed.index").toUri());
        DataFileIndexWriter writer =
                new DataFileIndexWriter(
                        fileIO,
                        path,
                        RowType.builder().field("a", DataTypes.INT()).build(),
                        new FileIndexOptions(new CoreOptions(options)),
                        null);
        writer.write(GenericRow.of(1));
        assertThatThrownBy(writer::close)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Test index write failure");
        assertThat(fileIO.exists(path)).isFalse();
        assertThat(writer.result().independentIndexFile()).isNull();
    }

    @Test
    public void testV2FailedWriteReportsDeleteFailure() throws Exception {
        Options options = new Options();
        options.setString("file-index.format.version", "2");
        options.setString("file-index.in-manifest-threshold", "1B");
        options.setString("file-index.stream-test.columns", "a");
        options.setString("file-index.stream-test.a.fail", "true");
        Path path = new Path(tempFile.resolve("undeleted.index").toUri());
        FileIO deleteFailingFileIO =
                new LocalFileIO() {
                    @Override
                    public boolean delete(Path path, boolean recursive) {
                        return false;
                    }
                };
        DataFileIndexWriter writer =
                new DataFileIndexWriter(
                        deleteFailingFileIO,
                        path,
                        RowType.builder().field("a", DataTypes.INT()).build(),
                        new FileIndexOptions(new CoreOptions(options)),
                        null);
        writer.write(GenericRow.of(1));

        Throwable failure = catchThrowable(writer::close);
        assertThat(failure)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Test index write failure");
        assertThat(failure.getSuppressed()).hasSize(1);
        assertThat(failure.getSuppressed()[0])
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to delete partial file index file");
        assertThat(deleteFailingFileIO.exists(path)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testCreatingMultipleIndexesOnOneColumn(int version) throws Exception {

        String tableName = "test";
        String col1 = "f0";
        String col2 = "f1";
        Identifier identifier = Identifier.create(tableName, tableName);

        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("file-index.format.version", Integer.toString(version));
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
                Path indexPath = dataFilePathFactory.toAlignedPath(indexFiles.get(0), dataFileMeta);
                try (FileIndexFormat.Reader reader =
                        FileIndexFormat.createReader(
                                fileIO.newInputStream(indexPath),
                                tableSchema.logicalRowType(),
                                fileIO.getFileStatus(indexPath).getLen())) {
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
