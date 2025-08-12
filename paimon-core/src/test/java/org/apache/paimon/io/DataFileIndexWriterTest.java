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
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;

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

    protected void foreachIndexReader(
            FileSystemCatalog fileSystemCatalog,
            String tableName,
            String columnName,
            Consumer<FileIndexReader> consumer)
            throws Catalog.TableNotExistException {
        Path tableRoot =
                fileSystemCatalog.getTableLocation(Identifier.create(tableName, tableName));
        SchemaManager schemaManager = new SchemaManager(fileIO, tableRoot);
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
