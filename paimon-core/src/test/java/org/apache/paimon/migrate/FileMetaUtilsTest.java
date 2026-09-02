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
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileMetaUtils}, especially that {@code metadata.stats-mode} is honored. */
class FileMetaUtilsTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.BIGINT()}, new String[] {"a", "b"});

    @TempDir java.nio.file.Path tempDir;

    private Path tablePath;
    private Path externalLocation;

    @BeforeEach
    void before() {
        tablePath = new Path(String.format("%s://%s", TraceableFileIO.SCHEME, tempDir.toString()));
        externalLocation =
                new Path(
                        String.format(
                                "%s://%s/external", TraceableFileIO.SCHEME, tempDir.toString()));
    }

    /** With stats-mode=none, the migrated DataFileMeta must not carry column statistics. */
    @Test
    void testStatsModeNoneProducesNoColumnStats() throws Exception {
        FileStoreTable table = createFileStoreTable(CoreOptions.METADATA_STATS_MODE.key(), "none");

        String fileName = writeExternalParquetFile(table);

        List<DataFileMeta> metas =
                FileMetaUtils.construct(
                        table.fileIO(),
                        "parquet",
                        externalLocation.toString(),
                        table,
                        status -> true,
                        new Path(externalLocation, "migrated"),
                        new HashMap<>());

        assertThat(metas).hasSize(1);
        DataFileMeta meta = metas.get(0);
        assertThat(meta.fileName()).isEqualTo(fileName);
        // stats-mode=none + dense-store (default true): all columns are skipped -> empty stats
        SimpleStats valueStats = meta.valueStats();
        assertThat(valueStats.minValues().getFieldCount()).isZero();
        assertThat(valueStats.maxValues().getFieldCount()).isZero();
        assertThat(valueStats.nullCounts().size()).isZero();
        // row count must still be extracted from the file footer
        assertThat(meta.rowCount()).isEqualTo(2L);
    }

    /** With stats-mode=full, the migrated DataFileMeta must carry full column statistics. */
    @Test
    void testStatsModeFullProducesColumnStats() throws Exception {
        FileStoreTable table = createFileStoreTable(CoreOptions.METADATA_STATS_MODE.key(), "full");

        writeExternalParquetFile(table);

        List<DataFileMeta> metas =
                FileMetaUtils.construct(
                        table.fileIO(),
                        "parquet",
                        externalLocation.toString(),
                        table,
                        status -> true,
                        new Path(externalLocation, "migrated"),
                        new HashMap<>());

        assertThat(metas).hasSize(1);
        DataFileMeta meta = metas.get(0);
        assertThat(meta.rowCount()).isEqualTo(2L);

        SimpleStats valueStats = meta.valueStats();
        assertThat(valueStats.minValues().getFieldCount()).isEqualTo(2);
        assertThat(valueStats.maxValues().getFieldCount()).isEqualTo(2);
        assertThat(valueStats.minValues().getInt(0)).isEqualTo(1);
        assertThat(valueStats.maxValues().getInt(0)).isEqualTo(2);
        assertThat(valueStats.minValues().getLong(1)).isEqualTo(10L);
        assertThat(valueStats.maxValues().getLong(1)).isEqualTo(20L);
        assertThat(valueStats.nullCounts().getLong(0)).isZero();
        assertThat(valueStats.nullCounts().getLong(1)).isZero();
    }

    /**
     * Writes two rows through the table, locates the generated parquet data file, and copies it
     * into {@link #externalLocation} so it looks like an external file to be migrated. Returns the
     * copied file name.
     */
    private String writeExternalParquetFile(FileStoreTable table) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(1, 10L));
            write.write(GenericRow.of(2, 20L));
            commit.commit(write.prepareCommit());
        }

        List<Split> splits = table.newScan().plan().splits();
        assertThat(splits).hasSize(1);
        DataSplit dataSplit = (DataSplit) splits.get(0);
        List<DataFileMeta> dataFiles = dataSplit.dataFiles();
        assertThat(dataFiles).hasSize(1);

        DataFilePathFactory pathFactory =
                table.store()
                        .pathFactory()
                        .createDataFilePathFactory(dataSplit.partition(), dataSplit.bucket());
        Path dataFile = pathFactory.toPath(dataFiles.get(0));

        table.fileIO().mkdirs(externalLocation);
        Path target = new Path(externalLocation, dataFile.getName());
        table.fileIO().copyFile(dataFile, target, true);
        return dataFile.getName();
    }

    private FileStoreTable createFileStoreTable(String optionKey, String optionValue)
            throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_PARQUET);
        conf.set(CoreOptions.BUCKET, -1); // unaware bucket, single bucket dir
        conf.setString(optionKey, optionValue);

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                conf.toMap(),
                                ""));
        FileStoreTable table =
                new AppendOnlyFileStoreTable(
                        FileIOFinder.find(tablePath),
                        tablePath,
                        tableSchema,
                        CatalogEnvironment.empty());

        // sanity: ensure the option actually landed on the table
        assertThat(table.coreOptions().toMap()).containsEntry(optionKey, optionValue);
        return table;
    }
}
