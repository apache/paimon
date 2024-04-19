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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndex;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.IndexFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.io.DataFilePathFactory.INDEX_PATH_SUFFIX;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SnapshotReader}. */
public class SnapshotReaderTest {

    private @TempDir java.nio.file.Path tempDir;

    private Path tablePath;
    private FileIO fileIO;

    @BeforeEach
    public void before() throws Exception {
        tablePath = new Path(TraceableFileIO.SCHEME + "://" + tempDir);
        fileIO = FileIOFinder.find(tablePath);
    }

    @Test
    public void testGetPrimaryKeyRawFiles() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "k", "v"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType, Collections.singletonList("pt"), Arrays.asList("pt", "k"));

        String commitUser = UUID.randomUUID().toString();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        SnapshotReader reader = table.newSnapshotReader();

        // write one file on level 0

        write.write(GenericRow.of(BinaryString.fromString("one"), 11, 1101L));
        write.write(GenericRow.of(BinaryString.fromString("one"), 12, 1201L));
        write.write(GenericRow.of(BinaryString.fromString("two"), 21, 2101L));
        write.write(GenericRow.of(BinaryString.fromString("two"), 22, 2201L));
        commit.commit(1, write.prepareCommit(false, 1));

        List<DataSplit> dataSplits = reader.read().dataSplits();
        assertThat(dataSplits).hasSize(2);
        for (DataSplit dataSplit : dataSplits) {
            assertThat(dataSplit.dataFiles()).hasSize(1);
            DataFileMeta meta = dataSplit.dataFiles().get(0);
            String partition = dataSplit.partition().getString(0).toString();
            assertThat(dataSplit.convertToRawFiles()).isPresent();
        }

        // write another file on level 0

        write.write(GenericRow.of(BinaryString.fromString("one"), 11, 1102L));
        write.write(GenericRow.of(BinaryString.fromString("one"), 12, 1202L));
        write.write(GenericRow.of(BinaryString.fromString("two"), 21, 2102L));
        write.write(GenericRow.of(BinaryString.fromString("two"), 22, 2202L));
        commit.commit(2, write.prepareCommit(false, 2));

        dataSplits = reader.read().dataSplits();
        assertThat(dataSplits).hasSize(2);
        for (DataSplit dataSplit : dataSplits) {
            assertThat(dataSplit.dataFiles()).hasSize(2);
            assertThat(dataSplit.convertToRawFiles()).isNotPresent();
        }

        // compact all files

        InternalRowSerializer serializer =
                new InternalRowSerializer(RowType.of(DataTypes.STRING()));
        write.compact(
                serializer.toBinaryRow(GenericRow.of(BinaryString.fromString("one"))).copy(),
                0,
                true);
        write.compact(
                serializer.toBinaryRow(GenericRow.of(BinaryString.fromString("two"))).copy(),
                0,
                true);
        commit.commit(3, write.prepareCommit(true, 3));

        dataSplits = reader.read().dataSplits();
        assertThat(dataSplits).hasSize(2);
        for (DataSplit dataSplit : dataSplits) {
            assertThat(dataSplit.dataFiles()).hasSize(1);
            DataFileMeta meta = dataSplit.dataFiles().get(0);
            String partition = dataSplit.partition().getString(0).toString();
            assertThat(dataSplit.convertToRawFiles())
                    .hasValue(
                            Collections.singletonList(
                                    new RawFile(
                                            String.format(
                                                    "%s/pt=%s/bucket-0/%s",
                                                    tablePath, partition, meta.fileName()),
                                            0,
                                            meta.fileSize(),
                                            meta.level() == 5 ? "orc" : "avro",
                                            meta.schemaId(),
                                            meta.rowCount())));
        }

        // write another file on level 0

        write.write(GenericRow.of(BinaryString.fromString("one"), 11, 1103L));
        write.write(GenericRow.of(BinaryString.fromString("one"), 12, 1203L));
        write.write(GenericRow.of(BinaryString.fromString("two"), 21, 2103L));
        write.write(GenericRow.of(BinaryString.fromString("two"), 22, 2203L));
        commit.commit(4, write.prepareCommit(false, 4));

        dataSplits = reader.read().dataSplits();
        assertThat(dataSplits).hasSize(2);
        for (DataSplit dataSplit : dataSplits) {
            assertThat(dataSplit.dataFiles()).hasSize(2);
            assertThat(dataSplit.convertToRawFiles()).isNotPresent();
        }

        write.close();
        commit.close();
    }

    @Test
    public void testGetAppendOnlyRawFiles() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});
        FileStoreTable table =
                createFileStoreTable(rowType, Collections.emptyList(), Collections.emptyList());

        String commitUser = UUID.randomUUID().toString();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        SnapshotReader reader = table.newSnapshotReader();

        // write one file

        write.write(GenericRow.of(11, 1101L));
        write.write(GenericRow.of(12, 1201L));
        write.write(GenericRow.of(21, 2101L));
        write.write(GenericRow.of(22, 2201L));
        commit.commit(1, write.prepareCommit(false, 1));

        List<DataSplit> dataSplits = reader.read().dataSplits();
        assertThat(dataSplits).hasSize(1);
        DataSplit dataSplit = dataSplits.get(0);
        assertThat(dataSplit.dataFiles()).hasSize(1);
        DataFileMeta meta = dataSplit.dataFiles().get(0);
        assertThat(dataSplit.convertToRawFiles())
                .hasValue(
                        Collections.singletonList(
                                new RawFile(
                                        String.format("%s/bucket-0/%s", tablePath, meta.fileName()),
                                        0,
                                        meta.fileSize(),
                                        "avro",
                                        meta.schemaId(),
                                        meta.rowCount())));

        // change file schema

        write.close();
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        schemaManager.commitChanges(SchemaChange.addColumn("v2", DataTypes.STRING()));
        table = table.copyWithLatestSchema();
        write = table.newWrite(commitUser);

        // write another file

        write.write(GenericRow.of(11, 1102L, BinaryString.fromString("eleven")));
        write.write(GenericRow.of(12, 1202L, BinaryString.fromString("twelve")));
        write.write(GenericRow.of(21, 2102L, BinaryString.fromString("twenty-one")));
        write.write(GenericRow.of(22, 2202L, BinaryString.fromString("twenty-two")));
        commit.commit(2, write.prepareCommit(false, 2));

        dataSplits = reader.read().dataSplits();
        assertThat(dataSplits).hasSize(1);
        dataSplit = dataSplits.get(0);
        assertThat(dataSplit.dataFiles()).hasSize(2);
        DataFileMeta meta0 = dataSplit.dataFiles().get(0);
        DataFileMeta meta1 = dataSplit.dataFiles().get(1);
        assertThat(dataSplit.convertToRawFiles())
                .hasValue(
                        Arrays.asList(
                                new RawFile(
                                        String.format(
                                                "%s/bucket-0/%s", tablePath, meta0.fileName()),
                                        0,
                                        meta0.fileSize(),
                                        "avro",
                                        meta0.schemaId(),
                                        meta0.rowCount()),
                                new RawFile(
                                        String.format(
                                                "%s/bucket-0/%s", tablePath, meta1.fileName()),
                                        0,
                                        meta1.fileSize(),
                                        "avro",
                                        meta1.schemaId(),
                                        meta1.rowCount())));

        write.close();
        commit.close();
    }

    @Test
    public void testGetAppendOnlyIndexFiles() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"k", "v"});
        FileStoreTable table =
                createFileStoreTable(rowType, Collections.emptyList(), Collections.emptyList());

        String commitUser = UUID.randomUUID().toString();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        SnapshotReader reader = table.newSnapshotReader();

        // write one file

        write.write(GenericRow.of(11, 1101L));
        write.write(GenericRow.of(12, 1201L));
        write.write(GenericRow.of(21, 2101L));
        write.write(GenericRow.of(22, 2201L));
        commit.commit(1, write.prepareCommit(false, 1));

        List<DataSplit> dataSplits = reader.read().dataSplits();
        assertThat(dataSplits).hasSize(1);
        DataSplit dataSplit = dataSplits.get(0);
        assertThat(dataSplit.dataFiles()).hasSize(1);
        DataFileMeta meta = dataSplit.dataFiles().get(0);
        assertThat(dataSplit.indexFiles())
                .hasValue(
                        Collections.singletonList(
                                new IndexFile(
                                        String.format(
                                                "%s/bucket-0/%s" + INDEX_PATH_SUFFIX,
                                                tablePath,
                                                meta.fileName()))));

        // change file schema

        write.close();
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        schemaManager.commitChanges(SchemaChange.addColumn("v2", DataTypes.STRING()));
        table = table.copyWithLatestSchema();
        write = table.newWrite(commitUser);

        // write another file

        write.write(GenericRow.of(11, 1102L, BinaryString.fromString("eleven")));
        write.write(GenericRow.of(12, 1202L, BinaryString.fromString("twelve")));
        write.write(GenericRow.of(21, 2102L, BinaryString.fromString("twenty-one")));
        write.write(GenericRow.of(22, 2202L, BinaryString.fromString("twenty-two")));
        commit.commit(2, write.prepareCommit(false, 2));

        dataSplits = reader.read().dataSplits();
        assertThat(dataSplits).hasSize(1);
        dataSplit = dataSplits.get(0);
        assertThat(dataSplit.dataFiles()).hasSize(2);
        DataFileMeta meta0 = dataSplit.dataFiles().get(0);
        DataFileMeta meta1 = dataSplit.dataFiles().get(1);
        assertThat(dataSplit.indexFiles())
                .hasValue(
                        Arrays.asList(
                                new IndexFile(
                                        String.format(
                                                "%s/bucket-0/%s" + INDEX_PATH_SUFFIX,
                                                tablePath,
                                                meta0.fileName())),
                                new IndexFile(
                                        String.format(
                                                "%s/bucket-0/%s" + INDEX_PATH_SUFFIX,
                                                tablePath,
                                                meta1.fileName()))));

        write.close();
        commit.close();
    }

    private FileStoreTable createFileStoreTable(
            RowType rowType, List<String> partitionKeys, List<String> primaryKeys)
            throws Exception {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER, 5);
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FileFormatType.AVRO);
        Map<String, String> formatPerLevel = new HashMap<>();
        formatPerLevel.put("5", "orc");
        options.set(CoreOptions.FILE_FORMAT_PER_LEVEL, formatPerLevel);
        // test read with extra files
        options.set(
                CoreOptions.FILE_INDEX
                        + "."
                        + BloomFilterFileIndex.BLOOM_FILTER
                        + "."
                        + CoreOptions.COLUMNS,
                rowType.getFieldNames().get(0));

        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        TableSchema tableSchema =
                schemaManager.createTable(
                        new Schema(
                                rowType.getFields(),
                                partitionKeys,
                                primaryKeys,
                                options.toMap(),
                                ""));
        return FileStoreTableFactory.create(
                fileIO,
                tablePath,
                tableSchema,
                options,
                new CatalogEnvironment(Lock.emptyFactory(), null, null));
    }
}
