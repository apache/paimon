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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LookupTable} with remote files. */
public class LookupRemoteFileTableTest extends TableTestBase {

    @TempDir java.nio.file.Path tempDir;
    private IOManager ioManager;

    @BeforeEach
    public void before() throws IOException {
        this.ioManager = new IOManagerImpl(tempDir.toString());
    }

    @Test
    public void testRemoteFile() throws Exception {
        innerTestRemoteFile(false, false);
    }

    @Test
    public void testRemoteFileSchemEvolution() throws Exception {
        innerTestRemoteFile(true, false);
    }

    @Test
    public void testRemoteFileSchemEvolutionAndNotCompatible() throws Exception {
        innerTestRemoteFile(true, true);
    }

    private void innerTestRemoteFile(boolean schemaEvolution, boolean notCompatible)
            throws Exception {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        options.set(CoreOptions.LOOKUP_REMOTE_FILE_ENABLED, true);
        Identifier identifier = new Identifier("default", "t");
        Schema schema =
                new Schema(
                        RowType.of(new IntType(), new IntType()).getFields(),
                        Collections.emptyList(),
                        Collections.singletonList("f0"),
                        options.toMap(),
                        null);
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        LocalFileIO fileIO = LocalFileIO.create();

        // first write
        try (BatchTableWrite write = writeBuilder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1, 1));
            write.write(GenericRow.of(2, 1));
            write.write(GenericRow.of(3, 1));
            commit.commit(write.prepareCommit());
        }

        // second write
        try (BatchTableWrite write = writeBuilder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1, 1));
            write.write(GenericRow.of(4, 1));
            write.write(GenericRow.of(5, 1));
            commit.commit(write.prepareCommit());
        }

        // plan to assert
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        assertThat(splits).hasSize(1);
        allShouldHaveRemoteSst(splits);

        // third write with lookup but no data file

        // delete file first
        DataSplit firstSplit = (DataSplit) splits.get(0);
        DataFileMeta firstFile = firstSplit.dataFiles().get(0);
        Path firstPath =
                table.store()
                        .pathFactory()
                        .createDataFilePathFactory(firstSplit.partition(), firstSplit.bucket())
                        .toPath(firstFile);
        Path tmpPath = new Path(firstPath.getParent(), "tmp_file");
        fileIO.copyFile(firstPath, tmpPath, false);
        fileIO.delete(firstPath, false);

        // should no exception when lookup in write
        if (schemaEvolution) {
            catalog.alterTable(
                    identifier,
                    SchemaChange.setOption(CoreOptions.COMPACTION_SIZE_RATIO.key(), "0"),
                    false);
            if (notCompatible) {
                catalog.alterTable(
                        identifier, SchemaChange.addColumn("new_col", DataTypes.INT()), false);
            }
            table = (FileStoreTable) catalog.getTable(identifier);
        } else {
            table =
                    table.copy(
                            Collections.singletonMap(CoreOptions.COMPACTION_SIZE_RATIO.key(), "0"));
        }
        writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1, 2, 2));
            write.write(GenericRow.of(2, 2, 2));
            if (notCompatible) {
                assertThatThrownBy(() -> commit.commit(write.prepareCommit()))
                        .hasMessageContaining("FileNotFoundException");
                return;
            } else {
                commit.commit(write.prepareCommit());
            }
        }

        // check remote lookup files
        splits = readBuilder.newScan().plan().splits();
        allShouldHaveRemoteSst(splits);

        // restore file and check reading
        fileIO.copyFile(tmpPath, firstPath, false);
        List<GenericRow> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(splits)
                .forEachRemaining(r -> result.add(GenericRow.of(r.getInt(0), r.getInt(1))));
        assertThat(result)
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 2),
                        GenericRow.of(2, 2),
                        GenericRow.of(3, 1),
                        GenericRow.of(4, 1),
                        GenericRow.of(5, 1));
    }

    private void allShouldHaveRemoteSst(List<Split> check) {
        for (Split split : check) {
            for (DataFileMeta file : ((DataSplit) split).dataFiles()) {
                List<String> extraFiles = file.extraFiles();
                String extraFile = extraFiles.get(0);
                // data-410685c7-4cc2-47d7-9dec-393f6cfe9d64-0.parquet.115.position.v1.lookup
                assertThat(extraFile).endsWith(".position.v1.lookup");
                long lookupFileSize;
                try {
                    lookupFileSize =
                            LocalFileIO.create()
                                    .getFileSize(
                                            new Path(
                                                    new Path(tempPath.toUri()),
                                                    "default.db/t/bucket-0/" + extraFile));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                String[] extraFileSplit = extraFile.split("\\.");
                assertThat(extraFileSplit[extraFileSplit.length - 4])
                        .isEqualTo(String.valueOf(lookupFileSize));
            }
        }
    }

    @Test
    public void testRemoteFileLevelThreshold() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        options.set(CoreOptions.LOOKUP_REMOTE_FILE_ENABLED, true);
        options.set(CoreOptions.LOOKUP_REMOTE_LEVEL_THRESHOLD, 5);
        Identifier identifier = new Identifier("default", "t");
        Schema schema =
                new Schema(
                        RowType.of(new IntType(), new IntType()).getFields(),
                        Collections.emptyList(),
                        Collections.singletonList("f0"),
                        options.toMap(),
                        null);
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

        // first write
        try (BatchTableWrite write = writeBuilder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1, 1));
            write.write(GenericRow.of(2, 1));
            write.write(GenericRow.of(3, 1));
            commit.commit(write.prepareCommit());
        }

        // second write
        try (BatchTableWrite write = writeBuilder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1, 1));
            write.write(GenericRow.of(4, 1));
            write.write(GenericRow.of(5, 1));
            commit.commit(write.prepareCommit());
        }

        // third write generate level 4
        table = table.copy(Collections.singletonMap(CoreOptions.COMPACTION_SIZE_RATIO.key(), "0"));
        writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1, 2, 2));
            write.write(GenericRow.of(2, 2, 2));
            commit.commit(write.prepareCommit());
        }

        // assert level threshold
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        assertThat(splits).hasSize(1);
        List<DataFileMeta> files = ((DataSplit) splits.get(0)).dataFiles();
        DataFileMeta level5 = files.stream().filter(f -> f.level() == 5).findFirst().get();
        DataFileMeta level4 = files.stream().filter(f -> f.level() == 4).findFirst().get();
        assertThat(level5.extraFiles()).hasSize(1);
        assertThat(level4.extraFiles()).hasSize(0);
    }
}
