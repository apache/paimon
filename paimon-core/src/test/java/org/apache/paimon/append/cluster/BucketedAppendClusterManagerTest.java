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

package org.apache.paimon.append.cluster;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BucketedAppendClusterManager}. */
public class BucketedAppendClusterManagerTest {

    @TempDir java.nio.file.Path tempDir;
    @TempDir java.nio.file.Path ioManagerTempDir;

    FileStoreTable table;
    BaseAppendFileStoreWrite write;
    StreamTableCommit commit;

    @BeforeEach
    public void before() throws Exception {
        table = createFileStoreTable();
        write =
                (BaseAppendFileStoreWrite)
                        table.store()
                                .newWrite("ss")
                                .withIOManager(IOManager.create(ioManagerTempDir.toString()));
        commit = table.newStreamWriteBuilder().newCommit();

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                write.write(BinaryRow.EMPTY_ROW, 0, GenericRow.of(0, i, j));
                commit.commit(i, write.prepareCommit(false, i));
            }
        }
    }

    @Test
    public void testBucketedAppendClusterTask() throws Exception {
        List<DataFileMeta> toCluster =
                table.newSnapshotReader().read().dataSplits().get(0).dataFiles();

        BucketedAppendClusterManager.BucketedAppendClusterTask task =
                new BucketedAppendClusterManager.BucketedAppendClusterTask(
                        toCluster, 5, files -> write.clusterRewrite(BinaryRow.EMPTY_ROW, 0, files));

        CompactResult result = task.doCompact();
        assertThat(result.before().size()).isEqualTo(9);
        assertThat(result.after().size()).isEqualTo(1);
        List<String> rows = new ArrayList<>();
        try (RecordReaderIterator<InternalRow> clusterRows =
                new RecordReaderIterator<>(
                        ((AppendOnlyFileStoreTable) table)
                                .store()
                                .newRead()
                                .createReader(
                                        BinaryRow.EMPTY_ROW, 0, result.after(), (List) null))) {
            while (clusterRows.hasNext()) {
                InternalRow row = clusterRows.next();
                rows.add(String.format("%d,%d", row.getInt(1), row.getInt(2)));
            }
        }

        assertThat(rows)
                .containsExactly("0,0", "0,1", "1,0", "1,1", "0,2", "1,2", "2,0", "2,1", "2,2");
    }

    @Test
    public void testTriggerCompaction() throws Exception {
        List<DataFileMeta> toCluster =
                table.newSnapshotReader().read().dataSplits().get(0).dataFiles();
        CoreOptions options = table.coreOptions();
        BucketedAppendClusterManager manager =
                new BucketedAppendClusterManager(
                        Executors.newSingleThreadExecutor(),
                        toCluster,
                        table.schemaManager(),
                        options.clusteringColumns(),
                        options.maxSizeAmplificationPercent(),
                        options.sortedRunSizeRatio(),
                        options.numSortedRunCompactionTrigger(),
                        options.numLevels(),
                        files -> write.clusterRewrite(BinaryRow.EMPTY_ROW, 0, files));
        assertThat(manager.levels().levelSortedRuns().size()).isEqualTo(9);

        manager.triggerCompaction(false);

        CompactResult compactResult = manager.getCompactionResult(true).get();
        assertThat(compactResult.before().size()).isEqualTo(9);
        assertThat(compactResult.after().size()).isEqualTo(1);

        assertThat(manager.levels().levelSortedRuns().size()).isEqualTo(1);
        assertThat(manager.levels().levelSortedRuns().get(0).level()).isEqualTo(5);
    }

    private FileStoreTable createFileStoreTable() throws Exception {
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.INT())
                        .column("f2", DataTypes.INT())
                        .option("bucket", "1")
                        .option("bucket-key", "f0")
                        .option("compaction.min.file-num", "10")
                        .option("clustering.columns", "f1, f2")
                        .option("clustering.strategy", "zorder")
                        .build();
        Identifier identifier = Identifier.create("default", "test");
        catalog.createDatabase("default", false);
        catalog.createTable(identifier, schema, false);
        return (FileStoreTable) catalog.getTable(identifier);
    }
}
