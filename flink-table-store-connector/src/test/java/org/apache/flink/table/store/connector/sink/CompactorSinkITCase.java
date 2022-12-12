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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

/** IT cases for {@link CompactorSinkBuilder} and {@link CompactorSink}. */
public class CompactorSinkITCase extends AbstractTestBase {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType()
                    },
                    new String[] {"dt", "hh", "k", "v"});

    private Path tablePath;
    private String commitUser;

    @Before
    public void before() throws IOException {
        tablePath = new Path(TEMPORARY_FOLDER.newFolder().toString());
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testCompact() throws Exception {
        FileStoreTable table = createFileStoreTable();
        SnapshotManager snapshotManager = table.snapshotManager();
        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);

        write.write(rowData(20221208, 15, 1, 100));
        write.write(rowData(20221208, 16, 1, 100));
        write.write(rowData(20221209, 15, 1, 100));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(20221208, 15, 2, 200));
        write.write(rowData(20221208, 16, 2, 200));
        write.write(rowData(20221209, 15, 2, 200));
        commit.commit(1, write.prepareCommit(true, 1));

        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        Assert.assertEquals(2, snapshot.id());
        Assert.assertEquals(Snapshot.CommitKind.APPEND, snapshot.commitKind());

        write.close();
        commit.close();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<RowData> source =
                env.fromElements(rowData(20221208, 15, 0), rowData(20221209, 15, 0));
        new CompactorSinkBuilder(table).withInput(source).build();
        env.execute();

        snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        Assert.assertEquals(3, snapshot.id());
        Assert.assertEquals(Snapshot.CommitKind.COMPACT, snapshot.commitKind());

        DataTableScan.DataFilePlan plan = table.newScan().plan();
        Assert.assertEquals(3, plan.splits().size());
        for (DataSplit split : plan.splits) {
            if (split.partition().getInt(1) == 15) {
                // compacted
                Assert.assertEquals(1, split.files().size());
            } else {
                // not compacted
                Assert.assertEquals(2, split.files().size());
            }
        }
    }

    private GenericRowData rowData(Object... values) {
        return GenericRowData.of(values);
    }

    private FileStoreTable createFileStoreTable() throws Exception {
        SchemaManager schemaManager = new SchemaManager(tablePath);
        TableSchema tableSchema =
                schemaManager.commitNewVersion(
                        new UpdateSchema(
                                ROW_TYPE,
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                Collections.emptyMap(),
                                ""));
        return FileStoreTableFactory.create(tablePath, tableSchema);
    }
}
