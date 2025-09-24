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

package org.apache.paimon.flink.sink;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.source.CompactorSourceBuilder;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CompactorSinkBuilder} and {@link CompactorSink}. */
public class CompactorSinkITCase extends AbstractTestBase {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()
                    },
                    new String[] {"k", "v", "hh", "dt"});

    private Path tablePath;
    private String commitUser;

    @BeforeEach
    public void before() throws IOException {
        tablePath = new Path(getTempDirPath());
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testCompact() throws Exception {
        FileStoreTable table = createFileStoreTable();
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();

        write.write(rowData(1, 100, 15, BinaryString.fromString("20221208")));
        write.write(rowData(1, 100, 16, BinaryString.fromString("20221208")));
        write.write(rowData(1, 100, 15, BinaryString.fromString("20221209")));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(2, 200, 15, BinaryString.fromString("20221208")));
        write.write(rowData(2, 200, 16, BinaryString.fromString("20221208")));
        write.write(rowData(2, 200, 15, BinaryString.fromString("20221209")));
        commit.commit(1, write.prepareCommit(true, 1));

        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(2);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

        write.close();
        commit.close();

        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        CompactorSourceBuilder sourceBuilder =
                new CompactorSourceBuilder(tablePath.toString(), table);
        DataStreamSource<RowData> source =
                sourceBuilder
                        .withEnv(env)
                        .withContinuousMode(false)
                        .withPartitionPredicate(
                                PartitionPredicate.fromMaps(
                                        table.schema().logicalPartitionType(),
                                        getSpecifiedPartitions(),
                                        table.coreOptions().partitionDefaultName()))
                        .build();
        new CompactorSinkBuilder(table, true).withInput(source).build();
        env.execute();

        snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(3);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        assertThat(plan.splits().size()).isEqualTo(3);
        for (Split split : plan.splits()) {
            DataSplit dataSplit = (DataSplit) split;
            if (dataSplit.partition().getInt(1) == 15) {
                // compacted
                assertThat(dataSplit.dataFiles().size()).isEqualTo(1);
            } else {
                // not compacted
                assertThat(dataSplit.dataFiles().size()).isEqualTo(2);
            }
        }
    }

    @Test
    public void testCompactParallelism() throws Exception {
        FileStoreTable table = createFileStoreTable();

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().streamingMode().build();
        CompactorSourceBuilder sourceBuilder =
                new CompactorSourceBuilder(tablePath.toString(), table);
        DataStreamSource<RowData> source =
                sourceBuilder
                        .withEnv(env)
                        .withContinuousMode(false)
                        .withPartitionPredicate(
                                PartitionPredicate.fromMaps(
                                        table.schema().logicalPartitionType(),
                                        getSpecifiedPartitions(),
                                        table.coreOptions().partitionDefaultName()))
                        .build();
        Integer sinkParalellism = new Random().nextInt(100) + 1;
        new CompactorSinkBuilder(
                        table.copy(
                                new HashMap<String, String>() {
                                    {
                                        put(
                                                FlinkConnectorOptions.SINK_PARALLELISM.key(),
                                                String.valueOf(sinkParalellism));
                                    }
                                }),
                        false)
                .withInput(source)
                .build();

        Assertions.assertThat(env.getTransformations().get(0).getParallelism())
                .isEqualTo(sinkParalellism);
    }

    private List<Map<String, String>> getSpecifiedPartitions() {
        Map<String, String> partition1 = new HashMap<>();
        partition1.put("dt", "20221208");
        partition1.put("hh", "15");

        Map<String, String> partition2 = new HashMap<>();
        partition2.put("dt", "20221209");
        partition2.put("hh", "15");

        return Arrays.asList(partition1, partition2);
    }

    private GenericRow rowData(Object... values) {
        return GenericRow.of(values);
    }

    private FileStoreTable createFileStoreTable() throws Exception {
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        TableSchema tableSchema =
                schemaManager.createTable(
                        new Schema(
                                ROW_TYPE.getFields(),
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                Collections.singletonMap("bucket", "1"),
                                ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }
}
