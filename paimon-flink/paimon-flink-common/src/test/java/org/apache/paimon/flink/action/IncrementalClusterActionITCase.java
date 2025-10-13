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

package org.apache.paimon.flink.action;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for incremental clustering action. */
public class IncrementalClusterActionITCase extends ActionITCaseBase {

    @Test
    public void testClusterUnpartitionedTable() throws Exception {
        FileStoreTable table = createTable(null);

        BinaryString randomStr =
                BinaryString.fromString(
                        UUID.randomUUID().toString() + UUID.randomUUID() + UUID.randomUUID());
        List<CommitMessage> messages1 = new ArrayList<>();

        // first write
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                messages1.addAll(write(GenericRow.of(i, j, randomStr)));
            }
        }
        commit(messages1);
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {0, 1});
        List<String> result1 =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        readBuilder.readType());
        List<String> expected1 =
                Lists.newArrayList(
                        "+I[0, 0]",
                        "+I[0, 1]",
                        "+I[0, 2]",
                        "+I[1, 0]",
                        "+I[1, 1]",
                        "+I[1, 2]",
                        "+I[2, 0]",
                        "+I[2, 1]",
                        "+I[2, 2]");
        assertThat(result1).containsExactlyElementsOf(expected1);

        // first cluster
        runAction(Collections.emptyList());
        checkSnapshot(table);
        List<Split> splits = readBuilder.newScan().plan().splits();
        assertThat(splits.size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(0).level()).isEqualTo(5);
        List<String> result2 = getResult(readBuilder.newRead(), splits, readBuilder.readType());
        List<String> expected2 =
                Lists.newArrayList(
                        "+I[0, 0]",
                        "+I[0, 1]",
                        "+I[1, 0]",
                        "+I[1, 1]",
                        "+I[0, 2]",
                        "+I[1, 2]",
                        "+I[2, 0]",
                        "+I[2, 1]",
                        "+I[2, 2]");
        assertThat(result2).containsExactlyElementsOf(expected2);

        // second write
        messages1.clear();
        messages1.addAll(
                write(
                        GenericRow.of(0, 3, null),
                        GenericRow.of(1, 3, null),
                        GenericRow.of(2, 3, null)));
        messages1.addAll(
                write(
                        GenericRow.of(3, 0, null),
                        GenericRow.of(3, 1, null),
                        GenericRow.of(3, 2, null),
                        GenericRow.of(3, 3, null)));
        commit(messages1);

        List<String> result3 =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        readBuilder.readType());
        List<String> expected3 = new ArrayList<>(expected2);
        expected3.addAll(
                Lists.newArrayList(
                        "+I[0, 3]",
                        "+I[1, 3]",
                        "+I[2, 3]",
                        "+I[3, 0]",
                        "+I[3, 1]",
                        "+I[3, 2]",
                        "+I[3, 3]"));
        assertThat(result3).containsExactlyElementsOf(expected3);

        // second cluster
        runAction(Collections.emptyList());
        checkSnapshot(table);
        splits = readBuilder.newScan().plan().splits();
        List<String> result4 = getResult(readBuilder.newRead(), splits, readBuilder.readType());
        List<String> expected4 = new ArrayList<>(expected2);
        expected4.addAll(
                Lists.newArrayList(
                        "+I[0, 3]",
                        "+I[1, 3]",
                        "+I[3, 0]",
                        "+I[3, 1]",
                        "+I[2, 3]",
                        "+I[3, 2]",
                        "+I[3, 3]"));
        assertThat(splits.size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().size()).isEqualTo(2);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(0).level()).isEqualTo(5);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(1).level()).isEqualTo(4);
        assertThat(result4).containsExactlyElementsOf(expected4);

        // full cluster
        runAction(Lists.newArrayList("--compact_strategy", "full"));
        checkSnapshot(table);
        splits = readBuilder.newScan().plan().splits();
        List<String> result5 = getResult(readBuilder.newRead(), splits, readBuilder.readType());
        List<String> expected5 = new ArrayList<>();
        expected5.addAll(
                Lists.newArrayList(
                        "+I[0, 0]",
                        "+I[0, 1]",
                        "+I[1, 0]",
                        "+I[1, 1]",
                        "+I[0, 2]",
                        "+I[0, 3]",
                        "+I[1, 2]",
                        "+I[1, 3]",
                        "+I[2, 0]",
                        "+I[2, 1]",
                        "+I[3, 0]",
                        "+I[3, 1]",
                        "+I[2, 2]",
                        "+I[2, 3]",
                        "+I[3, 2]",
                        "+I[3, 3]"));
        assertThat(splits.size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(0).level()).isEqualTo(5);
        assertThat(result5).containsExactlyElementsOf(expected5);
    }

    @Test
    public void testClusterPartitionedTable() throws Exception {
        FileStoreTable table = createTable(null);

        BinaryString randomStr =
                BinaryString.fromString(
                        UUID.randomUUID().toString() + UUID.randomUUID() + UUID.randomUUID());
        List<CommitMessage> messages1 = new ArrayList<>();

        // first write
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                messages1.addAll(write(GenericRow.of(i, j, randomStr)));
            }
        }
        commit(messages1);
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {0, 1});
        List<String> result1 =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        readBuilder.readType());
        List<String> expected1 =
                Lists.newArrayList(
                        "+I[0, 0]",
                        "+I[0, 1]",
                        "+I[0, 2]",
                        "+I[1, 0]",
                        "+I[1, 1]",
                        "+I[1, 2]",
                        "+I[2, 0]",
                        "+I[2, 1]",
                        "+I[2, 2]");
        assertThat(result1).containsExactlyElementsOf(expected1);

        // first cluster
        runAction(Collections.emptyList());
        checkSnapshot(table);
        List<Split> splits = readBuilder.newScan().plan().splits();
        assertThat(splits.size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(0).level()).isEqualTo(5);
        List<String> result2 = getResult(readBuilder.newRead(), splits, readBuilder.readType());
        List<String> expected2 =
                Lists.newArrayList(
                        "+I[0, 0]",
                        "+I[0, 1]",
                        "+I[1, 0]",
                        "+I[1, 1]",
                        "+I[0, 2]",
                        "+I[1, 2]",
                        "+I[2, 0]",
                        "+I[2, 1]",
                        "+I[2, 2]");
        assertThat(result2).containsExactlyElementsOf(expected2);

        // second write
        messages1.clear();
        messages1.addAll(
                write(
                        GenericRow.of(0, 3, null),
                        GenericRow.of(1, 3, null),
                        GenericRow.of(2, 3, null)));
        messages1.addAll(
                write(
                        GenericRow.of(3, 0, null),
                        GenericRow.of(3, 1, null),
                        GenericRow.of(3, 2, null),
                        GenericRow.of(3, 3, null)));
        commit(messages1);

        List<String> result3 =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        readBuilder.readType());
        List<String> expected3 = new ArrayList<>(expected2);
        expected3.addAll(
                Lists.newArrayList(
                        "+I[0, 3]",
                        "+I[1, 3]",
                        "+I[2, 3]",
                        "+I[3, 0]",
                        "+I[3, 1]",
                        "+I[3, 2]",
                        "+I[3, 3]"));
        assertThat(result3).containsExactlyElementsOf(expected3);

        // second cluster
        runAction(Collections.emptyList());
        checkSnapshot(table);
        splits = readBuilder.newScan().plan().splits();
        List<String> result4 = getResult(readBuilder.newRead(), splits, readBuilder.readType());
        List<String> expected4 = new ArrayList<>(expected2);
        expected4.addAll(
                Lists.newArrayList(
                        "+I[0, 3]",
                        "+I[1, 3]",
                        "+I[3, 0]",
                        "+I[3, 1]",
                        "+I[2, 3]",
                        "+I[3, 2]",
                        "+I[3, 3]"));
        assertThat(splits.size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().size()).isEqualTo(2);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(0).level()).isEqualTo(5);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(1).level()).isEqualTo(4);
        assertThat(result4).containsExactlyElementsOf(expected4);

        // full cluster
        runAction(Lists.newArrayList("--compact_strategy", "full"));
        checkSnapshot(table);
        splits = readBuilder.newScan().plan().splits();
        List<String> result5 = getResult(readBuilder.newRead(), splits, readBuilder.readType());
        List<String> expected5 = new ArrayList<>();
        expected5.addAll(
                Lists.newArrayList(
                        "+I[0, 0]",
                        "+I[0, 1]",
                        "+I[1, 0]",
                        "+I[1, 1]",
                        "+I[0, 2]",
                        "+I[0, 3]",
                        "+I[1, 2]",
                        "+I[1, 3]",
                        "+I[2, 0]",
                        "+I[2, 1]",
                        "+I[3, 0]",
                        "+I[3, 1]",
                        "+I[2, 2]",
                        "+I[2, 3]",
                        "+I[3, 2]",
                        "+I[3, 3]"));
        assertThat(splits.size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(0).level()).isEqualTo(5);
        assertThat(result5).containsExactlyElementsOf(expected5);
    }

    protected FileStoreTable createTable(String partitionKeys) throws Exception {
        catalog.createDatabase(database, true);
        catalog.createTable(identifier(), schema(partitionKeys), true);
        return (FileStoreTable) catalog.getTable(identifier());
    }

    private FileStoreTable getTable() throws Exception {
        return (FileStoreTable) catalog.getTable(identifier());
    }

    private Identifier identifier() {
        return Identifier.create(database, tableName);
    }

    private List<CommitMessage> write(GenericRow... data) throws Exception {
        BatchWriteBuilder builder = getTable().newBatchWriteBuilder();
        try (BatchTableWrite batchTableWrite = builder.newWrite()) {
            for (GenericRow row : data) {
                batchTableWrite.write(row);
            }
            return batchTableWrite.prepareCommit();
        }
    }

    private void commit(List<CommitMessage> messages) throws Exception {
        BatchTableCommit commit = getTable().newBatchWriteBuilder().newCommit();
        commit.commit(messages);
        commit.close();
    }

    private static Schema schema(String partitionKeys) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("a", DataTypes.INT());
        schemaBuilder.column("b", DataTypes.INT());
        schemaBuilder.column("c", DataTypes.STRING());
        schemaBuilder.option("bucket", "-1");
        schemaBuilder.option("num-levels", "6");
        schemaBuilder.option("num-sorted-run.compaction-trigger", "2");
        schemaBuilder.option("clustering.columns", "a,b");
        schemaBuilder.option("clustering.strategy", "zorder");
        schemaBuilder.option("clustering.incremental", "true");
        schemaBuilder.option("scan.parallelism", "1");
        schemaBuilder.option("sink.parallelism", "1");
        if (!StringUtils.isNullOrWhitespaceOnly(partitionKeys)) {
            schemaBuilder.partitionKeys(partitionKeys);
        }
        return schemaBuilder.build();
    }

    private void checkSnapshot(FileStoreTable table) {
        assertThat(table.latestSnapshot().get().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
    }

    private void runAction(List<String> extra) throws Exception {
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        ArrayList<String> baseArgs =
                Lists.newArrayList("compact", "--database", database, "--table", tableName);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        if (random.nextBoolean()) {
            baseArgs.addAll(Lists.newArrayList("--warehouse", warehouse));
        } else {
            baseArgs.addAll(Lists.newArrayList("--catalog_conf", "warehouse=" + warehouse));
        }
        baseArgs.addAll(extra);

        CompactAction action = createAction(CompactAction.class, baseArgs.toArray(new String[0]));
        //        action.withStreamExecutionEnvironment(env).build();
        //        env.execute();
        action.withStreamExecutionEnvironment(env);
        action.run();
    }
}
