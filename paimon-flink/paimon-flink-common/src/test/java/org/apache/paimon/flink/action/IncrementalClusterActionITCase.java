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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** IT cases for incremental clustering action. */
public class IncrementalClusterActionITCase extends ActionITCaseBase {

    @Test
    public void testClusterUnpartitionedTable() throws Exception {
        FileStoreTable table = createTable(null);

        BinaryString randomStr = BinaryString.fromString(randomString(150));
        List<CommitMessage> messages = new ArrayList<>();

        // first write
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                messages.addAll(write(GenericRow.of(i, j, randomStr, 0)));
            }
        }
        commit(messages);
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
        messages.clear();
        messages.addAll(
                write(
                        GenericRow.of(0, 3, null, 0),
                        GenericRow.of(1, 3, null, 0),
                        GenericRow.of(2, 3, null, 0)));
        messages.addAll(
                write(
                        GenericRow.of(3, 0, null, 0),
                        GenericRow.of(3, 1, null, 0),
                        GenericRow.of(3, 2, null, 0),
                        GenericRow.of(3, 3, null, 0)));
        commit(messages);

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
        List<String> expected5 =
                new ArrayList<>(
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
        FileStoreTable table = createTable("pt");

        BinaryString randomStr = BinaryString.fromString(randomString(150));
        List<CommitMessage> messages = new ArrayList<>();

        // first write
        List<String> expected1 = new ArrayList<>();
        for (int pt = 0; pt < 2; pt++) {
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < 3; j++) {
                    messages.addAll(write(GenericRow.of(i, j, (pt == 0) ? randomStr : null, pt)));
                    expected1.add(String.format("+I[%s, %s, %s]", i, j, pt));
                }
            }
        }
        commit(messages);
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {0, 1, 3});
        List<String> result1 =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        readBuilder.readType());
        assertThat(result1).containsExactlyElementsOf(expected1);

        // first cluster
        runAction(Collections.emptyList());
        checkSnapshot(table);
        List<Split> splits = readBuilder.newScan().plan().splits();
        assertThat(splits.size()).isEqualTo(2);
        assertThat(((DataSplit) splits.get(0)).dataFiles().size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(0).level()).isEqualTo(5);
        List<String> result2 = getResult(readBuilder.newRead(), splits, readBuilder.readType());
        List<String> expected2 = new ArrayList<>();
        for (int pt = 0; pt < 2; pt++) {
            expected2.add(String.format("+I[0, 0, %s]", pt));
            expected2.add(String.format("+I[0, 1, %s]", pt));
            expected2.add(String.format("+I[1, 0, %s]", pt));
            expected2.add(String.format("+I[1, 1, %s]", pt));
            expected2.add(String.format("+I[0, 2, %s]", pt));
            expected2.add(String.format("+I[1, 2, %s]", pt));
            expected2.add(String.format("+I[2, 0, %s]", pt));
            expected2.add(String.format("+I[2, 1, %s]", pt));
            expected2.add(String.format("+I[2, 2, %s]", pt));
        }
        assertThat(result2).containsExactlyElementsOf(expected2);

        // second write
        messages.clear();
        for (int pt = 0; pt < 2; pt++) {
            messages.addAll(
                    write(
                            GenericRow.of(0, 3, null, pt),
                            GenericRow.of(1, 3, null, pt),
                            GenericRow.of(2, 3, null, pt)));
            messages.addAll(
                    write(
                            GenericRow.of(3, 0, null, pt),
                            GenericRow.of(3, 1, null, pt),
                            GenericRow.of(3, 2, null, pt),
                            GenericRow.of(3, 3, null, pt)));
        }
        commit(messages);

        List<String> result3 =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        readBuilder.readType());
        List<String> expected3 = new ArrayList<>();
        for (int pt = 0; pt < 2; pt++) {
            expected3.addAll(expected2.subList(9 * pt, 9 * pt + 9));
            expected3.add(String.format("+I[0, 3, %s]", pt));
            expected3.add(String.format("+I[1, 3, %s]", pt));
            expected3.add(String.format("+I[2, 3, %s]", pt));
            expected3.add(String.format("+I[3, 0, %s]", pt));
            expected3.add(String.format("+I[3, 1, %s]", pt));
            expected3.add(String.format("+I[3, 2, %s]", pt));
            expected3.add(String.format("+I[3, 3, %s]", pt));
        }
        assertThat(result3).containsExactlyElementsOf(expected3);

        // second cluster
        runAction(Collections.emptyList());
        checkSnapshot(table);
        splits = readBuilder.newScan().plan().splits();
        List<String> result4 = getResult(readBuilder.newRead(), splits, readBuilder.readType());
        List<String> expected4 = new ArrayList<>();
        // for partition-0: only file in level-0 will be picked for clustering, outputLevel is 4
        expected4.add("+I[0, 0, 0]");
        expected4.add("+I[0, 1, 0]");
        expected4.add("+I[1, 0, 0]");
        expected4.add("+I[1, 1, 0]");
        expected4.add("+I[0, 2, 0]");
        expected4.add("+I[1, 2, 0]");
        expected4.add("+I[2, 0, 0]");
        expected4.add("+I[2, 1, 0]");
        expected4.add("+I[2, 2, 0]");
        expected4.add("+I[0, 3, 0]");
        expected4.add("+I[1, 3, 0]");
        expected4.add("+I[3, 0, 0]");
        expected4.add("+I[3, 1, 0]");
        expected4.add("+I[2, 3, 0]");
        expected4.add("+I[3, 2, 0]");
        expected4.add("+I[3, 3, 0]");
        // for partition-1:all files will be picked for clustering, outputLevel is 5
        expected4.add("+I[0, 0, 1]");
        expected4.add("+I[0, 1, 1]");
        expected4.add("+I[1, 0, 1]");
        expected4.add("+I[1, 1, 1]");
        expected4.add("+I[0, 2, 1]");
        expected4.add("+I[0, 3, 1]");
        expected4.add("+I[1, 2, 1]");
        expected4.add("+I[1, 3, 1]");
        expected4.add("+I[2, 0, 1]");
        expected4.add("+I[2, 1, 1]");
        expected4.add("+I[3, 0, 1]");
        expected4.add("+I[3, 1, 1]");
        expected4.add("+I[2, 2, 1]");
        expected4.add("+I[2, 3, 1]");
        expected4.add("+I[3, 2, 1]");
        expected4.add("+I[3, 3, 1]");
        assertThat(splits.size()).isEqualTo(2);
        assertThat(((DataSplit) splits.get(0)).dataFiles().size()).isEqualTo(2);
        assertThat(((DataSplit) splits.get(1)).dataFiles().size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(0).level()).isEqualTo(5);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(1).level()).isEqualTo(4);
        assertThat(((DataSplit) splits.get(1)).dataFiles().get(0).level()).isEqualTo(5);
        assertThat(result4).containsExactlyElementsOf(expected4);
    }

    @Test
    public void testClusterSpecifyPartition() throws Exception {
        FileStoreTable table = createTable("pt");

        BinaryString randomStr = BinaryString.fromString(randomString(150));
        List<CommitMessage> messages = new ArrayList<>();

        // first write
        List<String> expected1 = new ArrayList<>();
        for (int pt = 0; pt < 2; pt++) {
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < 3; j++) {
                    messages.addAll(write(GenericRow.of(i, j, (pt == 0) ? randomStr : null, pt)));
                    expected1.add(String.format("+I[%s, %s, %s]", i, j, pt));
                }
            }
        }
        commit(messages);
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {0, 1, 3});
        List<String> result1 =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        readBuilder.readType());
        assertThat(result1).containsExactlyElementsOf(expected1);

        runAction(Lists.newArrayList("--partition", "pt=0", "--compact_strategy", "full"));
        checkSnapshot(table);
        List<Split> splits = readBuilder.newScan().plan().splits();
        assertThat(splits.size()).isEqualTo(2);
        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            if (dataSplit.partition().getInt(0) == 0) {
                assertThat(dataSplit.dataFiles().size()).isEqualTo(1);
                assertThat(dataSplit.dataFiles().get(0).level()).isEqualTo(5);
            } else {
                assertThat(dataSplit.dataFiles().size()).isGreaterThan(1);
                assertThat(dataSplit.dataFiles().get(0).level()).isEqualTo(0);
            }
        }
    }

    @Test
    public void testClusterHistoryPartition() throws Exception {
        Map<String, String> options = commonOptions();
        options.put(CoreOptions.CLUSTERING_HISTORY_PARTITION_IDLE_TIME.key(), "3s");
        FileStoreTable table = createTable("pt", options);

        BinaryString randomStr = BinaryString.fromString(randomString(150));
        List<CommitMessage> messages = new ArrayList<>();

        // first write
        List<String> expected1 = new ArrayList<>();
        for (int pt = 0; pt < 4; pt++) {
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < 3; j++) {
                    messages.addAll(write(GenericRow.of(i, j, randomStr, pt)));
                    expected1.add(String.format("+I[%s, %s, %s]", i, j, pt));
                }
            }
        }
        commit(messages);
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {0, 1, 3});
        List<String> result1 =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        readBuilder.readType());
        assertThat(result1).containsExactlyElementsOf(expected1);

        // first cluster, files in four partitions will be in top level
        runAction(Collections.emptyList());
        checkSnapshot(table);
        List<Split> splits = readBuilder.newScan().plan().splits();
        assertThat(splits.size()).isEqualTo(4);
        assertThat(((DataSplit) splits.get(0)).dataFiles().size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(0).level()).isEqualTo(5);
        List<String> result2 = getResult(readBuilder.newRead(), splits, readBuilder.readType());
        List<String> expected2 = new ArrayList<>();
        for (int pt = 0; pt < 4; pt++) {
            expected2.add(String.format("+I[0, 0, %s]", pt));
            expected2.add(String.format("+I[0, 1, %s]", pt));
            expected2.add(String.format("+I[1, 0, %s]", pt));
            expected2.add(String.format("+I[1, 1, %s]", pt));
            expected2.add(String.format("+I[0, 2, %s]", pt));
            expected2.add(String.format("+I[1, 2, %s]", pt));
            expected2.add(String.format("+I[2, 0, %s]", pt));
            expected2.add(String.format("+I[2, 1, %s]", pt));
            expected2.add(String.format("+I[2, 2, %s]", pt));
        }
        assertThat(result2).containsExactlyElementsOf(expected2);

        // second write
        messages.clear();
        for (int pt = 0; pt < 4; pt++) {
            messages.addAll(
                    write(
                            GenericRow.of(0, 3, null, pt),
                            GenericRow.of(1, 3, null, pt),
                            GenericRow.of(2, 3, null, pt)));
            messages.addAll(
                    write(
                            GenericRow.of(3, 0, null, pt),
                            GenericRow.of(3, 1, null, pt),
                            GenericRow.of(3, 2, null, pt),
                            GenericRow.of(3, 3, null, pt)));
            // pt-0, pt-1 will be history partition
            if (pt == 1) {
                Thread.sleep(3000);
            }
        }
        commit(messages);

        List<String> result3 =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        readBuilder.readType());
        List<String> expected3 = new ArrayList<>();
        for (int pt = 0; pt < 4; pt++) {
            expected3.addAll(expected2.subList(9 * pt, 9 * pt + 9));
            expected3.add(String.format("+I[0, 3, %s]", pt));
            expected3.add(String.format("+I[1, 3, %s]", pt));
            expected3.add(String.format("+I[2, 3, %s]", pt));
            expected3.add(String.format("+I[3, 0, %s]", pt));
            expected3.add(String.format("+I[3, 1, %s]", pt));
            expected3.add(String.format("+I[3, 2, %s]", pt));
            expected3.add(String.format("+I[3, 3, %s]", pt));
        }
        assertThat(result3).containsExactlyElementsOf(expected3);

        // second cluster
        runAction(Lists.newArrayList("--partition", "pt=3"));
        checkSnapshot(table);
        splits = readBuilder.newScan().plan().splits();
        List<String> result4 = getResult(readBuilder.newRead(), splits, readBuilder.readType());
        List<String> expected4 = new ArrayList<>();
        assertThat(splits.size()).isEqualTo(4);
        // for pt-0 and pt-1: history partition, full clustering, all files will be
        // picked for clustering, outputLevel is 5.
        for (int pt = 0; pt <= 1; pt++) {
            expected4.add(String.format("+I[0, 0, %s]", pt));
            expected4.add(String.format("+I[0, 1, %s]", pt));
            expected4.add(String.format("+I[1, 0, %s]", pt));
            expected4.add(String.format("+I[1, 1, %s]", pt));
            expected4.add(String.format("+I[0, 2, %s]", pt));
            expected4.add(String.format("+I[0, 3, %s]", pt));
            expected4.add(String.format("+I[1, 2, %s]", pt));
            expected4.add(String.format("+I[1, 3, %s]", pt));
            expected4.add(String.format("+I[2, 0, %s]", pt));
            expected4.add(String.format("+I[2, 1, %s]", pt));
            expected4.add(String.format("+I[3, 0, %s]", pt));
            expected4.add(String.format("+I[3, 1, %s]", pt));
            expected4.add(String.format("+I[2, 2, %s]", pt));
            expected4.add(String.format("+I[2, 3, %s]", pt));
            expected4.add(String.format("+I[3, 2, %s]", pt));
            expected4.add(String.format("+I[3, 3, %s]", pt));
            // the table has enabled 'scan.plan-sort-partition', so the splits has been sorted by
            // partition
            assertThat(((DataSplit) splits.get(pt)).dataFiles().size()).isEqualTo(1);
            assertThat(((DataSplit) splits.get(pt)).dataFiles().get(0).level()).isEqualTo(5);
        }
        // for pt-2, non history partition, nor specified partition, nothing happened
        expected4.addAll(expected3.subList(32, 48));
        assertThat(((DataSplit) splits.get(2)).dataFiles().size()).isEqualTo(3);
        // for pt-3: minor clustering, only file in level-0 will be picked for clustering,
        // outputLevel is 4
        expected4.add("+I[0, 0, 3]");
        expected4.add("+I[0, 1, 3]");
        expected4.add("+I[1, 0, 3]");
        expected4.add("+I[1, 1, 3]");
        expected4.add("+I[0, 2, 3]");
        expected4.add("+I[1, 2, 3]");
        expected4.add("+I[2, 0, 3]");
        expected4.add("+I[2, 1, 3]");
        expected4.add("+I[2, 2, 3]");
        expected4.add("+I[0, 3, 3]");
        expected4.add("+I[1, 3, 3]");
        expected4.add("+I[3, 0, 3]");
        expected4.add("+I[3, 1, 3]");
        expected4.add("+I[2, 3, 3]");
        expected4.add("+I[3, 2, 3]");
        expected4.add("+I[3, 3, 3]");
        assertThat(((DataSplit) splits.get(3)).dataFiles().size()).isEqualTo(2);
        assertThat(
                        ((DataSplit) splits.get(3))
                                .dataFiles().stream()
                                        .map(DataFileMeta::level)
                                        .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(4, 5);

        assertThat(result4).containsExactlyElementsOf(expected4);
    }

    @Test
    public void testClusterOnEmptyData() throws Exception {
        createTable("pt");
        assertThatCode(() -> runAction(Collections.emptyList())).doesNotThrowAnyException();
    }

    @Test
    public void testMultiParallelism() throws Exception {
        Map<String, String> options = commonOptions();
        options.put("scan.parallelism", "2");
        FileStoreTable table = createTable(null, options);

        BinaryString randomStr = BinaryString.fromString(randomString(150));
        List<CommitMessage> messages = new ArrayList<>();

        // first write
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                messages.addAll(write(GenericRow.of(i, j, randomStr, 0)));
            }
        }
        commit(messages);
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

        runAction(Lists.newArrayList("--table_conf", "scan.parallelism=2"));
        checkSnapshot(table);
        List<Split> splits = readBuilder.newScan().plan().splits();
        assertThat(splits.size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().size()).isGreaterThanOrEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(0).level()).isEqualTo(5);
    }

    @Test
    public void testClusterWithDeletionVector() throws Exception {
        Map<String, String> dynamicOptions = commonOptions();
        dynamicOptions.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        FileStoreTable table = createTable(null, dynamicOptions);

        BinaryString randomStr = BinaryString.fromString(randomString(150));
        List<CommitMessage> messages = new ArrayList<>();
        // first write
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                messages.addAll(write(GenericRow.of(i, j, randomStr, 0)));
            }
        }
        commit(messages);
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(new int[] {0, 1});

        // first cluster
        runAction(Collections.emptyList());

        // second write
        messages.clear();
        messages.addAll(
                write(
                        GenericRow.of(0, 3, null, 0),
                        GenericRow.of(1, 3, null, 0),
                        GenericRow.of(2, 3, null, 0)));
        messages.addAll(
                write(
                        GenericRow.of(3, 0, null, 0),
                        GenericRow.of(3, 1, null, 0),
                        GenericRow.of(3, 2, null, 0),
                        GenericRow.of(3, 3, null, 0)));
        commit(messages);

        // write deletion vector for the table
        AppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        table.store().newIndexFileHandler(),
                        table.latestSnapshot().get(),
                        BinaryRow.EMPTY_ROW);
        List<DataFileMeta> files =
                readBuilder.newScan().plan().splits().stream()
                        .map(s -> ((DataSplit) s).dataFiles())
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        // delete (0,0) and (0,3)
        for (DataFileMeta file : files) {
            if (file.rowCount() == 9 || file.rowCount() == 3) {
                BitmapDeletionVector dv = new BitmapDeletionVector();
                dv.delete(0);
                maintainer.notifyNewDeletionVector(file.fileName(), dv);
            }
        }
        commit(produceDvIndexMessages(table, maintainer));
        List<String> result1 =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        readBuilder.readType());
        List<String> expected1 =
                Lists.newArrayList(
                        "+I[0, 1]",
                        "+I[1, 0]",
                        "+I[1, 1]",
                        "+I[0, 2]",
                        "+I[1, 2]",
                        "+I[2, 0]",
                        "+I[2, 1]",
                        "+I[2, 2]",
                        "+I[1, 3]",
                        "+I[2, 3]",
                        "+I[3, 0]",
                        "+I[3, 1]",
                        "+I[3, 2]",
                        "+I[3, 3]");
        assertThat(result1).containsExactlyElementsOf(expected1);

        // second cluster
        runAction(Collections.emptyList());
        checkSnapshot(table);
        List<Split> splits = readBuilder.newScan().plan().splits();
        List<String> result2 =
                getResult(
                        readBuilder.newRead(),
                        readBuilder.newScan().plan().splits(),
                        readBuilder.readType());
        assertThat(result2.size()).isEqualTo(expected1.size());
        assertThat(splits.size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().size()).isEqualTo(2);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(0).level()).isEqualTo(5);
        // dv index for level-5 file should be retained
        assertThat(splits.get(0).deletionFiles().get().get(0)).isNotNull();
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(1).level()).isEqualTo(4);
        assertThat((splits.get(0).deletionFiles().get().get(1))).isNull();

        // full cluster
        runAction(Lists.newArrayList("--compact_strategy", "full"));
        checkSnapshot(table);
        splits = readBuilder.newScan().plan().splits();
        assertThat(splits.size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().size()).isEqualTo(1);
        assertThat(((DataSplit) splits.get(0)).dataFiles().get(0).level()).isEqualTo(5);
        assertThat(splits.get(0).deletionFiles().get().get(0)).isNull();
    }

    protected FileStoreTable createTable(String partitionKeys) throws Exception {
        return createTable(partitionKeys, commonOptions());
    }

    protected FileStoreTable createTable(String partitionKeys, Map<String, String> options)
            throws Exception {
        catalog.createDatabase(database, true);
        catalog.createTable(identifier(), buildSchema(partitionKeys, options), true);
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

    private static Schema buildSchema(String partitionKeys, Map<String, String> options) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("a", DataTypes.INT());
        schemaBuilder.column("b", DataTypes.INT());
        schemaBuilder.column("c", DataTypes.STRING());
        schemaBuilder.column("pt", DataTypes.INT());
        for (String key : options.keySet()) {
            schemaBuilder.option(key, options.get(key));
        }
        if (!StringUtils.isNullOrWhitespaceOnly(partitionKeys)) {
            schemaBuilder.partitionKeys(partitionKeys);
        }
        return schemaBuilder.build();
    }

    private static Map<String, String> commonOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "-1");
        options.put("num-levels", "6");
        options.put("num-sorted-run.compaction-trigger", "2");
        options.put("scan.plan-sort-partition", "true");
        options.put("clustering.columns", "a,b");
        options.put("clustering.strategy", "zorder");
        options.put("clustering.incremental", "true");
        options.put("scan.parallelism", "1");
        options.put("sink.parallelism", "1");
        return options;
    }

    private static String randomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }

    private void checkSnapshot(FileStoreTable table) {
        assertThat(table.latestSnapshot().get().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);
    }

    private List<CommitMessage> produceDvIndexMessages(
            FileStoreTable table, AppendDeleteFileMaintainer maintainer) {
        List<IndexFileMeta> newIndexFiles = new ArrayList<>();
        List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
        List<IndexManifestEntry> indexEntries = maintainer.persist();
        for (IndexManifestEntry entry : indexEntries) {
            if (entry.kind() == FileKind.ADD) {
                newIndexFiles.add(entry.indexFile());
            } else {
                deletedIndexFiles.add(entry.indexFile());
            }
        }
        return Collections.singletonList(
                new CommitMessageImpl(
                        maintainer.getPartition(),
                        0,
                        table.coreOptions().bucket(),
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                newIndexFiles,
                                deletedIndexFiles)));
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
        action.withStreamExecutionEnvironment(env);
        action.run();
    }
}
