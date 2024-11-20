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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for compact strategy {@link CompactAction}. */
public class MinorCompactActionITCase extends CompactActionITCaseBase {

    @Test
    @Timeout(60)
    public void testBatchMinorCompactStrategy() throws Exception {
        FileStoreTable table =
                prepareTable(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        Collections.emptyList(),
                        Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")));

        writeData(
                rowData(2, 100, 15, BinaryString.fromString("20221208")),
                rowData(2, 100, 16, BinaryString.fromString("20221208")));

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        CompactAction action =
                createAction(
                        CompactAction.class,
                        "compact",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--compact_strategy",
                        "minor",
                        "--table_conf",
                        CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key() + "=3");
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        action.withStreamExecutionEnvironment(env).build();
        env.execute();

        // Due to the limitation of parameter 'num-sorted-run.compaction-trigger', so compact is not
        // performed.
        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        // Make par-15 has 3 datafile and par-16 has 2 datafile, so par-16 will not be picked out to
        // compact.
        writeData(rowData(2, 100, 15, BinaryString.fromString("20221208")));

        env = streamExecutionEnvironmentBuilder().batchMode().build();
        action.withStreamExecutionEnvironment(env).build();
        env.execute();

        checkLatestSnapshot(table, 4, Snapshot.CommitKind.COMPACT);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits.size()).isEqualTo(2);
        for (DataSplit split : splits) {
            // Par-16 is not compacted.
            assertThat(split.dataFiles().size())
                    .isEqualTo(split.partition().getInt(1) == 16 ? 2 : 1);
        }
    }

    @Test
    @Timeout(60)
    public void testBatchFullCompactStrategy() throws Exception {
        FileStoreTable table =
                prepareTable(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        Collections.emptyList(),
                        Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")));

        writeData(
                rowData(2, 100, 15, BinaryString.fromString("20221208")),
                rowData(2, 100, 16, BinaryString.fromString("20221208")));

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        CompactAction action =
                createAction(
                        CompactAction.class,
                        "compact",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--compact_strategy",
                        "full",
                        "--table_conf",
                        CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key() + "=3");
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        action.withStreamExecutionEnvironment(env).build();
        env.execute();

        checkLatestSnapshot(table, 3, Snapshot.CommitKind.COMPACT);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits.size()).isEqualTo(2);
        for (DataSplit split : splits) {
            assertThat(split.dataFiles().size()).isEqualTo(1);
        }
    }

    @Test
    @Timeout(60)
    public void testStreamingFullCompactStrategy() throws Exception {
        prepareTable(
                Arrays.asList("dt", "hh"),
                Arrays.asList("dt", "hh", "k"),
                Collections.emptyList(),
                Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        CompactAction action =
                createAction(
                        CompactAction.class,
                        "compact",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--compact_strategy",
                        "full",
                        "--table_conf",
                        CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key() + "=3");
        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().streamingMode().build();
        Assertions.assertThatThrownBy(() -> action.withStreamExecutionEnvironment(env).build())
                .hasMessage(
                        "full compact strategy is only supported in batch mode. Please add -Dexecution.runtime-mode=BATCH.");
    }
}
