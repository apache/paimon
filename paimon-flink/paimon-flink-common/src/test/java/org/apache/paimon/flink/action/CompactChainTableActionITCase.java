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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildDdl;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CompactChainTableAction}. */
public class CompactChainTableActionITCase extends CompactActionITCaseBase {

    private static final List<String> FIELDS_SPEC =
            Arrays.asList("t1 INT", "t2 INT", "t3 INT", "dt STRING");
    private static final List<String> PRIMARY_KEYS = Arrays.asList("dt", "t1");
    private static final List<String> PARTITION_KEYS = Collections.singletonList("dt");

    @BeforeEach
    public void setUp() throws Exception {
        init(warehouse);
        prepareChainTable(Collections.emptyMap());
    }

    private void prepareChainTable(Map<String, String> extraOptions) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "1");
        options.put("bucket-key", "t1");
        options.put("sequence.field", "t2");
        options.put("merge-engine", "deduplicate");
        options.put("chain-table.enabled", "true");
        options.put("partition.timestamp-pattern", "$dt");
        options.put("partition.timestamp-formatter", "yyyyMMdd");
        options.putAll(extraOptions);

        String ddl = buildDdl(tableName, FIELDS_SPEC, PRIMARY_KEYS, PARTITION_KEYS, options);
        bEnv.executeSql(ddl).await();

        // Create snapshot / delta branches and configure fallback branches.
        bEnv.executeSql(
                        String.format(
                                "CALL sys.create_branch('%s.%s', 'snapshot')", database, tableName))
                .await();
        bEnv.executeSql(
                        String.format(
                                "CALL sys.create_branch('%s.%s', 'delta')", database, tableName))
                .await();
        bEnv.executeSql(
                        String.format(
                                "ALTER TABLE `%s`.`%s` SET ('scan.fallback-snapshot-branch' = 'snapshot', 'scan.fallback-delta-branch' = 'delta')",
                                database, tableName))
                .await();
        bEnv.executeSql(
                        String.format(
                                "ALTER TABLE `%s`.`%s$branch_snapshot` SET ('scan.fallback-snapshot-branch' = 'snapshot', 'scan.fallback-delta-branch' = 'delta')",
                                database, tableName))
                .await();
        bEnv.executeSql(
                        String.format(
                                "ALTER TABLE `%s`.`%s$branch_delta` SET ('scan.fallback-snapshot-branch' = 'snapshot', 'scan.fallback-delta-branch' = 'delta')",
                                database, tableName))
                .await();
    }

    private void writeBranch(String branch, GenericRow... rows) throws Exception {
        FileStoreTable branchTable = getFileStoreTable(tableName).switchToBranch(branch);
        String commitUser = UUID.randomUUID().toString();
        try (org.apache.paimon.table.sink.StreamTableWrite write =
                        branchTable.newStreamWriteBuilder().withCommitUser(commitUser).newWrite();
                org.apache.paimon.table.sink.StreamTableCommit commit =
                        branchTable
                                .newStreamWriteBuilder()
                                .withCommitUser(commitUser)
                                .newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(0L, write.prepareCommit(true, 0L));
        }
    }

    private List<String> readSnapshot(Map<String, String> partitionFilter) throws Exception {
        FileStoreTable snapshotTable = getFileStoreTable(tableName).switchToBranch("snapshot");
        ReadBuilder readBuilder =
                snapshotTable.newReadBuilder().withPartitionFilter(partitionFilter);
        TableRead read = readBuilder.newRead();
        List<Split> splits = readBuilder.newScan().plan().splits();
        return getResult(read, splits, ROW_TYPE);
    }

    private void runAction(String partition, boolean overwrite) throws Exception {
        CompactChainTableAction action =
                createAction(
                        CompactChainTableAction.class,
                        "compact_chain_table",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--partition",
                        partition,
                        "--overwrite",
                        String.valueOf(overwrite));
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        action.withStreamExecutionEnvironment(env).run();
    }

    @Test
    public void testCompactChainTableBasic() throws Exception {
        // Compact empty partition
        runAction("dt=20260708", false);

        writeBranch(
                "snapshot",
                rowData(1, 1, 1, BinaryString.fromString("20260708")),
                rowData(2, 1, 1, BinaryString.fromString("20260708")));
        writeBranch("delta", rowData(3, 1, 1, BinaryString.fromString("20260709")));
        // Compact a partition that only exists in the delta branch.
        runAction("dt=20260709", false);
        // Compact empty partition
        runAction("dt=20260710", false);

        assertThat(readSnapshot(ImmutableMap.of("dt", "20260709")))
                .containsExactlyInAnyOrder(
                        "+I[1, 1, 1, 20260709]", "+I[2, 1, 1, 20260709]", "+I[3, 1, 1, 20260709]");
    }

    @Test
    public void testCompactChainTableOverwrite() throws Exception {
        writeBranch("snapshot", rowData(1, 1, 1, BinaryString.fromString("20260708")));
        writeBranch("snapshot", rowData(2, 1, 1, BinaryString.fromString("20260709")));
        writeBranch("delta", rowData(3, 1, 1, BinaryString.fromString("20260709")));

        // Without overwrite, the existing snapshot partition is skipped.
        runAction("dt=20260709", false);
        assertThat(readSnapshot(ImmutableMap.of("dt", "20260709")))
                .containsExactlyInAnyOrder("+I[2, 1, 1, 20260709]");

        // With overwrite, merge the snapshot anchor and the delta data.
        runAction("dt=20260709", true);
        assertThat(readSnapshot(ImmutableMap.of("dt", "20260709")))
                .containsExactlyInAnyOrder("+I[1, 1, 1, 20260709]", "+I[3, 1, 1, 20260709]");
    }
}
