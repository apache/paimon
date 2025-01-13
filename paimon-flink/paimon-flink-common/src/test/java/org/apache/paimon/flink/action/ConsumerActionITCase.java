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

import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.table.api.TableException;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testStreamingRead;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** IT cases for consumer management actions. */
public class ConsumerActionITCase extends ActionITCaseBase {

    @ParameterizedTest
    @Timeout(60)
    @ValueSource(strings = {"action", "procedure_indexed", "procedure_named"})
    public void testResetConsumer(String invoker) throws Exception {
        init(warehouse);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"pk1", "col1"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("pk1"),
                        Collections.emptyList(),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        // 3 snapshots
        writeData(rowData(1L, BinaryString.fromString("Hi")));
        writeData(rowData(2L, BinaryString.fromString("Hello")));
        writeData(rowData(3L, BinaryString.fromString("Paimon")));

        // use consumer streaming read table
        BlockingIterator<Row, Row> iterator =
                testStreamingRead(
                        "SELECT * FROM `"
                                + tableName
                                + "` /*+ OPTIONS('consumer-id'='myid','consumer.expiration-time'='3h') */",
                        Arrays.asList(
                                changelogRow("+I", 1L, "Hi"),
                                changelogRow("+I", 2L, "Hello"),
                                changelogRow("+I", 3L, "Paimon")));

        ConsumerManager consumerManager = new ConsumerManager(table.fileIO(), table.location());
        while (!consumerManager.consumer("myid").isPresent()) {
            Thread.sleep(1000);
        }
        iterator.close();

        Optional<Consumer> consumer1 = consumerManager.consumer("myid");
        assertThat(consumer1).isPresent();
        assertThat(consumer1.get().nextSnapshot()).isEqualTo(4);

        List<String> args =
                Arrays.asList(
                        "reset_consumer",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--consumer_id",
                        "myid",
                        "--next_snapshot",
                        "1");
        // reset consumer
        switch (invoker) {
            case "action":
                createAction(ResetConsumerAction.class, args).run();
                break;
            case "procedure_indexed":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer('%s.%s', 'myid', 1)",
                                database, tableName));
                break;
            case "procedure_named":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer(`table` => '%s.%s', consumer_id => 'myid', next_snapshot_id => cast(1 as bigint))",
                                database, tableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }
        Optional<Consumer> consumer2 = consumerManager.consumer("myid");
        assertThat(consumer2).isPresent();
        assertThat(consumer2.get().nextSnapshot()).isEqualTo(1);

        // delete consumer
        switch (invoker) {
            case "action":
                createAction(ResetConsumerAction.class, args.subList(0, 9)).run();
                break;
            case "procedure_indexed":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer('%s.%s', 'myid')", database, tableName));
                break;
            case "procedure_named":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer(`table` => '%s.%s', consumer_id => 'myid')",
                                database, tableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }
        Optional<Consumer> consumer3 = consumerManager.consumer("myid");
        assertThat(consumer3).isNotPresent();

        // reset consumer to a not exist snapshot id
        List<String> args1 =
                Arrays.asList(
                        "reset_consumer",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--consumer_id",
                        "myid",
                        "--next_snapshot",
                        "10");
        switch (invoker) {
            case "action":
                assertThrows(
                        RuntimeException.class,
                        () -> createAction(ResetConsumerAction.class, args1).run());
                break;
            case "procedure_indexed":
                assertThrows(
                        TableException.class,
                        () ->
                                executeSQL(
                                        String.format(
                                                "CALL sys.reset_consumer('%s.%s', 'myid', 10)",
                                                database, tableName)));
                break;
            case "procedure_named":
                assertThrows(
                        TableException.class,
                        () ->
                                executeSQL(
                                        String.format(
                                                "CALL sys.reset_consumer(`table` => '%s.%s', consumer_id => 'myid', next_snapshot_id => cast(10 as bigint))",
                                                database, tableName)));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }
    }

    @ParameterizedTest
    @Timeout(60)
    @ValueSource(strings = {"action", "procedure_indexed", "procedure_named"})
    public void testResetBranchConsumer(String invoker) throws Exception {
        init(warehouse);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"pk1", "col1"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("pk1"),
                        Collections.emptyList(),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        // 3 snapshots
        writeData(rowData(1L, BinaryString.fromString("Hi")));
        writeData(rowData(2L, BinaryString.fromString("Hello")));
        writeData(rowData(3L, BinaryString.fromString("Paimon")));

        table.createTag("tag", 3);
        String branchName = "b1";
        table.createBranch("b1", "tag");
        String branchTableName = tableName + "$branch_b1";

        // use consumer streaming read table
        BlockingIterator<Row, Row> iterator =
                testStreamingRead(
                        "SELECT * FROM `"
                                + branchTableName
                                + "` /*+ OPTIONS('consumer-id'='myid','consumer.expiration-time'='3h') */",
                        Arrays.asList(
                                changelogRow("+I", 1L, "Hi"),
                                changelogRow("+I", 2L, "Hello"),
                                changelogRow("+I", 3L, "Paimon")));

        ConsumerManager consumerManager =
                new ConsumerManager(table.fileIO(), table.location(), branchName);
        while (!consumerManager.consumer("myid").isPresent()) {
            Thread.sleep(1000);
        }
        iterator.close();

        Optional<Consumer> consumer1 = consumerManager.consumer("myid");
        assertThat(consumer1).isPresent();
        assertThat(consumer1.get().nextSnapshot()).isEqualTo(4);

        List<String> args =
                Arrays.asList(
                        "reset_consumer",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        branchTableName,
                        "--consumer_id",
                        "myid",
                        "--next_snapshot",
                        "3");
        // reset consumer
        switch (invoker) {
            case "action":
                createAction(ResetConsumerAction.class, args).run();
                break;
            case "procedure_indexed":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer('%s.%s', 'myid', 3)",
                                database, branchTableName));
                break;
            case "procedure_named":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer(`table` => '%s.%s', consumer_id => 'myid', next_snapshot_id => cast(3 as bigint))",
                                database, branchTableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }
        Optional<Consumer> consumer2 = consumerManager.consumer("myid");
        assertThat(consumer2).isPresent();
        assertThat(consumer2.get().nextSnapshot()).isEqualTo(3);

        // delete consumer
        switch (invoker) {
            case "action":
                createAction(ResetConsumerAction.class, args.subList(0, 9)).run();
                break;
            case "procedure_indexed":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer('%s.%s', 'myid')",
                                database, branchTableName));
                break;
            case "procedure_named":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer(`table` => '%s.%s', consumer_id => 'myid')",
                                database, branchTableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }
        Optional<Consumer> consumer3 = consumerManager.consumer("myid");
        assertThat(consumer3).isNotPresent();
    }
}
