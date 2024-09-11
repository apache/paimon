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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;

/** IT cases for {@link RollbackToAction}. */
public class RollbackToActionITCase extends ActionITCaseBase {

    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE = RowType.of(FIELD_TYPES, new String[] {"k", "v"});

    @BeforeEach
    public void setUp() {
        init(warehouse);
    }

    @ParameterizedTest
    @ValueSource(strings = {"action", "procedure_named", "procedure_indexed"})
    public void rollbackToSnapshotTest(String invoker) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.emptyMap());
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        writeData(rowData(1L, BinaryString.fromString("Hi")));
        writeData(rowData(2L, BinaryString.fromString("Hello")));
        writeData(rowData(2L, BinaryString.fromString("World")));
        writeData(rowData(2L, BinaryString.fromString("Flink")));

        switch (invoker) {
            case "action":
                createAction(
                                RollbackToAction.class,
                                "rollback_to",
                                "--warehouse",
                                warehouse,
                                "--database",
                                database,
                                "--table",
                                tableName,
                                "--version",
                                "2")
                        .run();
                break;
            case "procedure_indexed":
                callProcedure(
                        String.format(
                                "CALL sys.rollback_to('%s.%s', '', cast(2 as bigint))",
                                database, tableName));
                break;
            case "procedure_named":
                callProcedure(
                        String.format(
                                "CALL sys.rollback_to(`table` => '%s.%s', snapshot_id => cast(2 as bigint))",
                                database, tableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }

        testBatchRead(
                "SELECT * FROM `" + tableName + "`",
                Arrays.asList(Row.of(1L, "Hi"), Row.of(2L, "Hello")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"action", "procedure_named", "procedure_indexed"})
    public void rollbackToTagTest(String invoker) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.emptyMap());
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        writeData(rowData(1L, BinaryString.fromString("Hi")));
        writeData(rowData(2L, BinaryString.fromString("Apache")));
        writeData(rowData(2L, BinaryString.fromString("Paimon")));

        table.createTag("tag1", 1);
        table.createTag("tag2", 2);
        table.createTag("tag3", 3);

        switch (invoker) {
            case "action":
                createAction(
                                RollbackToAction.class,
                                "rollback_to",
                                "--warehouse",
                                warehouse,
                                "--database",
                                database,
                                "--table",
                                tableName,
                                "--version",
                                "tag2")
                        .run();
                break;
            case "procedure_indexed":
                callProcedure(
                        String.format(
                                "CALL sys.rollback_to('%s.%s', 'tag2')", database, tableName));
                break;
            case "procedure_named":
                callProcedure(
                        String.format(
                                "CALL sys.rollback_to(`table` => '%s.%s', tag => 'tag2')",
                                database, tableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }

        testBatchRead(
                "SELECT * FROM `" + tableName + "`",
                Arrays.asList(Row.of(1L, "Hi"), Row.of(2L, "Apache")));
    }
}
