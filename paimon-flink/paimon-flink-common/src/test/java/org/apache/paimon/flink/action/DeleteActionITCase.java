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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BlockingIterator;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildSimpleQuery;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testStreamingRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.validateStreamingReadResult;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link DeleteAction}. */
public class DeleteActionITCase extends ActionITCaseBase {
    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE = RowType.of(FIELD_TYPES, new String[] {"k", "v"});

    @BeforeEach
    public void setUp() {
        init(warehouse);
    }

    @Test
    public void testDeleteAction() throws Exception {
        List<Row> initialRecords =
                Arrays.asList(
                        changelogRow("+I", 1L, "World"),
                        changelogRow("+I", 2L, "Store"),
                        changelogRow("+I", 3L, "Developer"));
        List<Row> expected = Collections.singletonList(changelogRow("-D", 1L, "World"));

        prepareTable();

        DeleteAction action =
                createAction(
                        DeleteAction.class,
                        "delete",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--where",
                        "k=1");

        BlockingIterator<Row, Row> iterator =
                testStreamingRead(buildSimpleQuery(tableName), initialRecords);

        action.run();

        SnapshotManager snapshotManager = getFileStoreTable(tableName).snapshotManager();
        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(2);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

        validateStreamingReadResult(iterator, expected);
        iterator.close();
    }

    private void prepareTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        options);
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = streamWriteBuilder.newWrite();
        commit = streamWriteBuilder.newCommit();

        // prepare data
        writeData(
                rowData(1L, BinaryString.fromString("Hi")),
                rowData(1L, BinaryString.fromString("Hello")),
                rowData(1L, BinaryString.fromString("World")),
                rowData(2L, BinaryString.fromString("Flink")),
                rowData(2L, BinaryString.fromString("Table")),
                rowData(2L, BinaryString.fromString("Store")),
                rowData(3L, BinaryString.fromString("Developer")));

        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());

        assertThat(snapshot.id()).isEqualTo(1);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
    }
}
