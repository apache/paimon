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

package org.apache.flink.table.store.connector.action;

import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.DataTypes;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.buildSimpleQuery;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.init;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.testStreamingRead;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.validateStreamingReadResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** IT cases for {@link DeleteAction}. */
public class DeleteActionITCase extends ActionITCaseBase {
    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE = RowType.of(FIELD_TYPES, new String[] {"k", "v"});

    @BeforeEach
    public void setUp() {
        init(warehouse);
    }

    @ParameterizedTest(name = "hasPk-{0}")
    @MethodSource("data")
    public void testDeleteAction(boolean hasPk, List<Row> initialRecords, List<Row> expected)
            throws Exception {
        prepareTable(hasPk);

        DeleteAction action = new DeleteAction(warehouse, database, tableName, "k = 1");

        BlockingIterator<Row, Row> iterator =
                testStreamingRead(buildSimpleQuery(tableName), initialRecords);

        action.run();

        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(2);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

        validateStreamingReadResult(iterator, expected);
        iterator.close();
    }

    private void prepareTable(boolean hasPk) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Collections.emptyList(),
                        hasPk ? Collections.singletonList("k") : Collections.emptyList(),
                        new HashMap<>());
        snapshotManager = table.snapshotManager();
        write = table.newWrite(commitUser);
        commit = table.newCommit(commitUser);

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

    private static List<Arguments> data() {
        return Arrays.asList(
                arguments(
                        true,
                        Arrays.asList(
                                changelogRow("+I", 1L, "World"),
                                changelogRow("+I", 2L, "Store"),
                                changelogRow("+I", 3L, "Developer")),
                        Collections.singletonList(changelogRow("-D", 1L, "World"))),
                arguments(
                        false,
                        Arrays.asList(
                                changelogRow("+I", 1L, "Hi"),
                                changelogRow("+I", 1L, "Hello"),
                                changelogRow("+I", 1L, "World"),
                                changelogRow("+I", 2L, "Flink"),
                                changelogRow("+I", 2L, "Table"),
                                changelogRow("+I", 2L, "Store"),
                                changelogRow("+I", 3L, "Developer")),
                        Arrays.asList(
                                changelogRow("-D", 1L, "Hi"),
                                changelogRow("-D", 1L, "Hello"),
                                changelogRow("-D", 1L, "World"))));
    }
}
