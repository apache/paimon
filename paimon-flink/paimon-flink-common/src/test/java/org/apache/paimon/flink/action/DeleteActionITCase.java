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
import org.apache.paimon.WriteMode;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildSimpleQuery;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertInto;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testStreamingRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.validateStreamingReadResult;
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

        DeleteAction action =
                new DeleteAction(warehouse, database, tableName, "k = 1", Collections.emptyMap());

        BlockingIterator<Row, Row> iterator =
                testStreamingRead(buildSimpleQuery(tableName), initialRecords);

        action.run();

        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(2);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

        validateStreamingReadResult(iterator, expected);
        iterator.close();
    }

    @Test
    public void testWorkWithPartialUpdateTable() throws Exception {
        createFileStoreTable(
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"k", "a", "b"}),
                Collections.emptyList(),
                Collections.singletonList("k"),
                new HashMap<String, String>() {
                    {
                        put(
                                CoreOptions.MERGE_ENGINE.key(),
                                CoreOptions.MergeEngine.PARTIAL_UPDATE.toString());
                        put(CoreOptions.PARTIAL_UPDATE_IGNORE_DELETE.key(), "true");
                        put(
                                CoreOptions.CHANGELOG_PRODUCER.key(),
                                ThreadLocalRandom.current().nextBoolean()
                                        ? CoreOptions.ChangelogProducer.LOOKUP.toString()
                                        : CoreOptions.ChangelogProducer.FULL_COMPACTION.toString());
                    }
                });

        DeleteAction action =
                new DeleteAction(warehouse, database, tableName, "k < 3", Collections.emptyMap());

        insertInto(
                tableName, "(1, 'Say', 'A'), (2, 'Hi', 'B'), (3, 'To', 'C'), (4, 'Paimon', 'D')");

        BlockingIterator<Row, Row> streamItr =
                testStreamingRead(
                        buildSimpleQuery(tableName),
                        Arrays.asList(
                                changelogRow("+I", 1, "Say", "A"),
                                changelogRow("+I", 2, "Hi", "B"),
                                changelogRow("+I", 3, "To", "C"),
                                changelogRow("+I", 4, "Paimon", "D")));

        action.run();

        // test delete records hasn't been thrown
        validateStreamingReadResult(
                streamItr,
                Arrays.asList(changelogRow("-D", 1, "Say", "A"), changelogRow("-D", 2, "Hi", "B")));

        // test partial update still works after action
        insertInto(
                tableName, "(4, CAST (NULL AS STRING), '$')", "(4, 'Test', CAST (NULL AS STRING))");

        validateStreamingReadResult(
                streamItr,
                Arrays.asList(
                        changelogRow("-U", 4, "Paimon", "D"), changelogRow("+U", 4, "Test", "$")));
        streamItr.close();

        testBatchRead(
                buildSimpleQuery(tableName),
                Arrays.asList(
                        changelogRow("+I", 3, "To", "C"), changelogRow("+I", 4, "Test", "$")));
    }

    private void prepareTable(boolean hasPk) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_MODE.key(), WriteMode.CHANGE_LOG.toString());
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Collections.emptyList(),
                        hasPk ? Collections.singletonList("k") : Collections.emptyList(),
                        options);
        snapshotManager = table.snapshotManager();
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
