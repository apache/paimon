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
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link ExpirePartitionsAction}. */
public class ExpirePartitionsActionITCase extends ActionITCaseBase {

    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()};

    private static final RowType ROW_TYPE = RowType.of(FIELD_TYPES, new String[] {"k", "dt", "hm"});

    @BeforeEach
    public void setUp() {
        init(warehouse);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testExpirePartitionsAction(boolean forceStartFlinkJob) throws Exception {
        FileStoreTable table = prepareTable();
        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        List<String> actual = getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE);
        List<String> expected;
        expected = Arrays.asList("+I[1, 2024-01-01, 01:00]", "+I[2, 9999-09-20, 02:00]");

        assertThat(actual).isEqualTo(expected);

        createAction(
                        ExpirePartitionsAction.class,
                        "expire_partitions",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--expiration_time",
                        "1 d",
                        "--timestamp_formatter",
                        "yyyy-MM-dd",
                        "--force_start_flink_job",
                        Boolean.toString(forceStartFlinkJob))
                .run();
        SnapshotManager snapshotManager = getFileStoreTable(tableName).snapshotManager();
        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(3);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);

        plan = table.newReadBuilder().newScan().plan();
        actual = getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE);

        expected = Arrays.asList("+I[2, 9999-09-20, 02:00]");

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testExpirePartitionsActionWithTimePartition() throws Exception {
        FileStoreTable table = prepareTable();
        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        List<String> actual = getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE);
        List<String> expected;
        expected = Arrays.asList("+I[1, 2024-01-01, 01:00]", "+I[2, 9999-09-20, 02:00]");

        assertThat(actual).isEqualTo(expected);

        createAction(
                        ExpirePartitionsAction.class,
                        "expire_partitions",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--expiration_time",
                        "1 d",
                        "--timestamp_formatter",
                        "yyyy-MM-dd HH:mm",
                        "--timestamp_pattern",
                        "$dt $hm")
                .run();

        plan = table.newReadBuilder().newScan().plan();
        actual = getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE);

        expected = Arrays.asList("+I[2, 9999-09-20, 02:00]");

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testExpirePartitionsWithTableConf() throws Exception {
        FileStoreTable table = prepareTable();
        // prepare more data
        writeData(
                rowData(
                        BinaryString.fromString("3"),
                        BinaryString.fromString("2024-01-01"),
                        BinaryString.fromString("02:00")));
        writeData(
                rowData(
                        BinaryString.fromString("4"),
                        BinaryString.fromString("2024-01-02"),
                        BinaryString.fromString("01:00")));

        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        List<String> actual = getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE);
        List<String> expected;
        expected =
                Arrays.asList(
                        "+I[1, 2024-01-01, 01:00]",
                        "+I[2, 9999-09-20, 02:00]",
                        "+I[3, 2024-01-01, 02:00]",
                        "+I[4, 2024-01-02, 01:00]");

        assertThat(actual).isEqualTo(expected);

        // only expire one partition
        createAction(
                        ExpirePartitionsAction.class,
                        "expire_partitions",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--expiration_time",
                        "1 d",
                        "--timestamp_formatter",
                        "yyyy-MM-dd",
                        "--timestamp_pattern",
                        "$dt",
                        "--table_conf",
                        "partition.expiration-max-num=1",
                        "--table_conf",
                        "partition.expiration-batch-size=1")
                .run();

        plan = table.newReadBuilder().newScan().plan();
        actual = getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE);

        expected =
                Arrays.asList(
                        "+I[2, 9999-09-20, 02:00]",
                        "+I[3, 2024-01-01, 02:00]",
                        "+I[4, 2024-01-02, 01:00]");

        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({"true,,true", ",true,true", "true,false,false", ",,false"})
    public void testRetainLatestPartition(
            String tableOption, String actionOption, boolean retainLatest) throws Exception {
        Map<String, String> options = new HashMap<>();
        if (tableOption != null) {
            options.put("partition.expiration-retain-latest", tableOption);
        }
        // Explicit action arguments must still override the table's strategy and formatter.
        options.put("partition.expiration-strategy", "update-time");
        options.put("partition.timestamp-formatter", "yyyy-MM-dd");
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Collections.singletonList("dt"),
                        Arrays.asList("k", "dt"),
                        Collections.singletonList("k"),
                        options);
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();
        writeData(
                rowData(
                        BinaryString.fromString("1"),
                        BinaryString.fromString("20010101"),
                        BinaryString.fromString("01:00")));
        assertThat(getData(tableName)).hasSize(1);

        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "expire_partitions",
                                "--warehouse",
                                warehouse,
                                "--database",
                                database,
                                "--table",
                                tableName,
                                "--expiration_time",
                                "1 d",
                                "--timestamp_formatter",
                                "yyyyMMdd",
                                "--expire_strategy",
                                "values-time"));
        if (actionOption != null) {
            args.add("--table_conf");
            args.add("partition.expiration-retain-latest=" + actionOption);
        }
        ExpirePartitionsAction action = createAction(ExpirePartitionsAction.class, args);
        for (int i = 0; i < 2; i++) {
            action.executeLocally();
            assertThat(getData(tableName)).hasSize(retainLatest ? 1 : 0);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testInheritExpirationStrategy(boolean useTableConf) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("partition.expiration-strategy", useTableConf ? "values-time" : "update-time");
        prepareExpirationTable(options, Collections.singletonList("dt"));
        Map<String, String> tableConf =
                useTableConf
                        ? Collections.singletonMap("partition.expiration-strategy", "update-time")
                        : Collections.emptyMap();
        expireWithOptions(tableConf, null, null);
        assertThat(getData(tableName)).hasSize(1);
        expireWithOptions(tableConf, "values-time", null);
        assertThat(getData(tableName)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testInheritTimestampPattern(boolean useTableConf) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("partition.timestamp-pattern", useTableConf ? "$k" : "$dt");
        prepareExpirationTable(options, Arrays.asList("k", "dt"));
        Map<String, String> tableConf =
                useTableConf
                        ? Collections.singletonMap("partition.timestamp-pattern", "$dt")
                        : Collections.emptyMap();
        expireWithOptions(tableConf, null, "$k");
        assertThat(getData(tableName)).hasSize(1);
        expireWithOptions(tableConf, null, null);
        assertThat(getData(tableName)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " "})
    public void testExplicitBlankStrategyIsRejected(String strategy) throws Exception {
        prepareExpirationTable(Collections.emptyMap(), Collections.singletonList("dt"));
        assertThatThrownBy(() -> expireWithOptions(Collections.emptyMap(), strategy, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown partitionExpireStrategy");
        assertThat(getData(tableName)).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " "})
    public void testExplicitBlankPatternDoesNotInherit(String pattern) throws Exception {
        prepareExpirationTable(
                Collections.singletonMap("partition.timestamp-pattern", "$dt"),
                Collections.singletonList("dt"));
        expireWithOptions(Collections.emptyMap(), null, pattern);
        assertThat(getData(tableName)).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " "})
    public void testExplicitBlankFormatterDoesNotInherit(String formatter) throws Exception {
        prepareExpirationTable(
                Collections.singletonMap("partition.timestamp-formatter", "yyyy-MM-dd"),
                Collections.singletonList("dt"));
        new ExpirePartitionsAction(
                        database,
                        tableName,
                        Collections.singletonMap("warehouse", warehouse),
                        Collections.emptyMap(),
                        "7 d",
                        formatter,
                        null,
                        null)
                .executeLocally();
        assertThat(getData(tableName)).hasSize(1);
    }

    private void prepareExpirationTable(Map<String, String> options, List<String> partitions)
            throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        partitions,
                        Arrays.asList("k", "dt", "hm"),
                        Collections.singletonList("hm"),
                        options);
        StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = builder.newWrite();
        commit = builder.newCommit();
        writeData(
                rowData(
                        BinaryString.fromString("9999-01-01"),
                        BinaryString.fromString("2000-01-01"),
                        BinaryString.fromString("00:00")));
        assertThat(getData(tableName)).hasSize(1);
    }

    private void expireWithOptions(Map<String, String> tableConf, String strategy, String pattern)
            throws Exception {
        new ExpirePartitionsAction(
                        database,
                        tableName,
                        Collections.singletonMap("warehouse", warehouse),
                        tableConf,
                        "7 d",
                        "yyyy-MM-dd",
                        pattern,
                        strategy)
                .executeLocally();
    }

    private FileStoreTable prepareTable() throws Exception {
        init(warehouse);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"k", "dt", "hm"});
        String[] pk = {"k", "dt", "hm"};
        String[] partitions = {"dt", "hm"};
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        new ArrayList<>(Arrays.asList(partitions)),
                        new ArrayList<>(Arrays.asList(pk)),
                        Collections.singletonList("k"),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        // 3 snapshots
        writeData(
                rowData(
                        BinaryString.fromString("1"),
                        BinaryString.fromString("2024-01-01"),
                        BinaryString.fromString("01:00")));
        writeData(
                rowData(
                        BinaryString.fromString("2"),
                        BinaryString.fromString("9999-09-20"),
                        BinaryString.fromString("02:00")));

        return table;
    }
}
