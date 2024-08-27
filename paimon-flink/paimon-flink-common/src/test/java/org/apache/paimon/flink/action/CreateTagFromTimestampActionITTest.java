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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CreateTagFromTimestampAction}. */
public class CreateTagFromTimestampActionITTest extends ActionITCaseBase {
    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.STRING(), DataTypes.STRING()};

    private static final RowType ROW_TYPE = RowType.of(FIELD_TYPES, new String[] {"k", "v"});

    @BeforeEach
    public void setUp() {
        init(warehouse);
    }

    @Test
    public void testExpirePartitionsAction() throws Exception {
        FileStoreTable table = prepareTable();
        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        List<String> actual = getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE);
        List<String> expected;
        expected = Arrays.asList("+I[1, 2024-01-01]", "+I[2, 2024-01-02]", "+I[3, 2024-01-03]");

        assertThat(actual).isEqualTo(expected);
        long ts = table.snapshotManager().latestSnapshot().timeMillis();
        String tag = "tag_test";

        createAction(
                        CreateTagFromTimestampAction.class,
                        "create_tag_from_timestamp",
                        "--warehouse",
                        warehouse,
                        "--table",
                        database + "." + tableName,
                        "--tag",
                        tag,
                        "--timestamp",
                        Long.toString(ts))
                .run();

        Snapshot snapshot = table.tagManager().tags().firstKey();

        assertThat(table.tagManager().tagExists(tag)).isTrue();
        assertThat(table.tagManager().tagCount()).isEqualTo(1);
        assertThat(snapshot.timeMillis()).isEqualTo(ts);
    }

    private FileStoreTable prepareTable() throws Exception {
        init(warehouse);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"k", "v"});
        String[] pk = {"k", "v"};
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.singletonList("v"),
                        new ArrayList<>(Arrays.asList(pk)),
                        Collections.singletonList("k"),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        // 3 snapshots
        writeData(rowData(BinaryString.fromString("1"), BinaryString.fromString("2024-01-01")));
        writeData(rowData(BinaryString.fromString("2"), BinaryString.fromString("2024-01-02")));
        writeData(rowData(BinaryString.fromString("3"), BinaryString.fromString("2024-01-03")));

        return table;
    }
}
