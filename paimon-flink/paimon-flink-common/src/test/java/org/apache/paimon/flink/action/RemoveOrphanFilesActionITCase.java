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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** IT cases for {@link RemoveOrphanFilesAction}. */
public class RemoveOrphanFilesActionITCase extends ActionITCaseBase {

    @Test
    public void testRunWithoutException() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"k", "v"});

        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        writeData(rowData(1L, BinaryString.fromString("Hi")));

        Path orphanFile1 = new Path(table.location(), "bucket-0/orphan_file1");
        Path orphanFile2 = new Path(table.location(), "bucket-0/orphan_file2");

        FileIO fileIO = table.fileIO();
        fileIO.writeFile(orphanFile1, "a", true);
        Thread.sleep(2000);
        fileIO.writeFile(orphanFile2, "b", true);

        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "remove_orphan_files",
                                "--warehouse",
                                warehouse,
                                "--database",
                                database,
                                "--table",
                                tableName));
        RemoveOrphanFilesAction action1 = createAction(RemoveOrphanFilesAction.class, args);
        assertThatCode(action1::run).doesNotThrowAnyException();

        args.add("--older_than");
        args.add("2023-12-31 23:59:59");
        RemoveOrphanFilesAction action2 = createAction(RemoveOrphanFilesAction.class, args);
        assertThatCode(action2::run).doesNotThrowAnyException();

        String withoutOlderThan =
                String.format("CALL sys.remove_orphan_files('%s.%s')", database, tableName);
        CloseableIterator<Row> withoutOlderThanCollect = callProcedure(withoutOlderThan);
        assertThat(ImmutableList.copyOf(withoutOlderThanCollect).size()).isEqualTo(0);

        String withDryRun =
                String.format(
                        "CALL sys.remove_orphan_files('%s.%s', '2999-12-31 23:59:59', true)",
                        database, tableName);
        ImmutableList<Row> actualDryRunDeleteFile = ImmutableList.copyOf(callProcedure(withDryRun));
        assertThat(actualDryRunDeleteFile)
                .containsExactlyInAnyOrder(
                        Row.of(orphanFile1.toUri().getPath()),
                        Row.of(orphanFile2.toUri().getPath()));

        String withOlderThan =
                String.format(
                        "CALL sys.remove_orphan_files('%s.%s', '2999-12-31 23:59:59')",
                        database, tableName);
        ImmutableList<Row> actualDeleteFile = ImmutableList.copyOf(callProcedure(withOlderThan));

        assertThat(actualDeleteFile)
                .containsExactlyInAnyOrder(
                        Row.of(orphanFile1.toUri().getPath()),
                        Row.of(orphanFile2.toUri().getPath()));
    }
}
