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

import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.operation.ListUnexistingFilesTest;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link RemoveUnexistingFilesAction}. */
public class RemoveUnexistingFilesActionITCase extends ActionITCaseBase {

    @Test
    public void testPartitionArgument() throws Exception {
        int[] numDeletes = prepareTableMissingFilesInEveryPartition();

        // only repair pt=0
        createAction(
                        RemoveUnexistingFilesAction.class,
                        "remove_unexisting_files",
                        "--warehouse",
                        warehouse,
                        "--database",
                        "mydb",
                        "--table",
                        "t",
                        "--partition",
                        "pt=0")
                .withParallelism(2)
                .run();

        // the missing files of pt=1 are still in the manifest
        assertThat(countUnexistingFiles()).containsExactly(0, numDeletes[1]);
    }

    @Test
    public void testPartitionArgumentMatchingNothing() throws Exception {
        int[] numDeletes = prepareTableMissingFilesInEveryPartition();

        createAction(
                        RemoveUnexistingFilesAction.class,
                        "remove_unexisting_files",
                        "--warehouse",
                        warehouse,
                        "--database",
                        "mydb",
                        "--table",
                        "t",
                        "--partition",
                        "pt=99")
                .withParallelism(2)
                .run();

        assertThat(countUnexistingFiles()).containsExactly(numDeletes);
    }

    private int[] prepareTableMissingFilesInEveryPartition() throws Exception {
        int[] numDeletes = new int[2];
        FileStoreTable table =
                ListUnexistingFilesTest.prepareRandomlyDeletedTable(
                        warehouse, "mydb", "t", -1, 10, numDeletes, false);
        LocalFileIO fileIO = LocalFileIO.create();
        for (int i = 0; i < numDeletes.length; i++) {
            if (numDeletes[i] == 0) {
                FileStatus[] files =
                        fileIO.listStatus(new Path(table.location(), "pt=" + i + "/bucket-0"));
                fileIO.deleteQuietly(files[0].getPath());
                numDeletes[i] = 1;
            }
        }
        return numDeletes;
    }

    /** Counts the unexisting files per partition with a dry run over the whole table. */
    private int[] countUnexistingFiles() throws Exception {
        RemoveUnexistingFilesAction action =
                createAction(
                                RemoveUnexistingFilesAction.class,
                                "remove_unexisting_files",
                                "--warehouse",
                                warehouse,
                                "--database",
                                "mydb",
                                "--table",
                                "t",
                                "--dry_run",
                                "true")
                        .withParallelism(2);
        int[] actual = new int[2];
        Pattern pattern = Pattern.compile("pt=(\\d+?)/");
        try (CloseableIterator<String> it = action.buildDataStream().executeAndCollect()) {
            while (it.hasNext()) {
                Matcher matcher = pattern.matcher(it.next());
                if (matcher.find()) {
                    actual[Integer.parseInt(matcher.group(1))]++;
                }
            }
        }
        return actual;
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 3})
    public void testAction(int bucket) throws Exception {
        int numPartitions = 2;
        int numFiles = 10;
        int[] numDeletes = new int[numPartitions];
        ListUnexistingFilesTest.prepareRandomlyDeletedTable(
                warehouse, "mydb", "t", bucket, numFiles, numDeletes, false);

        RemoveUnexistingFilesAction action =
                createAction(
                                RemoveUnexistingFilesAction.class,
                                "remove_unexisting_files",
                                "--warehouse",
                                warehouse,
                                "--database",
                                "mydb",
                                "--table",
                                "t",
                                "--dry_run",
                                "true")
                        .withParallelism(2);
        int[] actual = new int[numPartitions];
        Pattern pattern = Pattern.compile("pt=(\\d+?)/");
        try (CloseableIterator<String> it = action.buildDataStream().executeAndCollect()) {
            while (it.hasNext()) {
                String path = it.next();
                Matcher matcher = pattern.matcher(path);
                if (matcher.find()) {
                    actual[Integer.parseInt(matcher.group(1))]++;
                }
            }
        }
        assertThat(actual).isEqualTo(numDeletes);

        action =
                createAction(
                                RemoveUnexistingFilesAction.class,
                                "remove_unexisting_files",
                                "--warehouse",
                                warehouse,
                                "--database",
                                "mydb",
                                "--table",
                                "t")
                        .withParallelism(2);
        action.run();
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql(
                "CREATE CATALOG mycat WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql("USE CATALOG mycat");
        try (CloseableIterator<Row> it =
                tEnv.executeSql("SELECT pt, CAST(COUNT(*) AS INT) FROM mydb.t GROUP BY pt")
                        .collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                assertThat(row.getField(1)).isEqualTo(numFiles - numDeletes[(int) row.getField(0)]);
            }
        }
    }
}
