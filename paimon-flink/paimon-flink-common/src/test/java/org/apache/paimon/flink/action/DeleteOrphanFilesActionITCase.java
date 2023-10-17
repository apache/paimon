/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/** IT cases for {@link DeleteOrphanFilesAction}. */
public class DeleteOrphanFilesActionITCase extends ActionITCaseBase {
    private static final DataType[] FIELD_TYPES =
            new DataType[] {
                DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()
            };

    private static final RowType ROW_TYPE =
            RowType.of(FIELD_TYPES, new String[] {"a", "b", "c", "d"});

    @ParameterizedTest
    @CsvSource({"1, 1", "1, 2", "2, 1", "2, 2"})
    public void testDeleteOrphanFilesAction(int parallelism, int maxConcurrentDeletes)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>());
        FileIO fileIO = table.fileIO();

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        writeData(rowData(1L, 1L, BinaryString.fromString("Hi"), BinaryString.fromString("Hi")));
        writeData(
                rowData(
                        2L,
                        2L,
                        BinaryString.fromString("Hello"),
                        BinaryString.fromString("Hello")));
        writeData(
                rowData(
                        3L,
                        3L,
                        BinaryString.fromString("Paimon"),
                        BinaryString.fromString("Paimon")));

        Path orphanFile1 = new Path(table.location().getPath() + "/bucket-0/a.txt");
        fileIO.writeFileUtf8(orphanFile1, "aaa");

        Path orphanFile2 = new Path(table.location().getPath() + "/manifest/b.txt");
        fileIO.writeFileUtf8(orphanFile2, "bbb");

        FileStatus[] fileStatuses =
                fileIO.listStatus(new Path(table.location().getPath() + "/bucket-0/"));
        Assertions.assertEquals(
                4, fileStatuses.length, "There should be 4 files on the bucket dir.");

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/manifest/"));
        Assertions.assertEquals(
                10, fileStatuses.length, "There should be 10 files on the manifest dir.");

        DeleteOrphanFilesAction action =
                new DeleteOrphanFilesAction(warehouse, database, tableName, new HashMap<>());
        action.withStreamExecutionEnvironment(env);
        action.olderThan(System.currentTimeMillis());
        action.maxConcurrentDeletes(maxConcurrentDeletes);
        action.run();

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/bucket-0/"));
        Assertions.assertEquals(3, fileStatuses.length, "There should be 3 files on the dir.");

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/manifest/"));
        Assertions.assertEquals(
                9, fileStatuses.length, "There should be 9 files on the manifest dir.");

        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        List<String> result = getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE);
        Assertions.assertEquals(
                "[+I[1, 1, Hi, Hi], +I[2, 2, Hello, Hello], +I[3, 3, Paimon, Paimon]]",
                result.toString());
    }

    @Test
    public void testWithLocation() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Arrays.asList("a", "b"),
                        Arrays.asList("a", "b", "c"),
                        new HashMap<>());
        FileIO fileIO = table.fileIO();

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        writeData(rowData(1L, 1L, BinaryString.fromString("Hi"), BinaryString.fromString("Hi")));
        writeData(
                rowData(
                        2L,
                        2L,
                        BinaryString.fromString("Hello"),
                        BinaryString.fromString("Hello")));
        writeData(
                rowData(
                        3L,
                        3L,
                        BinaryString.fromString("Paimon"),
                        BinaryString.fromString("Paimon")));

        // write orphan file to partition (a=1) and (a=2)
        Path orphanFile = new Path(table.location().getPath() + "/a=1/1.txt");
        fileIO.writeFileUtf8(orphanFile, "aaa");

        orphanFile = new Path(table.location().getPath() + "/a=2/2.txt");
        fileIO.writeFileUtf8(orphanFile, "bbb");

        FileStatus[] fileStatuses =
                fileIO.listStatus(new Path(table.location().getPath() + "/a=1/"));
        Assertions.assertEquals(
                2, fileStatuses.length, "There should be 2 files on the partition dir.");

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/a=2/"));
        Assertions.assertEquals(
                2, fileStatuses.length, "There should be 2 files on the partition dir.");

        String location = table.location().getPath() + "/a=1";

        DeleteOrphanFilesAction action =
                new DeleteOrphanFilesAction(warehouse, database, tableName, new HashMap<>());
        action.olderThan(System.currentTimeMillis());
        action.location(location);
        action.run();

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/a=1/"));
        Assertions.assertEquals(1, fileStatuses.length, "There should be 1 files on the dir.");

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/a=2/"));
        Assertions.assertEquals(2, fileStatuses.length, "There should be 2 files on the dir.");

        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        List<String> result = getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE);
        Assertions.assertEquals(
                "[+I[1, 1, Hi, Hi], +I[2, 2, Hello, Hello], +I[3, 3, Paimon, Paimon]]",
                result.toString());
    }

    @Test
    public void testWithPartitionedTable() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Arrays.asList("a", "b"),
                        Arrays.asList("a", "b", "c"),
                        new HashMap<>());
        FileIO fileIO = table.fileIO();

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        writeData(rowData(1L, 1L, BinaryString.fromString("Hi"), BinaryString.fromString("Hi")));
        writeData(
                rowData(
                        2L,
                        2L,
                        BinaryString.fromString("Hello"),
                        BinaryString.fromString("Hello")));
        writeData(
                rowData(
                        3L,
                        3L,
                        BinaryString.fromString("Paimon"),
                        BinaryString.fromString("Paimon")));

        Path orphanFile1 = new Path(table.location().getPath() + "/a=1/a.txt");
        fileIO.writeFileUtf8(orphanFile1, "aaa");

        Path orphanFile2 = new Path(table.location().getPath() + "/manifest/b.txt");
        fileIO.writeFileUtf8(orphanFile2, "bbb");

        FileStatus[] fileStatuses =
                fileIO.listStatus(new Path(table.location().getPath() + "/a=1/"));
        Assertions.assertEquals(
                2, fileStatuses.length, "There should be 2 files on the partition dir.");

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/manifest/"));
        Assertions.assertEquals(
                10, fileStatuses.length, "There should be 10 files on the manifest dir.");

        DeleteOrphanFilesAction action =
                new DeleteOrphanFilesAction(warehouse, database, tableName, new HashMap<>());
        action.olderThan(System.currentTimeMillis());
        action.run();

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/a=1/"));
        Assertions.assertEquals(1, fileStatuses.length, "There should be 1 files on the dir.");

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/manifest/"));
        Assertions.assertEquals(
                9, fileStatuses.length, "There should be 9 files on the manifest dir.");

        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        List<String> result = getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE);
        Assertions.assertEquals(
                "[+I[1, 1, Hi, Hi], +I[2, 2, Hello, Hello], +I[3, 3, Paimon, Paimon]]",
                result.toString());
    }

    @Test
    public void testWithOlderThan() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Arrays.asList("a", "b"),
                        Arrays.asList("a", "b", "c"),
                        new HashMap<>());
        FileIO fileIO = table.fileIO();

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        writeData(rowData(1L, 1L, BinaryString.fromString("Hi"), BinaryString.fromString("Hi")));
        writeData(
                rowData(
                        2L,
                        2L,
                        BinaryString.fromString("Hello"),
                        BinaryString.fromString("Hello")));
        writeData(
                rowData(
                        3L,
                        3L,
                        BinaryString.fromString("Paimon"),
                        BinaryString.fromString("Paimon")));

        Path orphanFile1 = new Path(table.location().getPath() + "/a=1/a.txt");
        fileIO.writeFileUtf8(orphanFile1, "aaa");

        Thread.sleep(100);

        Path orphanFile2 = new Path(table.location().getPath() + "/manifest/b.txt");
        fileIO.writeFileUtf8(orphanFile2, "bbb");

        FileStatus[] fileStatuses =
                fileIO.listStatus(new Path(table.location().getPath() + "/a=1/"));
        Assertions.assertEquals(
                2, fileStatuses.length, "There should be 2 files on the partition dir.");

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/manifest/"));
        Assertions.assertEquals(
                10, fileStatuses.length, "There should be 10 files on the manifest dir.");

        DeleteOrphanFilesAction action =
                new DeleteOrphanFilesAction(warehouse, database, tableName, new HashMap<>());
        action.olderThan(fileIO.getFileStatus(orphanFile2).getModificationTime() - 1);
        action.run();

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/a=1/"));
        Assertions.assertEquals(1, fileStatuses.length, "There should be 1 files on the dir.");

        // the orphan file which in the manifest dir will not be deleted.
        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/manifest/"));
        Assertions.assertEquals(
                10, fileStatuses.length, "There should be 10 files on the manifest dir.");

        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        List<String> result = getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE);
        Assertions.assertEquals(
                "[+I[1, 1, Hi, Hi], +I[2, 2, Hello, Hello], +I[3, 3, Paimon, Paimon]]",
                result.toString());
    }

    @Test
    public void testWithManyFiles() throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Arrays.asList("a", "b"),
                        Arrays.asList("a", "b", "c"),
                        new HashMap<>());
        FileIO fileIO = table.fileIO();

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        List<String> expect = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            writeData(
                    rowData(
                            i,
                            i,
                            BinaryString.fromString("Hi" + i),
                            BinaryString.fromString("Hi")));
            writeData(
                    rowData(
                            i,
                            i,
                            BinaryString.fromString("Hello" + i),
                            BinaryString.fromString("Hello")));
            writeData(
                    rowData(
                            i,
                            i,
                            BinaryString.fromString("Paimon" + i),
                            BinaryString.fromString("Paimon")));

            expect.add(String.format("+I[%s, %s, Hello%s, Hello]", i, i, i));
            expect.add(String.format("+I[%s, %s, Hi%s, Hi]", i, i, i));
            expect.add(String.format("+I[%s, %s, Paimon%s, Paimon]", i, i, i));
        }

        for (int i = 0; i < 100; i++) {
            Path orphanFile1 =
                    new Path(String.format("%s/a=%s/a.txt", table.location().getPath(), i));
            fileIO.writeFileUtf8(orphanFile1, "aaa");
        }

        Path orphanFile2 = new Path(table.location().getPath() + "/manifest/b.txt");
        fileIO.writeFileUtf8(orphanFile2, "bbb");

        FileStatus[] fileStatuses =
                fileIO.listStatus(new Path(table.location().getPath() + "/a=1/"));
        Assertions.assertEquals(
                2, fileStatuses.length, "There should be 2 files on the partition dir.");

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/manifest/"));
        Assertions.assertEquals(
                911, fileStatuses.length, "There should be 911 files on the manifest dir.");

        DeleteOrphanFilesAction action =
                new DeleteOrphanFilesAction(warehouse, database, tableName, new HashMap<>());
        action.olderThan(System.currentTimeMillis());
        action.run();

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/a=1/"));
        Assertions.assertEquals(1, fileStatuses.length, "There should be 1 files on the dir.");

        fileStatuses = fileIO.listStatus(new Path(table.location().getPath() + "/manifest/"));
        Assertions.assertEquals(
                610, fileStatuses.length, "There should be 610 files on the manifest dir.");

        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        List<String> result = getResult(table.newReadBuilder().newRead(), plan.splits(), ROW_TYPE);
        Assertions.assertEquals(expect, result);
    }
}
