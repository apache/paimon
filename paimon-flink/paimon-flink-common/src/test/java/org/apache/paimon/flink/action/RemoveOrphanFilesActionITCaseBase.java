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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.CoreOptions.SCAN_FALLBACK_BRANCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** IT cases base for {@link RemoveOrphanFilesAction}. */
public abstract class RemoveOrphanFilesActionITCaseBase extends ActionITCaseBase {

    private static final String ORPHAN_FILE_1 = "bucket-0/orphan_file1";
    private static final String ORPHAN_FILE_2 = "bucket-0/orphan_file2";

    private FileStoreTable createTableAndWriteData(String tableName) throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"k", "v"});

        FileStoreTable table =
                createFileStoreTable(
                        tableName,
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        writeData(rowData(1L, BinaryString.fromString("Hi")));

        Path orphanFile1 = getOrphanFilePath(table, ORPHAN_FILE_1);
        Path orphanFile2 = getOrphanFilePath(table, ORPHAN_FILE_2);

        FileIO fileIO = table.fileIO();
        fileIO.writeFile(orphanFile1, "a", true);
        fileIO.writeFile(orphanFile2, "b", true);
        Thread.sleep(2000);

        return table;
    }

    private Path getOrphanFilePath(FileStoreTable table, String orphanFile) {
        return new Path(table.location(), orphanFile);
    }

    private List<String> readTableData(FileStoreTable table) throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"k", "v"});

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        List<String> result =
                getResultLocal(
                        readBuilder.newRead(),
                        plan == null ? Collections.emptyList() : plan.splits(),
                        rowType);
        return result;
    }

    private List<String> getResultLocal(
            org.apache.paimon.table.source.TableRead read,
            List<org.apache.paimon.table.source.Split> splits,
            RowType rowType)
            throws Exception {
        try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow>
                recordReader = read.createReader(splits)) {
            List<String> result = new ArrayList<>();
            recordReader.forEachRemaining(
                    row -> result.add(internalRowToStringLocal(row, rowType)));
            return result;
        }
    }

    /**
     * Stringify the given {@link InternalRow}. This is a simplified version that handles basic
     * types. For complex types (Array, Map, Row), it falls back to toString().
     *
     * <p>This method is implemented locally to avoid dependency on paimon-common's test-jar, which
     * may not be available in CI environments.
     */
    private String internalRowToStringLocal(org.apache.paimon.data.InternalRow row, RowType type) {
        StringBuilder build = new StringBuilder();
        build.append(row.getRowKind().shortString()).append("[");
        for (int i = 0; i < type.getFieldCount(); i++) {
            if (i != 0) {
                build.append(", ");
            }
            if (row.isNullAt(i)) {
                build.append("NULL");
            } else {
                org.apache.paimon.data.InternalRow.FieldGetter fieldGetter =
                        org.apache.paimon.data.InternalRow.createFieldGetter(type.getTypeAt(i), i);
                Object field = fieldGetter.getFieldOrNull(row);
                if (field != null) {
                    build.append(field);
                } else {
                    build.append("NULL");
                }
            }
        }
        build.append("]");
        return build.toString();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRunWithoutException(boolean isNamedArgument) throws Exception {
        assumeTrue(!isNamedArgument || supportNamedArgument());

        FileStoreTable table = createTableAndWriteData(tableName);

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
                String.format(
                        isNamedArgument
                                ? "CALL sys.remove_orphan_files(`table` => '%s.%s')"
                                : "CALL sys.remove_orphan_files('%s.%s')",
                        database,
                        tableName);
        CloseableIterator<Row> withoutOlderThanCollect = executeSQL(withoutOlderThan);
        assertThat(ImmutableList.copyOf(withoutOlderThanCollect)).containsOnly(Row.of("0"));

        String olderThan =
                DateTimeUtils.formatLocalDateTime(
                        DateTimeUtils.toLocalDateTime(System.currentTimeMillis()), 3);
        String withDryRun =
                String.format(
                        isNamedArgument
                                ? "CALL sys.remove_orphan_files(`table` => '%s.%s', older_than => '%s', dry_run => true)"
                                : "CALL sys.remove_orphan_files('%s.%s', '%s', true)",
                        database,
                        tableName,
                        olderThan);
        ImmutableList<Row> actualDryRunDeleteFile = ImmutableList.copyOf(executeSQL(withDryRun));
        assertThat(actualDryRunDeleteFile).containsOnly(Row.of("2"));

        String withOlderThan =
                String.format(
                        isNamedArgument
                                ? "CALL sys.remove_orphan_files(`table` => '%s.%s', older_than => '%s')"
                                : "CALL sys.remove_orphan_files('%s.%s', '%s')",
                        database,
                        tableName,
                        olderThan);
        ImmutableList<Row> actualDeleteFile = ImmutableList.copyOf(executeSQL(withOlderThan));

        assertThat(actualDeleteFile).containsExactlyInAnyOrder(Row.of("2"), Row.of("2"));

        // test clean empty directories
        FileIO fileIO = table.fileIO();
        Path location = table.location();
        Path bucketDir = new Path(location, "bucket-0");

        // delete snapshots and clean orphan files
        fileIO.delete(new Path(location, "snapshot"), true);
        ImmutableList.copyOf(executeSQL(withOlderThan));
        assertThat(fileIO.exists(bucketDir)).isTrue();
        assertThat(fileIO.listDirectories(bucketDir)).isEmpty();

        // clean empty directories
        ImmutableList.copyOf(executeSQL(withOlderThan));
        assertThat(fileIO.exists(bucketDir)).isFalse();
        // table should not be deleted
        assertThat(fileIO.exists(location)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRemoveDatabaseOrphanFilesITCase(boolean isNamedArgument) throws Exception {
        assumeTrue(!isNamedArgument || supportNamedArgument());

        createTableAndWriteData("tableName1");
        createTableAndWriteData("tableName2");

        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "remove_orphan_files",
                                "--warehouse",
                                warehouse,
                                "--database",
                                database));

        if (ThreadLocalRandom.current().nextBoolean()) {
            args.add("--table");
            args.add("*");
        }

        RemoveOrphanFilesAction action1 = createAction(RemoveOrphanFilesAction.class, args);
        assertThatCode(action1::run).doesNotThrowAnyException();

        args.add("--older_than");
        args.add("2023-12-31 23:59:59");
        RemoveOrphanFilesAction action2 = createAction(RemoveOrphanFilesAction.class, args);
        assertThatCode(action2::run).doesNotThrowAnyException();

        args.add("--parallelism");
        args.add("5");
        RemoveOrphanFilesAction action3 = createAction(RemoveOrphanFilesAction.class, args);
        assertThatCode(action3::run).doesNotThrowAnyException();

        String withoutOlderThan =
                String.format(
                        isNamedArgument
                                ? "CALL sys.remove_orphan_files(`table` => '%s.%s')"
                                : "CALL sys.remove_orphan_files('%s.%s')",
                        database,
                        "*");
        CloseableIterator<Row> withoutOlderThanCollect = executeSQL(withoutOlderThan);
        assertThat(ImmutableList.copyOf(withoutOlderThanCollect)).containsOnly(Row.of("0"));

        String withParallelism =
                String.format("CALL sys.remove_orphan_files('%s.%s','',true,5)", database, "*");
        CloseableIterator<Row> withParallelismCollect = executeSQL(withParallelism);
        assertThat(ImmutableList.copyOf(withParallelismCollect)).containsOnly(Row.of("0"));

        String olderThan =
                DateTimeUtils.formatLocalDateTime(
                        DateTimeUtils.toLocalDateTime(System.currentTimeMillis()), 3);
        String withDryRun =
                String.format(
                        isNamedArgument
                                ? "CALL sys.remove_orphan_files(`table` => '%s.%s', older_than => '%s', dry_run => true)"
                                : "CALL sys.remove_orphan_files('%s.%s', '%s', true)",
                        database,
                        "*",
                        olderThan);
        ImmutableList<Row> actualDryRunDeleteFile = ImmutableList.copyOf(executeSQL(withDryRun));
        assertThat(actualDryRunDeleteFile).containsOnly(Row.of("4"));

        String withOlderThan =
                String.format(
                        isNamedArgument
                                ? "CALL sys.remove_orphan_files(`table` => '%s.%s', older_than => '%s')"
                                : "CALL sys.remove_orphan_files('%s.%s', '%s')",
                        database,
                        "*",
                        olderThan);
        ImmutableList<Row> actualDeleteFile = ImmutableList.copyOf(executeSQL(withOlderThan));

        assertThat(actualDeleteFile).containsOnly(Row.of("4"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCleanWithBranch(boolean isNamedArgument) throws Exception {
        assumeTrue(!isNamedArgument || supportNamedArgument());

        // create main branch
        FileStoreTable table = createTableAndWriteData(tableName);

        // create first branch and write some data
        table.createBranch("br");
        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location(), "br");
        TableSchema branchSchema =
                schemaManager.commitChanges(SchemaChange.addColumn("v2", DataTypes.INT()));
        Options branchOptions = new Options(branchSchema.options());
        branchOptions.set(CoreOptions.BRANCH, "br");
        branchSchema = branchSchema.copy(branchOptions.toMap());
        FileStoreTable branchTable =
                FileStoreTableFactory.create(table.fileIO(), table.location(), branchSchema);

        String commitUser = UUID.randomUUID().toString();
        StreamTableWrite write = branchTable.newWrite(commitUser);
        StreamTableCommit commit = branchTable.newCommit(commitUser);
        write.write(GenericRow.of(2L, BinaryString.fromString("Hello"), 20));
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();

        // create orphan file in snapshot directory of first branch
        Path orphanFile3 = new Path(table.location(), "branch/branch-br/snapshot/orphan_file3");
        branchTable.fileIO().writeFile(orphanFile3, "x", true);

        // create second branch, which is empty
        table.createBranch("br2");

        // create orphan file in snapshot directory of second branch
        Path orphanFile4 = new Path(table.location(), "branch/branch-br2/snapshot/orphan_file4");
        branchTable.fileIO().writeFile(orphanFile4, "y", true);
        Thread.sleep(2000);

        if (ThreadLocalRandom.current().nextBoolean()) {
            executeSQL(
                    String.format(
                            "ALTER TABLE `%s`.`%s` SET ('%s' = 'br')",
                            database, tableName, SCAN_FALLBACK_BRANCH.key()),
                    false,
                    true);
        }

        String olderThan =
                DateTimeUtils.formatLocalDateTime(
                        DateTimeUtils.toLocalDateTime(System.currentTimeMillis()), 3);
        String procedure =
                String.format(
                        isNamedArgument
                                ? "CALL sys.remove_orphan_files(`table` => '%s.%s', older_than => '%s')"
                                : "CALL sys.remove_orphan_files('%s.%s', '%s')",
                        database,
                        "*",
                        olderThan);
        ImmutableList<Row> actualDeleteFile = ImmutableList.copyOf(executeSQL(procedure));
        assertThat(actualDeleteFile).containsOnly(Row.of("4"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRunWithMode(boolean isNamedArgument) throws Exception {
        assumeTrue(!isNamedArgument || supportNamedArgument());

        createTableAndWriteData(tableName);

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
                String.format(
                        isNamedArgument
                                ? "CALL sys.remove_orphan_files(`table` => '%s.%s')"
                                : "CALL sys.remove_orphan_files('%s.%s')",
                        database,
                        tableName);
        CloseableIterator<Row> withoutOlderThanCollect = executeSQL(withoutOlderThan);
        assertThat(ImmutableList.copyOf(withoutOlderThanCollect)).containsOnly(Row.of("0"));

        String olderThan =
                DateTimeUtils.formatLocalDateTime(
                        DateTimeUtils.toLocalDateTime(System.currentTimeMillis()), 3);
        String withLocalMode =
                String.format(
                        isNamedArgument
                                ? "CALL sys.remove_orphan_files(`table` => '%s.%s', older_than => '%s', dry_run => true, parallelism => 5, mode => 'local')"
                                : "CALL sys.remove_orphan_files('%s.%s', '%s', true, 5, 'local')",
                        database,
                        tableName,
                        olderThan);
        ImmutableList<Row> actualLocalRunDeleteFile =
                ImmutableList.copyOf(executeSQL(withLocalMode));
        assertThat(actualLocalRunDeleteFile).containsOnly(Row.of("2"));

        String withDistributedMode =
                String.format(
                        isNamedArgument
                                ? "CALL sys.remove_orphan_files(`table` => '%s.%s', older_than => '%s', dry_run => true, parallelism => 5, mode => 'distributed')"
                                : "CALL sys.remove_orphan_files('%s.%s', '%s', true, 5, 'distributed')",
                        database,
                        tableName,
                        olderThan);
        ImmutableList<Row> actualDistributedRunDeleteFile =
                ImmutableList.copyOf(executeSQL(withDistributedMode));
        assertThat(actualDistributedRunDeleteFile).containsOnly(Row.of("2"));

        String withInvalidMode =
                String.format(
                        isNamedArgument
                                ? "CALL sys.remove_orphan_files(`table` => '%s.%s', older_than => '%s', dry_run => true, parallelism => 5, mode => 'unknown')"
                                : "CALL sys.remove_orphan_files('%s.%s', '%s', true, 5, 'unknown')",
                        database,
                        tableName,
                        olderThan);
        assertThatCode(() -> executeSQL(withInvalidMode))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unknown mode");
    }

    @org.junit.jupiter.api.Test
    public void testBatchTableProcessing() throws Exception {
        long fileCreationTime = System.currentTimeMillis();
        FileStoreTable table1 = createTableAndWriteData("batchTable1");
        FileStoreTable table2 = createTableAndWriteData("batchTable2");
        FileStoreTable table3 = createTableAndWriteData("batchTable3");

        FileIO fileIO1 = table1.fileIO();
        FileIO fileIO2 = table2.fileIO();
        FileIO fileIO3 = table3.fileIO();

        Path orphanFile1Table1 = getOrphanFilePath(table1, ORPHAN_FILE_1);
        Path orphanFile2Table1 = getOrphanFilePath(table1, ORPHAN_FILE_2);
        Path orphanFile1Table2 = getOrphanFilePath(table2, ORPHAN_FILE_1);
        Path orphanFile2Table2 = getOrphanFilePath(table2, ORPHAN_FILE_2);
        Path orphanFile1Table3 = getOrphanFilePath(table3, ORPHAN_FILE_1);
        Path orphanFile2Table3 = getOrphanFilePath(table3, ORPHAN_FILE_2);

        Path[] orphanFiles = {
            orphanFile1Table1, orphanFile2Table1,
            orphanFile1Table2, orphanFile2Table2,
            orphanFile1Table3, orphanFile2Table3
        };
        FileIO[] fileIOs = {fileIO1, fileIO1, fileIO2, fileIO2, fileIO3, fileIO3};

        Thread.sleep(5000);

        long currentTime = System.currentTimeMillis();
        long olderThanMillis = Math.max(fileCreationTime + 1000, currentTime - 1000);
        String olderThan =
                DateTimeUtils.formatLocalDateTime(
                        DateTimeUtils.toLocalDateTime(olderThanMillis), 3);

        long expectedFileCount = 6;
        long expectedTotalSize = 0;
        for (int i = 0; i < orphanFiles.length; i++) {
            if (fileIOs[i].exists(orphanFiles[i])) {
                expectedTotalSize += fileIOs[i].getFileSize(orphanFiles[i]);
            }
        }

        // Test non-batch mode
        String withoutBatchMode =
                String.format(
                        "CALL sys.remove_orphan_files('%s.%s', '%s', false)",
                        database, "*", olderThan);
        ImmutableList<Row> withoutBatchModeResult =
                ImmutableList.copyOf(executeSQL(withoutBatchMode));
        assertThat(withoutBatchModeResult).hasSize(2);
        long deletedFileCountWithoutBatch =
                Long.parseLong(withoutBatchModeResult.get(0).getField(0).toString());
        long deletedFileTotalLenInBytesWithoutBatch =
                Long.parseLong(withoutBatchModeResult.get(1).getField(0).toString());
        assertThat(deletedFileCountWithoutBatch)
                .as("Non-batch mode should delete 6 orphan files")
                .isEqualTo(expectedFileCount);
        assertThat(deletedFileTotalLenInBytesWithoutBatch)
                .as("Non-batch mode should delete files with expected total size")
                .isEqualTo(expectedTotalSize);

        // Verify files are deleted by non-batch mode
        for (int i = 0; i < orphanFiles.length; i++) {
            assertThat(fileIOs[i].exists(orphanFiles[i]))
                    .as("Orphan file should be deleted by non-batch mode")
                    .isFalse();
        }

        // Recreate orphan files for batch mode test
        long batchFileCreationTime = System.currentTimeMillis();
        for (int i = 0; i < orphanFiles.length; i++) {
            fileIOs[i].writeFile(orphanFiles[i], "orphan", true);
        }
        Thread.sleep(5000);

        long batchCurrentTime = System.currentTimeMillis();
        long batchOlderThanMillis = Math.max(batchFileCreationTime + 1000, batchCurrentTime - 1000);
        String batchOlderThan =
                DateTimeUtils.formatLocalDateTime(
                        DateTimeUtils.toLocalDateTime(batchOlderThanMillis), 3);

        // Test batch mode (COMBINED mode)
        List<String> args =
                new ArrayList<>(
                        Arrays.asList(
                                "remove_orphan_files",
                                "--warehouse",
                                warehouse,
                                "--database",
                                database,
                                "--table",
                                "*",
                                "--mode",
                                "combined",
                                "--dry_run",
                                "false",
                                "--older_than",
                                batchOlderThan));
        RemoveOrphanFilesAction action1 = createAction(RemoveOrphanFilesAction.class, args);
        assertThatCode(action1::run).doesNotThrowAnyException();

        // Verify files are deleted by batch mode (same result as non-batch mode)
        for (int i = 0; i < orphanFiles.length; i++) {
            assertThat(fileIOs[i].exists(orphanFiles[i]))
                    .as("Orphan file should be deleted by batch mode (same as non-batch mode)")
                    .isFalse();
        }

        // Verify that normal data in tables can still be read after batch mode deletion
        List<String> table1Data = readTableData(table1);
        assertThat(table1Data)
                .as("Table1 should still contain normal data after batch mode deletion")
                .containsExactly("+I[1, Hi]");

        List<String> table2Data = readTableData(table2);
        assertThat(table2Data)
                .as("Table2 should still contain normal data after batch mode deletion")
                .containsExactly("+I[1, Hi]");

        List<String> table3Data = readTableData(table3);
        assertThat(table3Data)
                .as("Table3 should still contain normal data after batch mode deletion")
                .containsExactly("+I[1, Hi]");
    }

    @org.junit.jupiter.api.Test
    public void testBatchTableProcessingWithBranch() throws Exception {
        long fileCreationTime = System.currentTimeMillis();

        // Create table with multiple branches to test bug: same table, multiple branches
        // This will trigger the bug in computeIfAbsent if branchTable is used instead of key
        FileStoreTable table = createTableAndWriteData("batchBranchTable");

        // Create first branch and write data
        table.createBranch("br1");
        FileStoreTable branchTable1 = createBranchTable(table, "br1");
        writeToBranch(branchTable1, GenericRow.of(2L, BinaryString.fromString("Hello"), 20));

        // Create second branch and write data
        table.createBranch("br2");
        FileStoreTable branchTable2 = createBranchTable(table, "br2");
        writeToBranch(branchTable2, GenericRow.of(3L, BinaryString.fromString("World"), 30));

        // Create orphan files in both branch snapshot directories
        // This is key: same table, multiple branches - will trigger bug in
        // endInputForUsedFilesForBatch
        Path orphanFileBr1 =
                new Path(table.location(), "branch/branch-br1/snapshot/orphan_file_br1");
        Path orphanFileBr2 =
                new Path(table.location(), "branch/branch-br2/snapshot/orphan_file_br2");
        branchTable1.fileIO().writeFile(orphanFileBr1, "x", true);
        branchTable2.fileIO().writeFile(orphanFileBr2, "y", true);

        Thread.sleep(5000);
        long olderThanMillis = Math.max(fileCreationTime + 1000, System.currentTimeMillis() - 1000);
        String olderThan =
                DateTimeUtils.formatLocalDateTime(
                        DateTimeUtils.toLocalDateTime(olderThanMillis), 3);

        // Test batch mode (COMBINED mode) with multiple branches in same table
        List<String> args =
                Arrays.asList(
                        "remove_orphan_files",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        "*",
                        "--mode",
                        "combined",
                        "--dry_run",
                        "false",
                        "--older_than",
                        olderThan);
        RemoveOrphanFilesAction action = createAction(RemoveOrphanFilesAction.class, args);
        assertThatCode(action::run).doesNotThrowAnyException();

        // Verify orphan files are deleted
        assertThat(branchTable1.fileIO().exists(orphanFileBr1)).isFalse();
        assertThat(branchTable2.fileIO().exists(orphanFileBr2)).isFalse();

        // Verify normal data can still be read
        assertThat(readTableData(table)).containsExactly("+I[1, Hi]");
        RowType branchRowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.INT()},
                        new String[] {"k", "v", "v2"});
        assertThat(readBranchData(branchTable1, branchRowType)).containsExactly("+I[2, Hello, 20]");
        assertThat(readBranchData(branchTable2, branchRowType)).containsExactly("+I[3, World, 30]");
    }

    @org.junit.jupiter.api.Test
    public void testCleanOrphanManifestListFiles() throws Exception {
        // Create table and write data to generate snapshots
        FileStoreTable table = createTableAndWriteData("manifestListTable");
        FileIO fileIO = table.fileIO();
        Path location = table.location();
        Path manifestPath = new Path(location, "manifest");

        // Wait for files to be old enough
        Thread.sleep(2000);

        // Get current snapshot to find manifest-list files
        org.apache.paimon.utils.SnapshotManager snapshotManager = table.snapshotManager();
        org.apache.paimon.Snapshot currentSnapshot = snapshotManager.latestSnapshot();
        assertThat(currentSnapshot).isNotNull();

        // Verify that valid manifest-list files exist (referenced by snapshot)
        String validManifestListName = null;
        if (currentSnapshot.baseManifestList() != null) {
            validManifestListName = new Path(currentSnapshot.baseManifestList()).getName();
            Path validManifestListPath = new Path(manifestPath, validManifestListName);
            assertThat(fileIO.exists(validManifestListPath))
                    .as("Valid manifest-list file should exist")
                    .isTrue();
        }

        // Create an orphan manifest-list file (not referenced by any snapshot)
        String orphanManifestList = "manifest-list-orphan-" + System.currentTimeMillis();
        Path orphanManifestListPath = new Path(manifestPath, orphanManifestList);
        fileIO.writeFile(orphanManifestListPath, "orphan manifest-list content", true);

        // Verify the orphan manifest-list file exists
        assertThat(fileIO.exists(orphanManifestListPath)).isTrue();

        // Calculate olderThan to ensure the orphan file is old enough
        long fileCreationTime = System.currentTimeMillis();
        Thread.sleep(1000);
        long olderThanMillis = Math.max(fileCreationTime + 1000, System.currentTimeMillis() - 1000);
        String olderThan =
                DateTimeUtils.formatLocalDateTime(
                        DateTimeUtils.toLocalDateTime(olderThanMillis), 3);

        // Run orphan files clean
        String cleanSQL =
                String.format(
                        "CALL sys.remove_orphan_files('%s.%s', '%s', false)",
                        database, "manifestListTable", olderThan);
        ImmutableList<Row> result = ImmutableList.copyOf(executeSQL(cleanSQL));

        // Verify the orphan manifest-list file is deleted
        assertThat(fileIO.exists(orphanManifestListPath))
                .as("Orphan manifest-list file should be deleted")
                .isFalse();

        // Verify that valid manifest-list files (referenced by snapshots) are not deleted
        if (validManifestListName != null) {
            Path validManifestListPath = new Path(manifestPath, validManifestListName);
            assertThat(fileIO.exists(validManifestListPath))
                    .as("Valid manifest-list file referenced by snapshot should not be deleted")
                    .isTrue();
        }

        // Verify normal data can still be read
        List<String> tableData = readTableData(table);
        assertThat(tableData)
                .as("Table should still contain normal data after manifest-list cleanup")
                .containsExactly("+I[1, Hi]");
    }

    private FileStoreTable createBranchTable(FileStoreTable table, String branchName)
            throws Exception {
        SchemaManager schemaManager =
                new SchemaManager(table.fileIO(), table.location(), branchName);
        TableSchema branchSchema =
                schemaManager.commitChanges(SchemaChange.addColumn("v2", DataTypes.INT()));
        Options branchOptions = new Options(branchSchema.options());
        branchOptions.set(CoreOptions.BRANCH, branchName);
        branchSchema = branchSchema.copy(branchOptions.toMap());
        return FileStoreTableFactory.create(table.fileIO(), table.location(), branchSchema);
    }

    private void writeToBranch(FileStoreTable branchTable, GenericRow data) throws Exception {
        String commitUser = UUID.randomUUID().toString();
        StreamTableWrite write = branchTable.newWrite(commitUser);
        StreamTableCommit commit = branchTable.newCommit(commitUser);
        write.write(data);
        commit.commit(1, write.prepareCommit(false, 1));
        write.close();
        commit.close();
    }

    private List<String> readBranchData(FileStoreTable branchTable, RowType rowType)
            throws Exception {
        ReadBuilder readBuilder = branchTable.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        return getResultLocal(
                readBuilder.newRead(),
                plan == null ? Collections.emptyList() : plan.splits(),
                rowType);
    }

    protected boolean supportNamedArgument() {
        return true;
    }
}
