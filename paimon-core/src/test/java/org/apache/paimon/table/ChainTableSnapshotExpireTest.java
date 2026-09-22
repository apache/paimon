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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests snapshot expiration maintenance for chain table branches. */
public class ChainTableSnapshotExpireTest {

    private static final DateTimeFormatter PARTITION_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd");

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testDeltaCommitExpiresSnapshotBranchSnapshotsAfterPartitionExpiration()
            throws Exception {
        Path tablePath = new Path(tempDir.toUri().toString(), "chain_snapshot_expire");
        createChainTable(tablePath, Collections.emptyMap());

        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");
        String commitUser = UUID.randomUUID().toString();

        String day90 = partitionDate(-90);
        String day65 = partitionDate(-65);
        String day40 = partitionDate(-40);
        String day10 = partitionDate(-10);

        // Build three snapshot anchors while partition expiration is disabled.
        write(snapshotTable, commitUser, day90, "v1");
        write(snapshotTable, commitUser, day65, "v2");
        write(snapshotTable, commitUser, day40, "v3");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        assertThat(listPartitions(snapshotTable)).containsExactly(day90, day65, day40);
        assertThat(snapshotTable.snapshotManager().snapshotCount()).isEqualTo(3);
        long latestSnapshotBeforeExpiration = snapshotTable.snapshotManager().latestSnapshotId();

        // Configure Snapshot retention on that branch itself, independently of the Delta writer.
        new FileSystemSchemaManager(LocalFileIO.create(), tablePath, "snapshot")
                .commitChanges(
                        Arrays.asList(
                                SchemaChange.setOption(
                                        CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1"),
                                SchemaChange.setOption(
                                        CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1"),
                                SchemaChange.setOption(
                                        CoreOptions.SNAPSHOT_TIME_RETAINED.key(), "0 ms")));

        Map<String, String> expireOptions = new HashMap<>();
        expireOptions.put(CoreOptions.WRITE_ONLY.key(), "false");
        expireOptions.put(CoreOptions.PARTITION_EXPIRATION_TIME.key(), "30 d");
        expireOptions.put(CoreOptions.END_INPUT_CHECK_PARTITION_EXPIRE.key(), "true");
        expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        expireOptions.put(CoreOptions.SNAPSHOT_TIME_RETAINED.key(), "0 ms");
        expireOptions.put(CoreOptions.SNAPSHOT_EXPIRE_EXECUTION_MODE.key(), "sync");
        deltaTable = deltaTable.copy(expireOptions);

        // A bounded Delta commit deterministically triggers ChainTablePartitionExpire. The two
        // oldest snapshot anchors are expired and the latest expired-time anchor (day40) is kept.
        // Dropping those Snapshot-branch partitions creates a new Snapshot-branch metadata
        // snapshot, which must then be expired according to the Snapshot branch's own policy.
        write(deltaTable, commitUser, day10, "v4");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        assertThat(listPartitions(snapshotTable)).containsExactly(day40);
        assertThat(snapshotTable.snapshotManager().latestSnapshotId())
                .isGreaterThan(latestSnapshotBeforeExpiration);
        assertThat(snapshotTable.snapshotManager().snapshotCount()).isEqualTo(1);
    }

    @ParameterizedTest
    @CsvSource({"3, 3, false, 3", "1, 10, false, 4", "3, 3, true, 3"})
    public void testDeltaCommitPreservesSnapshotBranchRetentionPolicy(
            int retainMin, int retainMax, boolean decoupledChangelog, int expectedSnapshots)
            throws Exception {
        Path tablePath = new Path(tempDir.toUri().toString(), "chain_snapshot_retention");
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), String.valueOf(retainMin));
        tableOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), String.valueOf(retainMax));
        tableOptions.put(CoreOptions.SNAPSHOT_TIME_RETAINED.key(), "365 d");
        if (decoupledChangelog) {
            tableOptions.put(CoreOptions.CHANGELOG_NUM_RETAINED_MIN.key(), "5");
            tableOptions.put(CoreOptions.CHANGELOG_NUM_RETAINED_MAX.key(), "5");
            tableOptions.put(CoreOptions.CHANGELOG_TIME_RETAINED.key(), "365 d");
        }
        // Persist the same policy on all branches. Only the Delta writer will override it.
        createChainTable(tablePath, tableOptions);

        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");
        String commitUser = UUID.randomUUID().toString();
        String day90 = partitionDate(-90);
        String day65 = partitionDate(-65);
        String day40 = partitionDate(-40);

        // Seed both branches without triggering maintenance before the commit under test.
        Map<String, String> writeOnly =
                Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true");
        FileStoreTable snapshotWriter = snapshotTable.copy(writeOnly);
        write(snapshotWriter, commitUser, day90, "v1");
        write(snapshotWriter, commitUser, day65, "v2");
        write(snapshotWriter, commitUser, day40, "v3");
        FileStoreTable deltaWriter = deltaTable.copy(writeOnly);
        write(deltaWriter, commitUser, partitionDate(-10), "v4");
        write(deltaWriter, commitUser, partitionDate(-9), "v5");
        write(deltaWriter, commitUser, partitionDate(-8), "v6");

        assertThat(snapshotTable.snapshotManager().snapshotCount()).isEqualTo(3);
        assertThat(deltaTable.snapshotManager().snapshotCount()).isEqualTo(3);
        long earliestSnapshot = snapshotTable.snapshotManager().earliestSnapshotId();
        long latestSnapshot = snapshotTable.snapshotManager().latestSnapshotId();
        assertThat(snapshotTable.coreOptions().changelogLifecycleDecoupled())
                .isEqualTo(decoupledChangelog);

        Map<String, String> expireOptions = new HashMap<>();
        expireOptions.put(CoreOptions.WRITE_ONLY.key(), "false");
        expireOptions.put(CoreOptions.PARTITION_EXPIRATION_TIME.key(), "30 d");
        expireOptions.put(CoreOptions.END_INPUT_CHECK_PARTITION_EXPIRE.key(), "true");
        expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        expireOptions.put(CoreOptions.SNAPSHOT_TIME_RETAINED.key(), "0 ms");
        expireOptions.put(CoreOptions.CHANGELOG_NUM_RETAINED_MIN.key(), "1");
        expireOptions.put(CoreOptions.CHANGELOG_NUM_RETAINED_MAX.key(), "1");
        expireOptions.put(CoreOptions.CHANGELOG_TIME_RETAINED.key(), "0 ms");
        expireOptions.put(CoreOptions.SNAPSHOT_EXPIRE_EXECUTION_MODE.key(), "sync");
        deltaWriter = deltaTable.copy(expireOptions);
        assertThat(deltaWriter.coreOptions().changelogLifecycleDecoupled()).isFalse();
        write(deltaWriter, commitUser, partitionDate(-7), "v7");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        // Partition expiration still creates a metadata snapshot, but Delta's shorter retention
        // must not remove Snapshot history protected by its count or time retention settings.
        assertThat(listPartitions(snapshotTable)).containsExactly(day40);
        assertThat(snapshotTable.snapshotManager().latestSnapshotId())
                .isGreaterThan(latestSnapshot);
        assertThat(snapshotTable.snapshotManager().snapshotCount()).isEqualTo(expectedSnapshots);
        assertThat(deltaTable.snapshotManager().snapshotCount()).isEqualTo(1);
        // A decoupled Snapshot lifecycle must archive the expired snapshot as a changelog even
        // though the Delta writer uses a coupled lifecycle.
        assertThat(snapshotTable.changelogManager().longLivedChangelogExists(earliestSnapshot))
                .isEqualTo(decoupledChangelog);
    }

    private void createChainTable(Path tablePath, Map<String, String> tableOptions)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        SchemaManager schemaManager = new FileSystemSchemaManager(fileIO, tablePath);

        Map<String, String> options = new HashMap<>(tableOptions);
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.MERGE_ENGINE.key(), "deduplicate");
        options.put(CoreOptions.SEQUENCE_FIELD.key(), "v");

        Schema schema =
                new Schema(
                        RowType.of(
                                        new org.apache.paimon.types.DataType[] {
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING()
                                        },
                                        new String[] {"dt", "pk", "v"})
                                .getFields(),
                        Collections.singletonList("dt"),
                        Arrays.asList("pk", "dt"),
                        options,
                        "");
        schemaManager.createTable(schema);

        FileStoreTable mainTable = loadTable(tablePath);
        mainTable.createBranch("snapshot");
        mainTable.createBranch("delta");

        List<SchemaChange> chainOptions =
                Arrays.asList(
                        SchemaChange.setOption(CoreOptions.CHAIN_TABLE_ENABLED.key(), "true"),
                        SchemaChange.setOption(
                                CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key(), "snapshot"),
                        SchemaChange.setOption(
                                CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key(), "delta"),
                        SchemaChange.setOption(
                                CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), "$dt"),
                        SchemaChange.setOption(
                                CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyyMMdd"));
        schemaManager.commitChanges(chainOptions);
        new FileSystemSchemaManager(fileIO, tablePath, "snapshot").commitChanges(chainOptions);
        new FileSystemSchemaManager(fileIO, tablePath, "delta").commitChanges(chainOptions);
    }

    private FileStoreTable loadTable(Path tablePath) {
        LocalFileIO fileIO = LocalFileIO.create();
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        String branchName = CoreOptions.branch(options.toMap());
        TableSchema tableSchema =
                new FileSystemSchemaManager(fileIO, tablePath, branchName).latest().get();
        return FileStoreTableFactory.create(
                fileIO, tablePath, tableSchema, CatalogEnvironment.empty());
    }

    private void write(FileStoreTable table, String commitUser, String dt, String value)
            throws Exception {
        StreamTableWrite write = table.newWrite(commitUser);
        write.write(
                GenericRow.of(
                        BinaryString.fromString(dt),
                        BinaryString.fromString(value),
                        BinaryString.fromString(value)));
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            List<CommitMessage> commitMessages = write.prepareCommit(true, Long.MAX_VALUE);
            commit.commit(Long.MAX_VALUE, commitMessages);
        }
        write.close();
    }

    private List<String> listPartitions(FileStoreTable table) {
        return table.newSnapshotReader().partitionEntries().stream()
                .map(PartitionEntry::partition)
                .map(partition -> partition.getString(0).toString())
                .sorted()
                .collect(Collectors.toList());
    }

    private String partitionDate(int daysFromToday) {
        return LocalDate.now().plusDays(daysFromToday).format(PARTITION_FORMATTER);
    }
}
