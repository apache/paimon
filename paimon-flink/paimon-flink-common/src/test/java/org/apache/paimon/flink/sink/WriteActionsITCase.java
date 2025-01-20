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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link CoreOptions.WriteAction }. */
public class WriteActionsITCase extends CatalogITCaseBase {

    private static final int TIMEOUT = 180;

    @Timeout(value = TIMEOUT)
    @ParameterizedTest
    @ValueSource(strings = {"PARTITION-EXPIRE", "SNAPSHOT-EXPIRE", "TAG-AUTOMATIC-CREATION"})
    public void testWriteActionsWhichExecutedDuringCommit(String val) throws Exception {

        CoreOptions.WriteAction writeAction =
                CoreOptions.WriteAction.valueOf(val.replace("-", "_"));

        HashMap<String, String> writeActionOptions =
                createOptions(
                        String.format(
                                "%s,%s,%s",
                                writeAction,
                                CoreOptions.WriteAction.FULL_COMPACT,
                                CoreOptions.WriteAction.MINOR_COMPACT));

        createPrimaryKeyTable("T", writeActionOptions);
        sql("INSERT INTO T VALUES ('HXH', '20250101')");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        switch (writeAction) {
            case PARTITION_EXPIRE:
                // Only do partition expiration, so generate 3 snapshot, append,compact,overwrite.
                expectTable(table, snapshotManager, 3, 3, 0, null);
                // Snapshot 1 is APPEND, data write.
                assertThat(snapshotManager.snapshot(1).commitKind())
                        .isEqualTo(Snapshot.CommitKind.APPEND);
                // Snapshot 2 is COMPACT, full compact.
                assertThat(snapshotManager.snapshot(2).commitKind())
                        .isEqualTo(Snapshot.CommitKind.COMPACT);
                // Snapshot 3 is OVERWRITE, partition expired.
                assertThat(snapshotManager.snapshot(3).commitKind())
                        .isEqualTo(Snapshot.CommitKind.OVERWRITE);
                break;
            case SNAPSHOT_EXPIRE:
                // Only do snapshot expiration, so only generate 2 snapshot, snapshot 1 has expired
                // and partition will be retained.
                expectTable(table, snapshotManager, 1, 2, 0, "20250101");
                // Snapshot expired. so snapshot 1 has expired and snapshot 2 is COMPACT.
                assertThat(snapshotManager.snapshot(2).commitKind())
                        .isEqualTo(Snapshot.CommitKind.COMPACT);
                break;
            case TAG_AUTOMATIC_CREATION:
                // Only do automatic tag creation, 1 tag will be created, 2 snapshot is data write
                // and full-compact.
                expectTable(table, snapshotManager, 2, 2, 1, "20250101");
                // Snapshot 1 is APPEND, data write.
                assertThat(snapshotManager.snapshot(1).commitKind())
                        .isEqualTo(Snapshot.CommitKind.APPEND);
                // Snapshot 2 is Compact, full compact.
                assertThat(snapshotManager.snapshot(2).commitKind())
                        .isEqualTo(Snapshot.CommitKind.COMPACT);
        }
    }

    @Timeout(value = TIMEOUT)
    @ParameterizedTest
    @ValueSource(strings = {"write-only", "do-all", "skip-all"})
    public void testSkipOrDoAllWriteActions(String action) throws Exception {

        // If write-only is true, the option of write-actions should be ignored.
        HashMap<String, String> options =
                createOptions(
                        action.equals("do-all") ? "all" : action.equals("skip-all") ? "" : "all");

        if (action.equals("write-only")) {
            options.put(CoreOptions.WRITE_ONLY.key(), "true");
        }

        createPrimaryKeyTable("T", options);
        sql("INSERT INTO T VALUES ('HXH', '20250101')");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        switch (action) {
            case "do-all":
                // Test case for no actions being skipped. (write-only is false)
                expectTable(table, snapshotManager, 2, 3, 1, null);
                // snapshot 2 is compact.
                assertThat(snapshotManager.snapshot(2).commitKind())
                        .isEqualTo(Snapshot.CommitKind.COMPACT);
                // partition expired.
                assertThat(snapshotManager.snapshot(3).commitKind())
                        .isEqualTo(Snapshot.CommitKind.OVERWRITE);
                break;

            case "write-only":
                // no compact, no expire, no tag.
                expectTable(table, snapshotManager, 1, 1, 0, "20250101");
                // only data write.
                assertThat(snapshotManager.latestSnapshot().commitKind())
                        .isEqualTo(Snapshot.CommitKind.APPEND);
                break;

            case "skip-all":
                // Even though write-only is false, all actions still skipped.
                expectTable(table, snapshotManager, 1, 1, 0, "20250101");
                //  only data write.
                assertThat(snapshotManager.latestSnapshot().commitKind())
                        .isEqualTo(Snapshot.CommitKind.APPEND);
                break;
        }
    }

    @Timeout(value = TIMEOUT)
    @ParameterizedTest
    @ValueSource(strings = {"FULL-COMPACT", "MINOR-COMPACT"})
    public void testAppendOnlyCompactActions(String val) throws Exception {

        CoreOptions.WriteAction writeAction =
                CoreOptions.WriteAction.valueOf(val.replace("-", "_"));

        HashMap<String, String> writeActionOptions = createOptions(writeAction.toString());
        writeActionOptions.put(CoreOptions.COMPACTION_MAX_FILE_NUM.key(), "4");

        createAppendOnlyTable("T", writeActionOptions);
        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        sql("INSERT INTO T VALUES ('HXH', '20250101')");
        sql("INSERT INTO T VALUES ('HXH', '20250101')");
        sql("INSERT INTO T VALUES ('HXH', '20250101')");

        switch (writeAction) {
            case FULL_COMPACT:
                // Trigger full compaction.
                expectTable(table, snapshotManager, 4, 4, 0, "20250101");
                break;
            case MINOR_COMPACT:
                // Will not trigger full compact because we skip it.
                expectTable(table, snapshotManager, 3, 3, 0, "20250101");
                // Trigger minor compact.
                sql("INSERT INTO T VALUES ('HXH', '20250101')");
                expectTable(table, snapshotManager, 5, 5, 0, "20250101");
        }
    }

    @Timeout(value = TIMEOUT)
    @ParameterizedTest
    @ValueSource(strings = {"FULL-COMPACT", "MINOR-COMPACT"})
    public void testPrimaryKeyTableCompactActions(String val) throws Exception {

        CoreOptions.WriteAction writeAction =
                CoreOptions.WriteAction.valueOf(val.replace("-", "_"));

        HashMap<String, String> writeActionOptions = createOptions(writeAction.toString());

        // Ensure that a single data write does not trigger a minor compaction.
        writeActionOptions.put(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "2");

        createPrimaryKeyTable("T", writeActionOptions);
        sql("INSERT INTO T VALUES ('HXH', '20250101')");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        switch (writeAction) {
            case FULL_COMPACT:
                // A single write will trigger full compact.
                expectTable(table, snapshotManager, 2, 2, 0, "20250101");
                // Snapshot 1 is APPEND.
                assertThat(snapshotManager.snapshot(1).commitKind())
                        .isEqualTo(Snapshot.CommitKind.APPEND);
                // Snapshot 2 is COMPACT.
                assertThat(snapshotManager.snapshot(2).commitKind())
                        .isEqualTo(Snapshot.CommitKind.COMPACT);
                break;
            case MINOR_COMPACT:
                // A single write will not trigger a minor compaction.
                expectTable(table, snapshotManager, 1, 1, 0, "20250101");
                // Snapshot 1 is APPEND.
                assertThat(snapshotManager.snapshot(1).commitKind())
                        .isEqualTo(Snapshot.CommitKind.APPEND);

                // Second write to trigger minor compact.
                sql("INSERT INTO T VALUES ('HXH2', '20250101')");
                expectTable(table, snapshotManager, 3, 3, 0, "20250101");
                // Snapshot 2 is APPEND.
                assertThat(snapshotManager.snapshot(2).commitKind())
                        .isEqualTo(Snapshot.CommitKind.APPEND);
                // Snapshot 3 is COMPACT.
                assertThat(snapshotManager.snapshot(3).commitKind())
                        .isEqualTo(Snapshot.CommitKind.COMPACT);
        }
    }

    @Test
    @Timeout(value = TIMEOUT)
    public void testSkipCreateTagWithBatchMode() throws Catalog.TableNotExistException {
        // only do partition expire.
        HashMap<String, String> options = createOptions("partition-expire");

        // Skipping tag creation will not take effect if the tag creation mode is batch.
        options.put(CoreOptions.TAG_AUTOMATIC_CREATION.key(), "batch");

        createPrimaryKeyTable("T", options);
        sql("INSERT INTO T VALUES ('a', '20250101')");
        FileStoreTable table = paimonTable("T");
        assertThat(table.tagManager().tagCount()).isEqualTo(1);
    }

    @Test
    public void testCompactProcedure() throws Catalog.TableNotExistException, IOException {
        // skip all actions, the option will be ignored.
        HashMap<String, String> writeActionOptions = createOptions("");

        createPrimaryKeyTable("T", writeActionOptions);
        sql("INSERT INTO T VALUES ('HXH', '20250101')");

        FileStoreTable table = paimonTable("T");
        SnapshotManager snapshotManager = table.snapshotManager();

        // Even though write-only is false , all actions still skipped.
        expectTable(table, snapshotManager, 1, 1, 0, "20250101");
        assertThat(snapshotManager.latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.APPEND);

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql("CALL sys.compact(`table` => 'default.T')");

        expectTable(table, snapshotManager, 2, 3, 1, null);
        // compact.
        assertThat(snapshotManager.snapshot(2).commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        // partition expired.
        assertThat(snapshotManager.snapshot(3).commitKind())
                .isEqualTo(Snapshot.CommitKind.OVERWRITE);
    }

    private HashMap<String, String> createOptions(String writeActions) {
        HashMap<String, String> options = new HashMap<>();
        // Partition expiration will be triggered every time.
        options.put(CoreOptions.PARTITION_EXPIRATION_TIME.key(), "1 d");
        options.put(CoreOptions.PARTITION_EXPIRATION_CHECK_INTERVAL.key(), "0 s");
        options.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyyMMdd");
        // Only keep one snapshot.
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        options.put(CoreOptions.TAG_AUTOMATIC_CREATION.key(), "process-time");
        options.put(CoreOptions.TAG_CREATION_PERIOD.key(), "daily");
        // full-compact will be triggered every time.
        options.put(CoreOptions.FULL_COMPACTION_DELTA_COMMITS.key(), "1");

        // what actions will be executed.
        if (writeActions != null) {
            options.put(CoreOptions.WRITE_ACTIONS.key(), writeActions);
        }

        return options;
    }

    private void expectTable(
            FileStoreTable table,
            SnapshotManager snapshotManager,
            long snapshotCount,
            long lastSnapshotId,
            long tagCount,
            String partition)
            throws IOException {
        assertThat(snapshotManager.snapshotCount()).isEqualTo(snapshotCount);
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(lastSnapshotId);
        assertThat(table.tagManager().tagCount()).isEqualTo(tagCount);
        if (partition == null) {
            assertThat(table.newScan().listPartitions().size()).isEqualTo(0);
        } else {
            assertThat(table.newScan().listPartitions().get(0).getString(0).toString())
                    .isEqualTo(partition);
        }
    }

    private void createPrimaryKeyTable(String tableName, HashMap<String, String> hintOptions) {

        StringBuilder sb = new StringBuilder();
        sb.append("'bucket' = '1'\n");
        hintOptions.forEach(
                (k, v) -> sb.append(",'").append(k).append("'='").append(v).append("'\n"));

        sql(
                String.format(
                        "CREATE TABLE %s ("
                                + " k STRING,"
                                + " dt STRING,"
                                + " PRIMARY KEY (k, dt) NOT ENFORCED"
                                + ") PARTITIONED BY (dt) WITH ("
                                + "%s"
                                + ")",
                        tableName, sb));
    }

    private void createAppendOnlyTable(String tableName, HashMap<String, String> hintOptions) {

        StringBuilder sb = new StringBuilder();
        sb.append("'bucket' = '1'\n");
        sb.append(",'bucket-key' = 'k'\n");
        hintOptions.forEach(
                (k, v) -> sb.append(",'").append(k).append("'='").append(v).append("'\n"));
        sql(
                String.format(
                        "CREATE TABLE %s ("
                                + " k STRING,"
                                + " dt STRING"
                                + ") PARTITIONED BY (dt) WITH ("
                                + "%s"
                                + ")",
                        tableName, sb));
    }
}
