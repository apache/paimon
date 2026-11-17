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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.snapshot.ScannerTestBase;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.NextSnapshotFetcher;
import org.apache.paimon.utils.ProcedureUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests discovering retained changelogs without read-time retention overrides. */
public class RetainedChangelogScanTest extends ScannerTestBase {

    @Override
    protected FileStoreTable createFileStoreTable() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.WRITE_ONLY, true);
        options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.INPUT);
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 1);
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 4);
        options.set(CoreOptions.CHANGELOG_NUM_RETAINED_MAX, 4);
        return createFileStoreTable(options);
    }

    @Test
    public void testRestoreAfterSnapshotRetentionOverride() throws Exception {
        writeFourCommits();
        expireSnapshots();
        FileStoreTable reloaded = FileStoreTableFactory.create(fileIO, tablePath);
        assertThat(reloaded.coreOptions().changelogLifecycleDecoupled()).isFalse();
        StreamTableScan scan = reloaded.newReadBuilder().newStreamScan();
        scan.restore(1L);
        assertHistory(reloaded, scan);
    }

    @ParameterizedTest
    @ValueSource(strings = {"timestamp", "creation-timestamp"})
    public void testRestoreAfterTimestampHistoryExpires(String start) throws Exception {
        writeFourCommits();
        expireSnapshots();
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            for (int i = 5; i <= 8; i++) {
                waitForNextCommitMillis();
                write.write(rowData(1, i, (long) i));
                commit.commit(i, write.prepareCommit(true, i));
            }
        }
        long timestamp = table.snapshotManager().snapshot(5L).timeMillis();
        table.newExpireSnapshots().config(table.coreOptions().expireConfig()).expire();
        assertThat(table.changelogManager().longLivedChangelogExists(3L)).isTrue();
        assertThat(table.snapshotManager().snapshotExists(4L)).isFalse();
        assertThat(table.changelogManager().longLivedChangelogExists(4L)).isFalse();

        Options options = new Options();
        options.set(
                start.equals("timestamp")
                        ? CoreOptions.SCAN_TIMESTAMP_MILLIS
                        : CoreOptions.SCAN_CREATION_TIME_MILLIS,
                timestamp);
        FileStoreTable reloaded =
                FileStoreTableFactory.create(fileIO, tablePath).copy(options.toMap());
        // Fresh startup cannot resolve the original timestamp across the gap.
        assertThatThrownBy(() -> reloaded.newReadBuilder().newStreamScan().plan())
                .isInstanceOf(OutOfRangeException.class)
                .hasMessageContaining("IDs 4 to 4");

        StreamTableScan scan = reloaded.newReadBuilder().newStreamScan();
        scan.restore(7L);
        for (int i = 7; i <= 8; i++) {
            assertThat(getResult(reloaded.newRead(), scan.plan().splits()))
                    .containsExactly("+I 1|" + i + "|" + i);
        }
        assertThat(scan.checkpoint()).isEqualTo(9L);
    }

    @ParameterizedTest
    @ValueSource(strings = {"snapshot", "timestamp", "consumer"})
    public void testStartAfterSnapshotRetentionOverride(String start) throws Exception {
        writeFourCommits();
        expireSnapshots();
        Options readOptions = new Options();
        if (start.equals("snapshot")) {
            readOptions.set(CoreOptions.SCAN_SNAPSHOT_ID, 1L);
        } else if (start.equals("timestamp")) {
            readOptions.set(CoreOptions.SCAN_TIMESTAMP_MILLIS, 0L);
        } else {
            table.consumerManager().resetConsumer("reader", new Consumer(1L));
            readOptions.set(CoreOptions.CONSUMER_ID, "reader");
        }
        FileStoreTable reloaded =
                FileStoreTableFactory.create(fileIO, tablePath).copy(readOptions.toMap());
        StreamTableScan scan = reloaded.newReadBuilder().newStreamScan();
        // Starting scanners select the next snapshot before planning its changes.
        assertThat(scan.plan().splits()).isEmpty();
        assertHistory(reloaded, scan);
    }

    @Test
    public void testStartFromCreationTimestamp() throws Exception {
        writeFourCommits();
        long timestamp = table.snapshotManager().snapshot(2L).timeMillis();
        assertThat(timestamp).isGreaterThan(table.snapshotManager().snapshot(1L).timeMillis());
        expireSnapshots();
        Options options = new Options();
        options.set(CoreOptions.SCAN_CREATION_TIME_MILLIS, timestamp);
        FileStoreTable reloaded =
                FileStoreTableFactory.create(fileIO, tablePath).copy(options.toMap());
        StreamTableScan scan = reloaded.newReadBuilder().newStreamScan();
        assertThat(scan.plan().splits()).isEmpty();
        for (int i = 2; i <= 4; i++) {
            assertThat(getResult(reloaded.newRead(), scan.plan().splits()))
                    .containsExactly("+I 1|" + i + "|" + i);
        }
        assertThat(scan.checkpoint()).isEqualTo(5L);
    }

    @ParameterizedTest
    @CsvSource({"true,0", "true,-1", "false,0", "false,-1"})
    public void testCreationTimestampAtEarliestHistory(boolean expire, long timeOffset)
            throws Exception {
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            for (int i = 1; i <= 4; i++) {
                waitForNextCommitMillis();
                write.write(rowData(1, 1, (long) i));
                commit.commit(i, write.prepareCommit(true, i));
            }
        }
        long timestamp = table.snapshotManager().snapshot(1L).timeMillis();
        if (expire) {
            expireSnapshots();
            assertThat(table.snapshotManager().snapshotExists(1L)).isFalse();
            assertThat(table.changelogManager().longLivedChangelogExists(1L)).isTrue();
        }
        Options options = new Options();
        options.set(CoreOptions.SCAN_CREATION_TIME_MILLIS, timestamp + timeOffset);
        FileStoreTable reloaded =
                FileStoreTableFactory.create(fileIO, tablePath).copy(options.toMap());
        StreamTableScan scan = reloaded.newReadBuilder().newStreamScan();
        if (timeOffset < 0) {
            // A strictly older timestamp still falls back to scanning current table files.
            assertThat(getResult(reloaded.newRead(), scan.plan().splits()))
                    .containsExactly("+I 1|1|4");
        } else {
            assertThat(scan.plan().splits()).isEmpty();
            for (int i = 1; i <= 4; i++) {
                assertThat(getResult(reloaded.newRead(), scan.plan().splits()))
                        .containsExactly("+I 1|1|" + i);
            }
        }
        assertThat(scan.checkpoint()).isEqualTo(5L);
    }

    @ParameterizedTest
    @CsvSource({
        "true,1,4", "true,2,4", "true,3,3", "true,4,4",
        "false,1,4", "false,2,4", "false,3,3", "false,4,4"
    })
    public void testBatchCreationTimestampUsesSnapshotHistory(
            boolean independent, int requestedId, int expectedValue) throws Exception {
        Options tableOptions = new Options(table.options());
        tableOptions.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 2);
        tableOptions.set(CoreOptions.CHANGELOG_NUM_RETAINED_MAX, independent ? 4 : 2);
        tablePath = new Path(tablePath, "batch");
        table = createFileStoreTable(tableOptions);
        long[] timestamps = new long[4];
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            for (int i = 1; i <= 4; i++) {
                waitForNextCommitMillis();
                write.write(rowData(1, 1, (long) i));
                commit.commit(i, write.prepareCommit(true, i));
                timestamps[i - 1] = table.snapshotManager().snapshot(i).timeMillis();
            }
        }
        table.newExpireSnapshots().config(table.coreOptions().expireConfig()).expire();
        assertThat(table.snapshotManager().earliestSnapshotId()).isEqualTo(3L);
        assertThat(table.snapshotManager().snapshotExists(1L)).isFalse();
        assertThat(table.changelogManager().longLivedChangelogExists(1L)).isEqualTo(independent);

        Options readOptions = new Options();
        readOptions.set(CoreOptions.SCAN_CREATION_TIME_MILLIS, timestamps[requestedId - 1]);
        FileStoreTable reloaded =
                FileStoreTableFactory.create(fileIO, tablePath).copy(readOptions.toMap());
        assertThat(reloaded.coreOptions().changelogLifecycleDecoupled()).isEqualTo(independent);
        // Times before snapshot 3 use the current files. Within the snapshot range, read
        // the selected full snapshot, regardless of whether older changelogs are retained.
        assertThat(getResult(reloaded.newRead(), reloaded.newScan().plan().splits()))
                .containsExactly("+I 1|1|" + expectedValue);
    }

    @ParameterizedTest
    @CsvSource({
        "timestamp,false,5,8",
        "creation-timestamp,false,5,8",
        "timestamp,true,5,8",
        "creation-timestamp,true,5,8",
        "timestamp,false,6,8",
        "timestamp,true,6,8",
        "creation-timestamp,false,6,8",
        "creation-timestamp,true,6,8",
        "snapshot,false,5,8",
        "snapshot,true,5,8",
        "timestamp,false,2,8",
        "timestamp,true,2,8",
        "timestamp,false,4,8",
        "timestamp,true,4,8",
        "creation-timestamp,false,4,8",
        "creation-timestamp,true,4,8",
        "timestamp,false,7,9",
        "timestamp,true,7,9"
    })
    public void testStartAfterMultipleExpirations(
            String start, boolean independentAgain, int requestedId, int lastId) throws Exception {
        writeFourCommits();
        expireSnapshots();
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            for (int i = 5; i <= lastId; i++) {
                waitForNextCommitMillis();
                write.write(rowData(1, i, (long) i));
                commit.commit(i, write.prepareCommit(true, i));
            }
        }
        long timestamp =
                requestedId < 4
                        ? table.changelogManager().changelog(requestedId).timeMillis()
                        : table.snapshotManager().snapshot(requestedId).timeMillis();
        table.newExpireSnapshots().config(table.coreOptions().expireConfig()).expire();
        if (independentAgain) {
            expireSnapshots();
        }
        assertThat(table.changelogManager().longLivedChangelogExists(1L)).isTrue();
        assertThat(table.snapshotManager().snapshotExists(4L)).isFalse();
        assertThat(table.changelogManager().longLivedChangelogExists(4L)).isFalse();
        Options options = new Options();
        if (start.equals("snapshot")) {
            options.set(CoreOptions.SCAN_SNAPSHOT_ID, (long) requestedId);
        } else {
            options.set(
                    start.equals("timestamp")
                            ? CoreOptions.SCAN_TIMESTAMP_MILLIS
                            : CoreOptions.SCAN_CREATION_TIME_MILLIS,
                    timestamp);
        }
        FileStoreTable reloaded =
                FileStoreTableFactory.create(fileIO, tablePath).copy(options.toMap());
        StreamTableScan scan = reloaded.newReadBuilder().newStreamScan();
        if (!start.equals("snapshot") && (requestedId == 4 || requestedId == 5)) {
            // The missing ID 4 may belong to the requested time, even when ID 5 matches it.
            assertThatThrownBy(scan::plan)
                    .isInstanceOf(OutOfRangeException.class)
                    .hasMessageContaining("IDs 4 to 4");
            return;
        }
        assertThat(scan.plan().splits()).isEmpty();
        int lastReadableId = requestedId <= 4 ? 3 : lastId;
        for (int i = requestedId; i <= lastReadableId; i++) {
            assertThat(getResult(reloaded.newRead(), scan.plan().splits()))
                    .containsExactly("+I 1|" + i + "|" + i);
        }
        if (requestedId <= 4) {
            assertExpired(scan, 4L);
        } else {
            assertThat(scan.checkpoint()).isEqualTo((long) lastId + 1);
        }
    }

    @ParameterizedTest
    @CsvSource({
        "timestamp,false,true",
        "timestamp,true,true",
        "creation-timestamp,false,true",
        "creation-timestamp,true,true",
        "timestamp,false,false",
        "creation-timestamp,false,false"
    })
    public void testTimestampWithEqualCommitTimes(String start, boolean independent, boolean gap)
            throws Exception {
        writeFourCommits();
        long timestamp = table.snapshotManager().snapshot(4L).timeMillis();
        expireSnapshots();
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            for (int i = 5; i <= 8; i++) {
                write.write(rowData(1, i, (long) i));
                commit.commit(i, write.prepareCommit(true, i));
            }
        }
        // Commit timestamps have millisecond precision and need not be distinct.
        Path snapshotPath = table.snapshotManager().snapshotPath(5L);
        ObjectNode snapshot =
                (ObjectNode)
                        JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(
                                fileIO.readFileUtf8(snapshotPath));
        snapshot.put("timeMillis", timestamp);
        fileIO.overwriteFileUtf8(
                snapshotPath, JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.writeValueAsString(snapshot));
        table = FileStoreTableFactory.create(fileIO, tablePath);
        assertThat(table.snapshotManager().snapshot(5L).timeMillis()).isEqualTo(timestamp);
        if (gap) {
            table.newExpireSnapshots().config(table.coreOptions().expireConfig()).expire();
            assertThat(table.snapshotManager().snapshotExists(4L)).isFalse();
            assertThat(table.changelogManager().longLivedChangelogExists(4L)).isFalse();
        }
        Options options = new Options();
        options.set(
                start.equals("timestamp")
                        ? CoreOptions.SCAN_TIMESTAMP_MILLIS
                        : CoreOptions.SCAN_CREATION_TIME_MILLIS,
                timestamp);
        if (independent) {
            options.set(CoreOptions.CHANGELOG_NUM_RETAINED_MAX, 5);
        }
        FileStoreTable reloaded = table.copy(options.toMap());
        StreamTableScan scan = reloaded.newReadBuilder().newStreamScan();
        if (gap) {
            assertThatThrownBy(scan::plan)
                    .isInstanceOf(OutOfRangeException.class)
                    .hasMessageContaining("Cannot determine the starting snapshot")
                    .hasMessageContaining("IDs 4 to 4");
        } else {
            assertThat(scan.plan().splits()).isEmpty();
            for (int i = 4; i <= 8; i++) {
                assertThat(getResult(reloaded.newRead(), scan.plan().splits()))
                        .containsExactly("+I 1|" + i + "|" + i);
            }
            assertThat(scan.checkpoint()).isEqualTo(9L);
        }
    }

    @Test
    public void testNoIndependentRetention() throws Exception {
        Options options = new Options(table.options());
        options.remove(CoreOptions.CHANGELOG_NUM_RETAINED_MAX);
        table = createFileStoreTable(true, options, new Path(tablePath, "coupled"));
        writeFourCommits();
        expireSnapshots();
        assertThat(table.changelogManager().earliestLongLivedChangelogId()).isNull();
        StreamTableScan scan = table.newReadBuilder().newStreamScan();
        scan.restore(1L);
        assertExpired(scan, 1L);
    }

    @Test
    public void testMissingChangelogDoesNotSkipHistory() throws Exception {
        writeFourCommits();
        expireSnapshots();
        fileIO.delete(table.changelogManager().longLivedChangelogPath(2L), false);
        StreamTableScan scan = table.newReadBuilder().newStreamScan();
        scan.restore(1L);
        assertThat(getResult(table.newRead(), scan.plan().splits())).containsExactly("+I 1|1|1");
        assertExpired(scan, 2L);
    }

    @Test
    public void testMissingChangelogFileFailsReading() throws Exception {
        writeFourCommits();
        expireSnapshots();
        StreamTableScan scan = table.newReadBuilder().newStreamScan();
        scan.restore(1L);
        List<Split> splits = scan.plan().splits();
        DataSplit split = (DataSplit) splits.get(0);
        fileIO.delete(new Path(split.bucketPath(), split.dataFiles().get(0).fileName()), false);
        assertThatThrownBy(() -> getResult(table.newRead(), splits))
                .hasStackTraceContaining("FileNotFoundException");
    }

    @Test
    public void testEmptyChangelogCanBeSkipped() throws Exception {
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser).ignoreEmptyCommit(false)) {
            write.write(rowData(1, 1, 1L));
            commit.commit(1, write.prepareCommit(true, 1));
            commit.commit(2, Collections.emptyList());
            write.write(rowData(1, 3, 3L));
            commit.commit(3, write.prepareCommit(true, 3));
        }
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(3L);
        expireSnapshots();
        assertThat(table.changelogManager().longLivedChangelogExists(2L)).isTrue();
        StreamTableScan scan = table.newReadBuilder().newStreamScan();
        scan.restore(1L);
        assertThat(getResult(table.newRead(), scan.plan().splits())).containsExactly("+I 1|1|1");
        assertThat(getResult(table.newRead(), scan.plan().splits())).containsExactly("+I 1|3|3");
        assertThat(scan.checkpoint()).isEqualTo(4L);
    }

    @ParameterizedTest
    @EnumSource(
            value = CoreOptions.ChangelogProducer.class,
            names = {"FULL_COMPACTION", "LOOKUP"})
    public void testCompactionProducedChangelog(CoreOptions.ChangelogProducer producer)
            throws Exception {
        Options options = new Options(table.options());
        options.set(CoreOptions.CHANGELOG_PRODUCER, producer);
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 20);
        options.set(CoreOptions.CHANGELOG_NUM_RETAINED_MAX, 20);
        options.set(CoreOptions.WRITE_ONLY, false);
        tablePath = new Path(tablePath, producer.toString());
        table = createFileStoreTable(true, options, tablePath);
        try (IOManager ioManager = IOManager.create(tempDir.resolve("io").toString());
                StreamTableWrite write = table.newWrite(commitUser).withIOManager(ioManager);
                StreamTableCommit commit = table.newCommit(commitUser).ignoreEmptyCommit(false)) {
            for (int i = 1; i <= 4; i++) {
                write.write(rowData(1, i, (long) i));
                commit.commit(i, write.prepareCommit(true, i));
            }
            write.compact(binaryRow(1), 0, true);
            commit.commit(5, write.prepareCommit(true, 5));
            commit.commit(6, Collections.emptyList());
        }
        if (producer == CoreOptions.ChangelogProducer.FULL_COMPACTION) {
            assertThat(table.snapshotManager().snapshot(1L).changelogManifestList()).isNull();
        }
        expireSnapshots();
        FileStoreTable reloaded = FileStoreTableFactory.create(fileIO, tablePath);
        assertThat(reloaded.coreOptions().changelogLifecycleDecoupled()).isFalse();
        StreamTableScan scan = reloaded.newReadBuilder().newStreamScan();
        scan.restore(1L);
        List<String> rows = new ArrayList<>();
        long latest = table.snapshotManager().latestSnapshotId();
        for (int i = 0; i < latest && scan.checkpoint() <= latest; i++) {
            rows.addAll(getResult(reloaded.newRead(), scan.plan().splits()));
        }
        assertThat(rows).containsExactlyInAnyOrder("+I 1|1|1", "+I 1|2|2", "+I 1|3|3", "+I 1|4|4");
        assertThat(scan.checkpoint()).isEqualTo(latest + 1);
    }

    @Test
    public void testFullReadsStillUseSnapshots() throws Exception {
        writeFourCommits();
        expireSnapshots();
        assertThat(getResult(table.newRead(), table.newScan().plan().splits()))
                .containsExactlyInAnyOrder("+I 1|1|1", "+I 1|2|2", "+I 1|3|3", "+I 1|4|4");
        Options options = new Options();
        options.set(CoreOptions.SCAN_MODE, CoreOptions.StartupMode.FROM_SNAPSHOT_FULL);
        options.set(CoreOptions.SCAN_SNAPSHOT_ID, 1L);
        StreamTableScan scan = table.copy(options.toMap()).newReadBuilder().newStreamScan();
        // Full startup continues to use the earliest available snapshot, not changelog 1.
        assertThat(getResult(table.newRead(), scan.plan().splits()))
                .containsExactlyInAnyOrder("+I 1|1|1", "+I 1|2|2", "+I 1|3|3", "+I 1|4|4");
        assertThat(scan.checkpoint()).isEqualTo(5L);
    }

    @ParameterizedTest
    @ValueSource(strings = {"overwrite", "file-monitor", "compact-bucket-table", "none"})
    public void testScansRequiringSnapshotFilesDoNotDiscoverChangelogs(String mode)
            throws Exception {
        writeFourCommits();
        expireSnapshots();
        Options options = new Options();
        if (mode.equals("overwrite")) {
            options.set(CoreOptions.STREAMING_READ_OVERWRITE, true);
        } else if (mode.equals("none")) {
            options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.NONE);
        } else {
            options.set(CoreOptions.STREAM_SCAN_MODE.key(), mode);
        }
        StreamTableScan scan = table.copy(options.toMap()).newReadBuilder().newStreamScan();
        scan.restore(1L);
        assertExpired(scan, 1L);
    }

    @Test
    public void testNormalSnapshotsAndNewCommits() throws Exception {
        writeFourCommits();
        StreamTableScan scan = table.newReadBuilder().newStreamScan();
        scan.restore(1L);
        assertHistory(table, scan);
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser).ignoreEmptyCommit(false)) {
            write.write(rowData(1, 5, 5L));
            commit.commit(5, write.prepareCommit(true, 5));
        }
        assertThat(getResult(table.newRead(), scan.plan().splits())).containsExactly("+I 1|5|5");
        assertThat(scan.checkpoint()).isEqualTo(6L);
    }

    private void assertExpired(StreamTableScan scan, long nextSnapshot) {
        assertThatThrownBy(
                        () -> {
                            for (int i = 0; i < NextSnapshotFetcher.RANGE_CHECK_INTERVAL; i++) {
                                assertThat(scan.plan().splits()).isEmpty();
                            }
                        })
                .isInstanceOf(OutOfRangeException.class);
        assertThat(scan.checkpoint()).isEqualTo(nextSnapshot);
    }

    private void writeFourCommits() throws Exception {
        try (StreamTableWrite write = table.newWrite(commitUser);
                StreamTableCommit commit = table.newCommit(commitUser).ignoreEmptyCommit(false)) {
            for (int i = 1; i <= 4; i++) {
                waitForNextCommitMillis();
                write.write(rowData(1, i, (long) i));
                commit.commit(i, write.prepareCommit(true, i));
            }
        }
    }

    private void waitForNextCommitMillis() throws InterruptedException {
        Long latest = table.snapshotManager().latestSnapshotId();
        if (latest != null) {
            long previousTime = table.snapshotManager().snapshot(latest).timeMillis();
            // Timestamp startup tests require distinct commit times, even on a fast filesystem.
            while (System.currentTimeMillis() <= previousTime) {
                Thread.sleep(1);
            }
        }
    }

    private void expireSnapshots() {
        table.newExpireSnapshots()
                .config(
                        ProcedureUtils.fillInSnapshotOptions(
                                        table.coreOptions(), 1, null, null, null)
                                .build())
                .expire();
        assertThat(table.snapshotManager().earliestSnapshotId())
                .isEqualTo(table.snapshotManager().latestSnapshotId());
    }

    private void assertHistory(FileStoreTable readTable, StreamTableScan scan) throws Exception {
        for (int i = 1; i <= 4; i++) {
            assertThat(getResult(readTable.newRead(), scan.plan().splits()))
                    .containsExactly("+I 1|" + i + "|" + i);
        }
        assertThat(scan.checkpoint()).isEqualTo(5L);
        assertThat(scan.plan().splits()).isEmpty();
    }
}
