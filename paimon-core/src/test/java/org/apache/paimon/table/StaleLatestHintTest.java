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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.HintFileUtils;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.OutputStreamAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for a table whose LATEST hint stays behind while commits and snapshot expiration go on, for
 * example because hint writes keep failing.
 */
public class StaleLatestHintTest {

    private static final String COMMIT_USER = "user";

    @TempDir java.nio.file.Path tempDir;

    private HintFileIO fileIO;
    private FileStoreTable table;
    private SnapshotManager snapshotManager;
    private List<CommitMessage> lastMessages;

    // ------------------------------------------------------------------------
    //  The LATEST hint stops at snapshot 1 while snapshots 2 to 7 are committed
    // ------------------------------------------------------------------------

    /**
     * Commits snapshot 1 normally, then snapshots 2 to 7 without updating the LATEST hint. Only 3
     * snapshots are retained, so expiration runs during these commits.
     */
    private void prepareStaleHint() throws Exception {
        fileIO = new HintFileIO();
        table = createTable(fileIO, "stale", false);
        snapshotManager = table.snapshotManager();
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(COMMIT_USER);
        try (StreamTableWrite write = writeBuilder.newWrite();
                StreamTableCommit commit = writeBuilder.newCommit()) {
            for (long identifier = 1L; identifier <= 7L; identifier++) {
                fileIO.skipLatestWrite = identifier > 1;
                write.write(GenericRow.of((int) identifier, (int) identifier));
                lastMessages = write.prepareCommit(false, identifier);
                commit.commit(identifier, lastMessages);
                // keep snapshot times distinct for the timestamp based scans
                Thread.sleep(5);
            }
        }
        assertThat(snapshotManager.readLatestHintStrictly()).hasValue(1L);
    }

    @Test
    public void testHintedSnapshotIsNotExpired() throws Exception {
        prepareStaleHint();
        assertThat(snapshotManager.snapshotExists(1)).isTrue();
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(7L);
        assertThat(snapshotManager.latestSnapshot().id()).isEqualTo(7L);
    }

    @Test
    public void testExpireResumesAfterHintWritesRecover() throws Exception {
        prepareStaleHint();
        fileIO.skipLatestWrite = false;
        commit(table, COMMIT_USER, 8);

        assertThat(snapshotManager.readLatestHintStrictly()).hasValue(8L);
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(8L);
        assertThat(snapshotManager.snapshotExists(1)).isFalse();
        assertThat(snapshotManager.snapshotCount()).isEqualTo(3L);
        assertThat(countRows(table)).isEqualTo(8L);
    }

    /** A committer restart replays a commit that is already in the latest snapshot. */
    @ParameterizedTest(name = "checkAppendFiles = {0}")
    @ValueSource(booleans = {true, false})
    public void testReplayCommittedCommit(boolean checkAppendFiles) throws Exception {
        prepareStaleHint();
        ManifestCommittable committable = new ManifestCommittable(7L);
        lastMessages.forEach(committable::addFileCommittable);
        try (TableCommitImpl commit = table.newCommit(COMMIT_USER)) {
            assertThat(
                            commit.filterAndCommitMultiple(
                                    Collections.singletonList(committable), checkAppendFiles))
                    .isEqualTo(0);
        }

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(7L);
        assertThat(countRows(table)).isEqualTo(7L);
    }

    @Test
    public void testStreamingReadFromLatest() throws Exception {
        prepareStaleHint();
        Map<String, String> options = new HashMap<>();
        options.put("scan.mode", "latest");
        StreamTableScan scan = table.copy(options).newStreamScan();
        scan.plan();
        assertThat(scan.checkpoint()).isEqualTo(8L);
    }

    @Test
    public void testRollback() throws Exception {
        prepareStaleHint();
        table.rollbackTo(5L);

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(5L);
        assertThat(snapshotManager.snapshotExists(6)).isFalse();
        assertThat(snapshotManager.snapshotExists(7)).isFalse();
        assertThat(countRows(table)).isEqualTo(5L);
    }

    @Test
    public void testIncrementalBetweenTimestamps() throws Exception {
        prepareStaleHint();
        long start = snapshotManager.snapshot(3).timeMillis();
        long end = snapshotManager.snapshot(5).timeMillis();
        Map<String, String> options = new HashMap<>();
        options.put("incremental-between-timestamp", start + "," + end);

        // snapshots 4 and 5 add one row each
        assertThat(countRows(table.copy(options))).isEqualTo(2L);
    }

    /** Another writer commits after expiration has read the LATEST hint. */
    @Test
    public void testCommitDuringExpiration() throws Exception {
        prepareStaleHint();
        // the other writer does not expire snapshots itself
        Map<String, String> writeOnly = new HashMap<>();
        writeOnly.put("write-only", "true");
        FileStoreTable other = createTable(LocalFileIO.create(), "stale", false).copy(writeOnly);
        fileIO.afterLatestRead =
                () -> {
                    try {
                        commit(other, "other", 8);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) table.newExpireSnapshots();
        expire.expireUntil(1, 6);

        assertThat(snapshotManager.snapshotExists(1)).isTrue();
        assertThat(snapshotManager.readLatestHintStrictly()).hasValue(8L);
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(8L);
        assertThat(countRows(table)).isEqualTo(8L);
    }

    // ------------------------------------------------------------------------
    //  Failures when writing or reading the LATEST hint
    // ------------------------------------------------------------------------

    @Test
    public void testCommitsGoOnWhileHintWritesFail() throws Exception {
        HintFileIO failingIO = new HintFileIO();
        FileStoreTable failing = createTable(failingIO, "write-failure", false);
        SnapshotManager sm = failing.snapshotManager();
        commit(failing, COMMIT_USER, 1);

        failingIO.failLatestWrite = true;
        for (long identifier = 2; identifier <= 5; identifier++) {
            commit(failing, COMMIT_USER, identifier);
        }
        assertThat(sm.readLatestHintStrictly()).hasValue(1L);
        assertThat(sm.snapshotCount()).isEqualTo(5L);
        assertThat(sm.latestSnapshotId()).isEqualTo(5L);
        assertThat(countRows(failing)).isEqualTo(5L);

        failingIO.failLatestWrite = false;
        commit(failing, COMMIT_USER, 6);
        assertThat(sm.readLatestHintStrictly()).hasValue(6L);
        assertThat(sm.snapshotCount()).isEqualTo(3L);
        assertThat(countRows(failing)).isEqualTo(6L);
    }

    @Test
    public void testUnreadableHintSkipsExpiration() throws Exception {
        HintFileIO failingIO = new HintFileIO();
        FileStoreTable writeOnly = createWriteOnlyTable(failingIO, "read-failure", 10);
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) writeOnly.newExpireSnapshots();

        failingIO.failLatestRead = true;
        assertThat(expire.expireUntil(1, 8)).isEqualTo(0);
        failingIO.failLatestRead = false;

        assertThat(writeOnly.snapshotManager().snapshotExists(1)).isTrue();
        assertEarliestHintMatchesSnapshots(writeOnly);
    }

    @Test
    public void testInvalidHintSkipsExpiration() throws Exception {
        HintFileIO io = new HintFileIO();
        FileStoreTable writeOnly = createWriteOnlyTable(io, "invalid", 10);
        SnapshotManager sm = writeOnly.snapshotManager();
        io.overwriteHintFile(new Path(sm.snapshotDirectory(), HintFileUtils.LATEST), "invalid");
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) writeOnly.newExpireSnapshots();

        assertThat(expire.expireUntil(1, 8)).isEqualTo(0);
        assertThat(sm.snapshotExists(1)).isTrue();
        assertEarliestHintMatchesSnapshots(writeOnly);
    }

    // ------------------------------------------------------------------------
    //  Boundary of the protected snapshots
    // ------------------------------------------------------------------------

    @Test
    public void testEarliestHintMatchesTheKeptSnapshots() throws Exception {
        FileStoreTable writeOnly = createWriteOnlyTable(new HintFileIO(), "earliest", 10);
        SnapshotManager sm = writeOnly.snapshotManager();
        sm.commitLatestHint(5);
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) writeOnly.newExpireSnapshots();
        expire.expireUntil(1, 8);

        assertThat(sm.snapshotExists(4)).isFalse();
        assertThat(sm.snapshotExists(5)).isTrue();
        assertThat(
                        HintFileUtils.readHint(
                                writeOnly.fileIO(), HintFileUtils.EARLIEST, sm.snapshotDirectory()))
                .isEqualTo(5L);
        assertThat(sm.earliestSnapshotId()).isEqualTo(5L);
        assertEarliestHintMatchesSnapshots(writeOnly);
    }

    /** The hinted snapshot is gone but the one after it is the earliest: keep everything. */
    @Test
    public void testHintJustBeforeEarliestKeepsTheNextSnapshot() throws Exception {
        FileStoreTable writeOnly = createWriteOnlyTable(new HintFileIO(), "before-earliest", 10);
        SnapshotManager sm = writeOnly.snapshotManager();
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) writeOnly.newExpireSnapshots();
        expire.expireUntil(1, 4);
        assertThat(sm.earliestSnapshotId()).isEqualTo(4L);

        sm.commitLatestHint(3);
        assertThat(expire.expireUntil(4, 8)).isEqualTo(0);

        assertThat(sm.snapshotExists(4)).isTrue();
        assertThat(sm.latestSnapshotId()).isEqualTo(10L);
        assertEarliestHintMatchesSnapshots(writeOnly);
    }

    /** A hint below the earliest snapshot cannot be used safely, so nothing is expired. */
    @Test
    public void testHintBelowEarliestSkipsExpiration() throws Exception {
        FileStoreTable writeOnly = createWriteOnlyTable(new HintFileIO(), "below-earliest", 10);
        SnapshotManager sm = writeOnly.snapshotManager();
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) writeOnly.newExpireSnapshots();
        expire.expireUntil(1, 5);

        sm.commitLatestHint(2);
        assertThat(expire.expireUntil(5, 8)).isEqualTo(0);
        assertThat(sm.snapshotExists(5)).isTrue();
        assertEarliestHintMatchesSnapshots(writeOnly);
    }

    /**
     * A hint on a missing snapshot in the middle keeps every snapshot, without deleting the data
     * files the older snapshots still use.
     */
    @Test
    public void testHintOnMissingSnapshot() throws Exception {
        FileStoreTable writeOnly = createWriteOnlyTable(new HintFileIO(), "gap", 10);
        SnapshotManager sm = writeOnly.snapshotManager();
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) writeOnly.newExpireSnapshots();
        sm.deleteSnapshot(4);
        sm.commitLatestHint(4);

        assertThat(expire.expireUntil(1, 8)).isEqualTo(0);
        assertThat(sm.snapshotExists(1)).isTrue();
        Map<String, String> timeTravel = new HashMap<>();
        timeTravel.put("scan.snapshot-id", "3");
        assertThat(countRows(writeOnly.copy(timeTravel))).isEqualTo(3L);
        assertThat(sm.latestSnapshotId()).isEqualTo(10L);

        // once a commit writes the hint again, expiration goes on
        commit(writeOnly, COMMIT_USER, 11);
        assertThat(sm.readLatestHintStrictly()).hasValue(11L);
        expire.expireUntil(sm.earliestSnapshotId(), 8);
        assertThat(sm.snapshotExists(7)).isFalse();
        assertThat(sm.latestSnapshotId()).isEqualTo(11L);
    }

    /**
     * On a primary key table, compactions delete data files that older snapshots still use. A hint
     * on a missing snapshot must not let expiration delete any of them.
     */
    @Test
    public void testHintOnMissingSnapshotKeepsDataFilesOfOlderSnapshots() throws Exception {
        LocalFileIO localFileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toString(), "pk-gap");
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("k")
                        .option("bucket", "1")
                        .option("num-sorted-run.compaction-trigger", "2")
                        .build();
        TableSchema tableSchema =
                new FileSystemSchemaManager(localFileIO, tablePath).createTable(schema);
        FileStoreTable pkTable =
                FileStoreTableFactory.create(
                        localFileIO, tablePath, tableSchema, CatalogEnvironment.empty());
        StreamWriteBuilder writeBuilder =
                pkTable.newStreamWriteBuilder().withCommitUser(COMMIT_USER);
        try (StreamTableWrite write = writeBuilder.newWrite();
                StreamTableCommit commit = writeBuilder.newCommit()) {
            for (long identifier = 1L; identifier <= 10L; identifier++) {
                write.write(GenericRow.of((int) identifier, (int) identifier));
                commit.commit(identifier, write.prepareCommit(true, identifier));
            }
        }
        SnapshotManager sm = pkTable.snapshotManager();
        List<Long> ids = sm.snapshotIdStream().sorted().collect(Collectors.toList());
        long gap = ids.get(ids.size() / 2);
        // a compaction before the gap has deleted data files that older snapshots use
        assertThat(ids.stream().filter(id -> id < gap).map(sm::snapshot))
                .anyMatch(snapshot -> snapshot.commitKind() == Snapshot.CommitKind.COMPACT);
        long beforeGap = gap - 1;
        long rowsBeforeGap = countRows(timeTravel(pkTable, beforeGap));

        sm.deleteSnapshot(gap);
        sm.commitLatestHint(gap);
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) pkTable.newExpireSnapshots();
        assertThat(expire.expireUntil(ids.get(0), ids.get(ids.size() - 1))).isEqualTo(0);

        assertThat(countRows(timeTravel(pkTable, beforeGap))).isEqualTo(rowsBeforeGap);
        assertThat(countRows(timeTravel(pkTable, ids.get(0)))).isGreaterThan(0L);
    }

    private static FileStoreTable timeTravel(FileStoreTable table, long snapshotId) {
        Map<String, String> options = new HashMap<>();
        options.put("scan.snapshot-id", String.valueOf(snapshotId));
        return table.copy(options);
    }

    /** A branch keeps its own LATEST hint, and expiration on the branch protects that one. */
    @Test
    public void testHintStopsMovingOnBranch() throws Exception {
        HintFileIO branchIO = new HintFileIO();
        FileStoreTable main = createTable(branchIO, "branch", false);
        commit(main, COMMIT_USER, 1);
        main.createBranch("b");
        FileStoreTable branch = main.switchToBranch("b");
        SnapshotManager branchSnapshots = branch.snapshotManager();

        commit(branch, COMMIT_USER, 1);
        branchIO.skipLatestWrite = true;
        for (long identifier = 2L; identifier <= 7L; identifier++) {
            commit(branch, COMMIT_USER, identifier);
        }
        assertThat(branchSnapshots.readLatestHintStrictly()).hasValue(1L);
        assertThat(branchSnapshots.snapshotExists(1)).isTrue();
        assertThat(branchSnapshots.latestSnapshotId()).isEqualTo(7L);
        assertThat(countRows(branch)).isEqualTo(7L);
        assertThat(main.snapshotManager().latestSnapshotId()).isEqualTo(1L);

        branchIO.skipLatestWrite = false;
        commit(branch, COMMIT_USER, 8);
        assertThat(branchSnapshots.readLatestHintStrictly()).hasValue(8L);
        assertThat(branchSnapshots.snapshotCount()).isEqualTo(3L);
        assertThat(main.snapshotManager().latestSnapshotId()).isEqualTo(1L);
    }

    /** When the catalog manages the snapshots, the LATEST file does not limit expiration. */
    @Test
    public void testCatalogManagedSnapshotsIgnoreTheHintFile() throws Exception {
        LocalFileIO localFileIO = LocalFileIO.create();
        FileStoreTable writeOnly = createWriteOnlyTable(localFileIO, "catalog-managed", 10);
        writeOnly.snapshotManager().commitLatestHint(2);

        CatalogLoader catalogLoader =
                () -> {
                    throw new UnsupportedOperationException();
                };
        CatalogEnvironment catalogManaged =
                new CatalogEnvironment(
                        Identifier.create("db", "t"),
                        null,
                        catalogLoader,
                        null,
                        null,
                        null,
                        true,
                        false);
        FileStoreTable table =
                FileStoreTableFactory.create(
                        localFileIO, writeOnly.location(), writeOnly.schema(), catalogManaged);
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) table.newExpireSnapshots();
        expire.expireUntil(1, 5);

        assertThat(writeOnly.snapshotManager().snapshotExists(4)).isFalse();
        assertThat(writeOnly.snapshotManager().snapshotExists(5)).isTrue();
    }

    // ------------------------------------------------------------------------
    //  Expiration paths, IO and concurrency
    // ------------------------------------------------------------------------

    /**
     * Expiration either stops early at the first snapshot within {@code snapshot.time-retained} or
     * goes up to the retention limit. Both paths must keep the hinted snapshot and the data of the
     * snapshots kept.
     */
    @ParameterizedTest(name = "timeRetain = {0}")
    @ValueSource(strings = {"PT1H", "PT0.001S"})
    public void testBothExpirationPathsKeepTheHintedSnapshot(String timeRetain) throws Exception {
        FileStoreTable writeOnly = createWriteOnlyTable(new HintFileIO(), "paths", 10);
        SnapshotManager sm = writeOnly.snapshotManager();
        sm.commitLatestHint(3);
        Thread.sleep(5);

        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) writeOnly.newExpireSnapshots();
        expire.config(
                        ExpireConfig.builder()
                                .snapshotRetainMin(1)
                                .snapshotRetainMax(3)
                                .snapshotTimeRetain(Duration.parse(timeRetain))
                                .snapshotMaxDeletes(Integer.MAX_VALUE)
                                .build())
                .expire();

        assertThat(sm.snapshotExists(2)).isFalse();
        assertThat(sm.snapshotExists(3)).isTrue();
        assertThat(countRows(timeTravel(writeOnly, 3))).isEqualTo(3L);
        assertEarliestHintMatchesSnapshots(writeOnly);
    }

    /**
     * With an up-to-date hint, the protection adds one read of the LATEST hint and no snapshot file
     * check, compared to expiration without it.
     */
    @Test
    public void testOnlyOneHintReadWithUpToDateHint() throws Exception {
        HintFileIO protectedIO = new HintFileIO();
        FileStoreTable protectedTable = createWriteOnlyTable(protectedIO, "io-protected", 10);
        HintFileIO plainIO = new HintFileIO();
        FileStoreTable plainTable = createWriteOnlyTable(plainIO, "io-plain", 10);
        ExpireSnapshotsImpl protectedExpire =
                (ExpireSnapshotsImpl) protectedTable.newExpireSnapshots();
        ExpireSnapshotsImpl plainExpire = withoutProtection(plainTable);

        protectedIO.resetCounters();
        plainIO.resetCounters();
        protectedExpire.expireUntil(1, 5);
        plainExpire.expireUntil(1, 5);

        assertThat(protectedIO.latestReads.get()).isEqualTo(plainIO.latestReads.get() + 1);
        assertThat(protectedIO.snapshotExistsChecks.get())
                .isEqualTo(plainIO.snapshotExistsChecks.get());
        assertThat(protectedTable.snapshotManager().snapshotExists(4)).isFalse();
    }

    private static ExpireSnapshotsImpl withoutProtection(FileStoreTable table) {
        return new ExpireSnapshotsImpl(
                table.snapshotManager(),
                table.changelogManager(),
                table.store().newSnapshotDeletion(),
                table.store().newTagManager(),
                null,
                false);
    }

    /** Another job rolls back after expiration has read the LATEST hint. */
    @Test
    public void testRollbackDuringExpiration() throws Exception {
        prepareStaleHint();
        FileStoreTable other = createTable(LocalFileIO.create(), "stale", false);
        fileIO.afterLatestRead = () -> other.rollbackTo(5L);
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) table.newExpireSnapshots();
        expire.expireUntil(1, 6);

        assertThat(snapshotManager.readLatestHintStrictly()).hasValue(5L);
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(5L);
        assertThat(snapshotManager.snapshotExists(6)).isFalse();
        assertThat(countRows(table)).isEqualTo(5L);
        commit(other, "other", 8);
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(6L);
    }

    // ------------------------------------------------------------------------
    //  Warnings
    // ------------------------------------------------------------------------

    @Test
    public void testExpirationWarnsOncePerHint() throws Exception {
        FileStoreTable writeOnly = createWriteOnlyTable(new HintFileIO(), "warn", 10);
        writeOnly.snapshotManager().commitLatestHint(5);
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) writeOnly.newExpireSnapshots();

        String logs =
                captureLogs(
                        ExpireSnapshotsImpl.class,
                        () -> {
                            expire.expireUntil(1, 8);
                            expire.expireUntil(5, 8);
                        });

        assertThat(logs)
                .contains("The LATEST hint 5")
                .contains("latest snapshot 10")
                .contains("Keeping snapshot 5 and later ones instead of expiring up to 8");
        assertThat(logs.split("The LATEST hint 5", -1)).hasSize(2);
    }

    @Test
    public void testRetryWarningOnlyWhenTheCommitIsFoundCommitted() throws Exception {
        HintFileIO failingIO = new HintFileIO();
        FileStoreTable writeOnly = createTable(failingIO, "retry-warning", true);

        String normal = captureLogs(FileStoreCommitImpl.class, () -> commit(writeOnly, "u", 1));
        assertThat(normal).doesNotContain("may not have been updated");

        failingIO.failLatestWrite = true;
        String retried = captureLogs(FileStoreCommitImpl.class, () -> commit(writeOnly, "u", 2));
        assertThat(retried)
                .contains("Snapshot #2 of table")
                .contains("The LATEST hint may not have been updated");
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private FileStoreTable createTable(LocalFileIO localFileIO, String name, boolean writeOnly)
            throws Exception {
        Path tablePath = new Path(tempDir.toString(), name);
        Optional<TableSchema> existing =
                new FileSystemSchemaManager(localFileIO, tablePath).latest();
        TableSchema tableSchema;
        if (existing.isPresent()) {
            tableSchema = existing.get();
        } else {
            Schema.Builder schema =
                    Schema.newBuilder()
                            .column("k", DataTypes.INT())
                            .column("v", DataTypes.INT())
                            .option("bucket", "-1")
                            .option("snapshot.num-retained.min", "1")
                            .option("snapshot.num-retained.max", "3");
            if (writeOnly) {
                schema.option("write-only", "true");
            }
            tableSchema =
                    new FileSystemSchemaManager(localFileIO, tablePath).createTable(schema.build());
        }
        return FileStoreTableFactory.create(
                localFileIO, tablePath, tableSchema, CatalogEnvironment.empty());
    }

    /** A write-only table, so commits do not expire snapshots, with snapshots 1 to n. */
    private FileStoreTable createWriteOnlyTable(LocalFileIO localFileIO, String name, int n)
            throws Exception {
        FileStoreTable writeOnly = createTable(localFileIO, name, true);
        for (long identifier = 1L; identifier <= n; identifier++) {
            commit(writeOnly, COMMIT_USER, identifier);
        }
        return writeOnly;
    }

    private static void commit(FileStoreTable table, String user, long identifier)
            throws Exception {
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(user);
        try (StreamTableWrite write = writeBuilder.newWrite();
                StreamTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of((int) identifier, (int) identifier));
            commit.commit(identifier, write.prepareCommit(false, identifier));
        }
    }

    private static long countRows(FileStoreTable table) throws IOException {
        ReadBuilder readBuilder = table.newReadBuilder();
        AtomicLong count = new AtomicLong();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(row -> count.incrementAndGet());
        return count.get();
    }

    private static void assertEarliestHintMatchesSnapshots(FileStoreTable table)
            throws IOException {
        SnapshotManager sm = table.snapshotManager();
        Long earliestHint =
                HintFileUtils.readHint(
                        table.fileIO(), HintFileUtils.EARLIEST, sm.snapshotDirectory());
        if (earliestHint != null) {
            assertThat(earliestHint)
                    .isEqualTo(sm.snapshotIdStream().reduce(Math::min).orElse(null));
        }
    }

    private static String captureLogs(Class<?> clazz, ThrowingRunnable action) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        OutputStreamAppender appender =
                OutputStreamAppender.newBuilder()
                        .setName("stale-latest-hint-test")
                        .setTarget(output)
                        .setLayout(PatternLayout.newBuilder().withPattern("%level %msg%n").build())
                        .build();
        Logger logger = (Logger) LogManager.getLogger(clazz);
        Level previousLevel = logger.getLevel();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
        try {
            action.run();
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(previousLevel);
            appender.stop();
        }
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** A {@link LocalFileIO} which can skip or fail writing and fail reading the LATEST hint. */
    private static class HintFileIO extends LocalFileIO {

        private final AtomicInteger latestReads = new AtomicInteger();
        private final AtomicInteger snapshotExistsChecks = new AtomicInteger();
        private volatile boolean skipLatestWrite = false;
        private volatile boolean failLatestWrite = false;
        private volatile boolean failLatestRead = false;
        private volatile ThrowingRunnable afterLatestRead;

        private void resetCounters() {
            latestReads.set(0);
            snapshotExistsChecks.set(0);
        }

        @Override
        public void overwriteHintFile(Path path, String content) throws IOException {
            if (path.getName().equals(HintFileUtils.LATEST)) {
                if (failLatestWrite) {
                    throw new IOException("Failed to write the LATEST hint");
                }
                if (skipLatestWrite) {
                    return;
                }
            }
            super.overwriteHintFile(path, content);
        }

        @Override
        public Optional<String> readOverwrittenFileUtf8(Path path) throws IOException {
            if (path.getName().equals(HintFileUtils.LATEST)) {
                latestReads.incrementAndGet();
                if (failLatestRead) {
                    throw new IOException("Failed to read the LATEST hint");
                }
                Optional<String> content = super.readOverwrittenFileUtf8(path);
                ThrowingRunnable hook = afterLatestRead;
                if (hook != null) {
                    afterLatestRead = null;
                    try {
                        hook.run();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return content;
            }
            return super.readOverwrittenFileUtf8(path);
        }

        @Override
        public boolean exists(Path path) throws IOException {
            if (path.getName().startsWith(SnapshotManager.SNAPSHOT_PREFIX)) {
                snapshotExistsChecks.incrementAndGet();
            }
            return super.exists(path);
        }
    }
}
