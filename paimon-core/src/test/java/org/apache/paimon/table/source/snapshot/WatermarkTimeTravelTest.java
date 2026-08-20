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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.SCAN_WATERMARK;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests for watermark time travel: snapshots are produced through the real commit path
 * ({@link TableCommitImpl} with {@link ManifestCommittable} watermarks, the same entry the Flink
 * committer uses) and reads go through the real batch scan link ({@code scan.watermark} &rarr;
 * {@link StaticFromWatermarkStartingScanner} &rarr; {@code SnapshotManager.laterOrEqualWatermark}).
 */
public class WatermarkTimeTravelTest extends ScannerTestBase {

    @Test
    public void testScanWatermarkWithDenseWatermarks() throws Exception {
        createAppendOnlyTableWithoutCompaction();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        // snapshots 1, 2, 3 carry watermarks 100, 200, 300
        for (int i = 0; i < 3; i++) {
            commitRow(write, commit, i, 100L * (i + 1), i);
        }

        assertThat(scanFromWatermark(50)).hasSameElementsAs(Arrays.asList("+I 1|0|0"));
        assertThat(scanFromWatermark(150))
                .hasSameElementsAs(Arrays.asList("+I 1|0|0", "+I 1|1|100"));
        assertThat(scanFromWatermark(200))
                .hasSameElementsAs(Arrays.asList("+I 1|0|0", "+I 1|1|100"));
        assertThat(scanFromWatermark(250))
                .hasSameElementsAs(Arrays.asList("+I 1|0|0", "+I 1|1|100", "+I 1|2|200"));
        assertNoSnapshotForWatermark(301);

        write.close();
        commit.close();
    }

    @Test
    public void testScanWatermarkOnTableWithoutWatermarks() throws Exception {
        createAppendOnlyTableWithoutCompaction();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        // snapshots 1, 2, 3 carry no watermark field, like any pure batch-written table
        for (int i = 0; i < 3; i++) {
            commitRow(write, commit, i, null, i);
        }

        // the defective guard unboxed the null watermark and threw a raw NullPointerException;
        // the fix makes it a clean, actionable error
        assertNoSnapshotForWatermark(100);

        write.close();
        commit.close();
    }

    @Test
    @Timeout(60) // the defective search loops forever on this layout; the fix must terminate
    public void testScanWatermarkWithInterleavedNullWatermarks() throws Exception {
        createAppendOnlyTableWithoutCompaction();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        // snapshots 1..10, then snapshots 1, 5, 10 are assigned watermarks 100, 200, 300
        for (int i = 0; i < 10; i++) {
            commitRow(write, commit, i, null, i);
        }
        patchWatermark(1, 100L);
        patchWatermark(5, 200L);
        patchWatermark(10, 300L);

        assertThat(scanFromWatermark(50)).hasSameElementsAs(Arrays.asList("+I 1|0|0"));
        assertThat(scanFromWatermark(150))
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 1|0|0",
                                "+I 1|1|100",
                                "+I 1|2|200",
                                "+I 1|3|300",
                                "+I 1|4|400"));
        assertThat(scanFromWatermark(250))
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 1|0|0",
                                "+I 1|1|100",
                                "+I 1|2|200",
                                "+I 1|3|300",
                                "+I 1|4|400",
                                "+I 1|5|500",
                                "+I 1|6|600",
                                "+I 1|7|700",
                                "+I 1|8|800",
                                "+I 1|9|900"));
        assertNoSnapshotForWatermark(301);

        write.close();
        commit.close();
    }

    @Test
    public void testScanWatermarkExactMatchWithInterleavedNullWatermarks() throws Exception {
        createAppendOnlyTableWithoutCompaction();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        // snapshots 1..5, then snapshots 1, 2, 5 are assigned watermarks 100, 150, 300
        for (int i = 0; i < 5; i++) {
            commitRow(write, commit, i, null, i);
        }
        patchWatermark(1, 100L);
        patchWatermark(2, 150L);
        patchWatermark(5, 300L);

        // the exact match is snapshot 2; the defective search returned the null-watermark
        // snapshot 3 instead, silently including commit 2's row
        assertThat(scanFromWatermark(150))
                .hasSameElementsAs(Arrays.asList("+I 1|0|0", "+I 1|1|100"));

        write.close();
        commit.close();
    }

    @Test
    public void testRollbackToWatermarkBelowMinimum() throws Exception {
        createAppendOnlyTableWithoutCompaction();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        // snapshots 1, 2, 3 carry watermarks 100, 200, 300
        for (int i = 0; i < 3; i++) {
            commitRow(write, commit, i, 100L * (i + 1), i);
        }

        // the rollback_to_watermark procedures do earlierOrEqualWatermark + checkNotNull +
        // rollbackTo. The defective inverted early-return handed them snapshot 1 (watermark
        // 100 > 50), so the procedure rolled back to it and deleted snapshots 2 and 3;
        // the fix returns null so the procedure rejects the request and the table stays intact
        SnapshotManager snapshotManager = table.snapshotManager();
        assertThat(snapshotManager.earlierOrEqualWatermark(50)).isNull();
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(3);

        // a request within range still rolls back correctly
        Snapshot target = snapshotManager.earlierOrEqualWatermark(150);
        assertThat(target.id()).isEqualTo(1);
        table.rollbackTo(target.id());
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(1);
        assertThat(getResult(table.newRead(), table.newScan().plan().splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|0|0"));

        write.close();
        commit.close();
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    /**
     * Creates an append-only table that never compacts, so the tests control the exact snapshot
     * layout: every commit produces exactly one snapshot, with sequential ids.
     */
    private void createAppendOnlyTableWithoutCompaction() throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.WRITE_ONLY, true);
        createAppendOnlyTable(conf);
    }

    /** Commits one row {@code (1, value, 100 * value)} as one snapshot with the given watermark. */
    private void commitRow(
            StreamTableWrite write,
            StreamTableCommit commit,
            long identifier,
            @Nullable Long watermark,
            int value)
            throws Exception {
        write.write(rowData(1, value, 100L * value));
        List<CommitMessage> messages = write.prepareCommit(true, identifier);
        if (watermark == null) {
            commit.commit(identifier, messages);
        } else {
            ManifestCommittable committable = new ManifestCommittable(identifier, watermark);
            messages.forEach(committable::addFileCommittable);
            ((TableCommitImpl) commit).commit(committable);
        }
    }

    private List<String> scanFromWatermark(long watermark) throws Exception {
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(SCAN_WATERMARK.key(), String.valueOf(watermark));
        List<Split> splits = table.copy(dynamicOptions).newScan().plan().splits();
        return getResult(table.newRead(), splits);
    }

    private void assertNoSnapshotForWatermark(long watermark) {
        assertThatThrownBy(() -> scanFromWatermark(watermark))
                .satisfies(
                        anyCauseMatches(
                                RuntimeException.class,
                                "There is currently no snapshot later than or equal to watermark"));
    }

    /**
     * Rewrites a snapshot file with the given watermark field set. Java commits carry the previous
     * watermark forward ({@code FileStoreCommitImpl}), so a pure-Java history can never hold
     * interleaved null watermarks; this produces on disk exactly what a mixed-engine history looks
     * like, e.g. Flink streaming commits (watermark-bearing) interleaved with paimon-rust /
     * pypaimon appends (no watermark field).
     */
    private void patchWatermark(long snapshotId, long watermark) throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot s = snapshotManager.snapshot(snapshotId);
        Snapshot patched =
                new Snapshot(
                        s.id(),
                        s.schemaId(),
                        s.baseManifestList(),
                        s.baseManifestListSize(),
                        s.deltaManifestList(),
                        s.deltaManifestListSize(),
                        s.changelogManifestList(),
                        s.changelogManifestListSize(),
                        s.indexManifest(),
                        s.commitUser(),
                        s.writerVersion(),
                        s.commitIdentifier(),
                        s.commitKind(),
                        s.timeMillis(),
                        s.totalRecordCount(),
                        s.deltaRecordCount(),
                        s.changelogRecordCount(),
                        watermark,
                        s.statistics(),
                        s.properties(),
                        s.nextRowId(),
                        s.operation());
        fileIO.delete(snapshotManager.snapshotPath(snapshotId), false);
        fileIO.tryToWriteAtomic(snapshotManager.snapshotPath(snapshotId), patched.toJson());
    }
}
