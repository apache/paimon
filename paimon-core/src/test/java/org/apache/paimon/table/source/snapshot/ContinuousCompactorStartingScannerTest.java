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
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ContinuousCompactorStartingScanner}. */
public class ContinuousCompactorStartingScannerTest extends ScannerTestBase {

    @Test
    public void testScan() throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(1, 40, 400L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 10, 101L));
        write.write(rowData(1, 30, 300L));
        write.write(rowData(1, 10, 102L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        write.compact(binaryRow(1), 0, true);
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(1, 10, 103L));
        write.write(rowData(1, 30, 301L));
        commit.commit(2, write.prepareCommit(true, 2));

        write.write(rowData(1, 20, 201L));
        write.write(rowData(1, 40, 401L));
        commit.commit(3, write.prepareCommit(true, 3));

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(5);

        ContinuousCompactorStartingScanner scanner =
                new ContinuousCompactorStartingScanner(snapshotManager);
        StartingScanner.NextSnapshot result =
                (StartingScanner.NextSnapshot) scanner.scan(snapshotReader);
        assertThat(result.nextSnapshotId()).isEqualTo(4);

        write.close();
        commit.close();
    }

    @Test
    public void testNoSnapshot() {
        SnapshotManager snapshotManager = table.snapshotManager();
        ContinuousCompactorStartingScanner scanner =
                new ContinuousCompactorStartingScanner(snapshotManager);
        assertThat(scanner.scan(snapshotReader)).isInstanceOf(StartingScanner.NoSnapshot.class);
    }

    @Test
    public void testLatestBaselineIsFollowedByDeltaScan() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.WRITE_ONLY, true);
        options.set(CoreOptions.STREAM_SCAN_MODE, CoreOptions.StreamScanMode.COMPACT_BUCKET_TABLE);
        options.set(
                CoreOptions.CONTINUOUS_COMPACTION_INITIAL_SCAN_MODE,
                CoreOptions.CompactionInitialScanMode.LATEST);
        createAppendOnlyTable(options);
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        commit.commit(0, write.prepareCommit(true, 0));
        write.write(rowData(1, 11, 101L));
        commit.commit(1, write.prepareCommit(true, 1));

        StreamTableScan scan = table.newStreamScan();
        TableScan.Plan baseline = scan.plan();
        assertThat(baseline.splits()).allMatch(split -> ((DataSplit) split).snapshotId() == 2L);
        assertThat(getResult(table.newRead(), baseline.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|10|100", "+I 1|11|101"));
        assertThat(scan.checkpoint()).isEqualTo(3L);

        write.write(rowData(1, 12, 102L));
        commit.commit(2, write.prepareCommit(true, 2));

        TableScan.Plan delta = scan.plan();
        assertThat(delta.splits()).allMatch(split -> ((DataSplit) split).snapshotId() == 3L);
        assertThat(delta.splits()).isNotEmpty();
        assertThat(getResult(table.newRead(), delta.splits())).containsExactly("+I 1|12|102");
        assertThat(scan.checkpoint()).isEqualTo(4L);

        write.close();
        commit.close();
    }

    @Test
    public void testNoCompactSnapshotLatestBaselineContainsAllPartitionsAndBuckets()
            throws Exception {
        Options options = new Options();
        options.set(CoreOptions.WRITE_ONLY, true);
        options.set(CoreOptions.BUCKET, 2);
        options.set(CoreOptions.BUCKET_KEY, "a");
        createAppendOnlyTable(options);
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 11, 101L));
        write.write(rowData(2, 10, 200L));
        write.write(rowData(2, 11, 201L));
        commit.commit(0, write.prepareCommit(true, 0));

        StartingScanner.NextSnapshot earliestResult =
                (StartingScanner.NextSnapshot)
                        new ContinuousCompactorStartingScanner(snapshotManager)
                                .scan(snapshotReader);
        assertThat(earliestResult.nextSnapshotId()).isEqualTo(1L);

        ContinuousCompactorStartingScanner scanner =
                new ContinuousCompactorStartingScanner(
                        snapshotManager, CoreOptions.CompactionInitialScanMode.LATEST);
        StartingScanner.ScannedResult result =
                (StartingScanner.ScannedResult) scanner.scan(snapshotReader);

        Set<String> partitionBuckets = new HashSet<>();
        for (Split split : result.splits()) {
            DataSplit dataSplit = (DataSplit) split;
            partitionBuckets.add(dataSplit.partition().getInt(0) + ":" + dataSplit.bucket());
        }
        assertThat(partitionBuckets).hasSize(4);
        assertThat(result.splits()).allMatch(split -> !((DataSplit) split).dataFiles().isEmpty());

        write.close();
        commit.close();
    }

    @Test
    public void testNoCompactSnapshotReadsLatestAsInitialBaseline() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.WRITE_ONLY, true);
        createAppendOnlyTable(options);
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        for (int i = 0; i < 5; i++) {
            write.write(rowData(1, i, (long) i));
            commit.commit(i, write.prepareCommit(true, i));
        }

        ContinuousCompactorStartingScanner scanner =
                new ContinuousCompactorStartingScanner(
                        snapshotManager, CoreOptions.CompactionInitialScanMode.LATEST);
        StartingScanner.ScannedResult result =
                (StartingScanner.ScannedResult) scanner.scan(snapshotReader);

        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(1L);
        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(5L);
        assertThat(snapshotManager.snapshot(5L).commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(result.currentSnapshotId()).isEqualTo(5);
        assertThat(result.plan().snapshotId()).isEqualTo(5);
        assertThat(result.splits()).isNotEmpty();
        assertThat(result.splits()).allMatch(split -> ((DataSplit) split).snapshotId() == 5);
        assertThat(result.splits()).allMatch(split -> !((DataSplit) split).dataFiles().isEmpty());

        write.close();
        commit.close();
    }
}
