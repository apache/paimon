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
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ContinuousFromTimestampStartingScanner}. */
public class ContinuousFromTimestampStartingScannerTest extends ScannerTestBase {

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
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        commit.commit(1, write.prepareCommit(true, 1));

        // wait for a little while
        Thread.sleep(50);

        write.write(rowData(1, 10, 102L));
        write.write(rowData(1, 20, 201L));
        commit.commit(2, write.prepareCommit(true, 2));

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(3);
        // snapshot 3 should not be read
        long timestamp = snapshotManager.snapshot(3).timeMillis();

        ContinuousFromTimestampStartingScanner scanner =
                new ContinuousFromTimestampStartingScanner(snapshotManager, timestamp, false);
        StartingScanner.NextSnapshot result =
                (StartingScanner.NextSnapshot) scanner.scan(snapshotReader);
        assertThat(result.nextSnapshotId()).isEqualTo(3);

        write.close();
        commit.close();
    }

    @Test
    public void testNoSnapshot() {
        SnapshotManager snapshotManager = table.snapshotManager();
        ContinuousFromTimestampStartingScanner scanner =
                new ContinuousFromTimestampStartingScanner(
                        snapshotManager, System.currentTimeMillis(), false);
        assertThat(scanner.scan(snapshotReader)).isInstanceOf(StartingScanner.NoSnapshot.class);
    }

    @Test
    public void testNoSnapshotBeforeTimestamp() throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(1, 40, 400L));
        commit.commit(0, write.prepareCommit(true, 0));

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(1);
        // snapshot 1 should not be read
        long timestamp = snapshotManager.snapshot(1).timeMillis();

        ContinuousFromTimestampStartingScanner scanner =
                new ContinuousFromTimestampStartingScanner(snapshotManager, timestamp, false);
        StartingScanner.NextSnapshot result =
                (StartingScanner.NextSnapshot) scanner.scan(snapshotReader);
        // next snapshot
        assertThat(result.nextSnapshotId()).isEqualTo(1);

        write.close();
        commit.close();
    }

    @ParameterizedTest(name = "changelog-producer = {0}")
    @ValueSource(strings = {"none", "input"})
    public void testScanFromChangelog(String changelogProducer) throws Exception {
        Options options = new Options();
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 2);
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 1);
        options.set(CoreOptions.CHANGELOG_NUM_RETAINED_MAX, 20);
        options.set(CoreOptions.CHANGELOG_NUM_RETAINED_MIN, 10);
        options.setString(CoreOptions.CHANGELOG_PRODUCER.key(), changelogProducer);
        FileStoreTable table =
                createFileStoreTable(
                        true,
                        options,
                        new Path(
                                TraceableFileIO.SCHEME
                                        + "://"
                                        + tempDir.toString()
                                        + "/"
                                        + UUID.randomUUID()));
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(1, 40, 400L));
        commit.commit(0, write.prepareCommit(true, 0));

        Thread.sleep(50);

        write.write(rowData(1, 10, 101L));
        write.write(rowData(1, 30, 300L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        commit.commit(1, write.prepareCommit(true, 1));

        // wait for a little while
        Thread.sleep(50);

        write.write(rowData(1, 10, 102L));
        write.write(rowData(1, 20, 201L));
        commit.commit(2, write.prepareCommit(true, 2));

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(3);
        assertThat(snapshotManager.earliestLongLivedChangelogId()).isEqualTo(1);
        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(2);

        ContinuousFromTimestampStartingScanner scanner =
                new ContinuousFromTimestampStartingScanner(
                        snapshotManager, snapshotManager.snapshot(3).timeMillis(), true);
        StartingScanner.NextSnapshot result =
                (StartingScanner.NextSnapshot) scanner.scan(snapshotReader);
        assertThat(result.nextSnapshotId()).isEqualTo(3);
        scanner =
                new ContinuousFromTimestampStartingScanner(
                        snapshotManager, snapshotManager.snapshot(2).timeMillis(), true);

        assertThat(((StartingScanner.NextSnapshot) scanner.scan(snapshotReader)).nextSnapshotId())
                .isEqualTo(2);

        scanner =
                new ContinuousFromTimestampStartingScanner(
                        snapshotManager, snapshotManager.changelog(1).timeMillis(), true);
        assertThat(((StartingScanner.NextSnapshot) scanner.scan(snapshotReader)).nextSnapshotId())
                .isEqualTo(1);

        write.close();
        commit.close();
    }
}
