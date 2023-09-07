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

import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FullCompactedStartingScanner}. */
public class FullCompactedStartingScannerTest extends ScannerTestBase {

    @Test
    public void testScan() throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        for (int i = 0; i < 5; i++) {
            write.write(rowData(1, 10, 102L));
            write.write(rowData(1, 20, 201L));
            write.compact(binaryRow(1), 0, true);
            commit.commit(i, write.prepareCommit(true, i));
        }

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(10);

        FullCompactedStartingScanner scanner = new FullCompactedStartingScanner(snapshotManager, 3);
        StartingScanner.ScannedResult result =
                (StartingScanner.ScannedResult) scanner.scan(snapshotReader);
        assertThat(result.currentSnapshotId()).isEqualTo(8);

        write.close();
        commit.close();
    }

    @Test
    public void testNoSnapshot() {
        SnapshotManager snapshotManager = table.snapshotManager();
        FullCompactedStartingScanner scanner = new FullCompactedStartingScanner(snapshotManager, 3);
        assertThat(scanner.scan(snapshotReader)).isInstanceOf(StartingScanner.NoSnapshot.class);
    }

    @Test
    public void testNoCompactSnapshot() throws Exception {
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
        write.compact(binaryRow(1), 0, true);
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(1, 10, 102L));
        write.write(rowData(1, 20, 201L));
        commit.commit(2, write.prepareCommit(true, 2));

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(4);

        FullCompactedStartingScanner scanner = new FullCompactedStartingScanner(snapshotManager, 3);

        // No compact snapshot found, reading from the latest snapshot
        StartingScanner.ScannedResult result =
                (StartingScanner.ScannedResult) scanner.scan(snapshotReader);
        assertThat(result.currentSnapshotId()).isEqualTo(4);

        write.close();
        commit.close();
    }
}
