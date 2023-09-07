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
}
