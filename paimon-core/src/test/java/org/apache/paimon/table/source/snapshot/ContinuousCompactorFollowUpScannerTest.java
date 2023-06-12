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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.system.BucketsTable;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ContinuousCompactorFollowUpScanner}. */
public class ContinuousCompactorFollowUpScannerTest extends ScannerTestBase {

    private final DataFileMetaSerializer dataFileMetaSerializer = new DataFileMetaSerializer();

    @Test
    public void testScan() throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(2, 40, 400L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(2, 30, 300L));
        write.write(rowDataWithKind(RowKind.DELETE, 2, 40, 400L));
        write.compact(binaryRow(1), 0, true);
        commit.commit(1, write.prepareCommit(true, 1));

        write.close();
        commit.close();

        Map<String, String> overwritePartition = new HashMap<>();
        overwritePartition.put("pt", "1");
        write = table.newWrite(commitUser).fromEmptyWriter(true);
        commit = table.newCommit(commitUser).withOverwrite(overwritePartition);
        write.write(rowData(1, 10, 101L));
        write.write(rowData(1, 20, 201L));
        commit.commit(2, write.prepareCommit(true, 2));

        write.close();
        commit.close();

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(4);

        BucketsTable bucketsTable = new BucketsTable(table, true);
        TableRead read = bucketsTable.newRead();
        ContinuousCompactorFollowUpScanner scanner = new ContinuousCompactorFollowUpScanner();

        Snapshot snapshot = snapshotManager.snapshot(1);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(scanner.shouldScanSnapshot(snapshot)).isTrue();
        TableScan.Plan plan = scanner.scan(1, snapshotSplitReader);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|1|0|1", "+I 1|2|0|1"));

        snapshot = snapshotManager.snapshot(2);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(scanner.shouldScanSnapshot(snapshot)).isTrue();
        plan = scanner.scan(2, snapshotSplitReader);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Collections.singletonList("+I 2|2|0|1"));

        snapshot = snapshotManager.snapshot(3);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(scanner.shouldScanSnapshot(snapshot)).isFalse();

        snapshot = snapshotManager.snapshot(4);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);
        assertThat(scanner.shouldScanSnapshot(snapshot)).isFalse();
    }

    @Override
    protected String rowDataToString(InternalRow rowData) {
        int numFiles;
        try {
            numFiles = dataFileMetaSerializer.deserializeList(rowData.getBinary(3)).size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return String.format(
                "%s %d|%d|%d|%d",
                rowData.getRowKind().shortString(),
                rowData.getLong(0),
                deserializeBinaryRow(rowData.getBinary(1)).getInt(0),
                rowData.getInt(2),
                numFiles);
    }
}
