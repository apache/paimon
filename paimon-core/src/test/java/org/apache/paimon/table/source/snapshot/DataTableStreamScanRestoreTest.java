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
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.StreamTableScan;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Regression tests for restoring a stream scan whose checkpointed snapshot has expired. */
public class DataTableStreamScanRestoreTest extends ScannerTestBase {

    @Test
    public void testExpiredCheckpointOnlyFallsBackForDedicatedCompactionJob() throws Exception {
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        for (int i = 0; i < 5; i++) {
            write.write(rowData(i, i, (long) i));
            commit.commit(i, write.prepareCommit(true, i));
        }
        write.close();
        commit.close();

        // Retain only the latest two snapshots, so a checkpoint restored at snapshot 2 sits below
        // the earliest retained snapshot.
        Options expireOptions = new Options();
        expireOptions.set(CoreOptions.SNAPSHOT_EXPIRE_LIMIT, 5);
        expireOptions.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 2);
        expireOptions.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 2);
        table.copy(expireOptions.toMap()).newCommit("").expireSnapshots();
        assertThat(table.snapshotManager().earliestSnapshotId()).isGreaterThan(2L);

        // An ordinary consumer must keep the stalled checkpoint. Resetting it would reapply the
        // configured startup mode and silently skip the snapshots that are still retained.
        StreamTableScan ordinaryScan = table.newStreamScan();
        ordinaryScan.restore(2L);
        assertThat(ordinaryScan.checkpoint()).isEqualTo(2L);

        // The dedicated compaction source has an idempotent recovery contract, so it may resume
        // from its starting scanner instead of stalling forever.
        FileStoreTable compactionTable =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.STREAM_SCAN_MODE.key(),
                                CoreOptions.StreamScanMode.COMPACT_BUCKET_TABLE.getValue()));
        StreamTableScan compactionScan = compactionTable.newStreamScan();
        compactionScan.restore(2L);
        assertThat(compactionScan.checkpoint()).isNull();
    }
}
