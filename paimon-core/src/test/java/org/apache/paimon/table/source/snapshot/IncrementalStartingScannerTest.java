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
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IncrementalStartingScanner}. */
public class IncrementalStartingScannerTest extends ScannerTestBase {

    @Test
    public void testScan() throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write =
                table.newWrite(commitUser).withIOManager(new IOManagerImpl(tempDir.toString()));
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(2, 20, 200L));
        write.write(rowData(3, 40, 400L));
        write.compact(binaryRow(1), 0, false);
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 10, 100L));
        write.write(rowData(2, 20, 200L));
        write.write(rowData(3, 40, 500L));
        write.compact(binaryRow(1), 0, false);
        commit.commit(1, write.prepareCommit(true, 1));

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(4);

        IncrementalStartingScanner deltaScanner =
                new IncrementalStartingScanner(snapshotManager, 1L, 4L, ScanMode.DELTA);
        StartingScanner.ScannedResult deltaResult =
                (StartingScanner.ScannedResult) deltaScanner.scan(snapshotReader);
        assertThat(deltaResult.currentSnapshotId()).isEqualTo(4);
        assertThat(getResult(table.newRead(), toSplits(deltaResult.splits())))
                .hasSameElementsAs(Arrays.asList("+I 2|20|200", "+I 1|10|100", "+I 3|40|500"));

        IncrementalStartingScanner changeLogScanner =
                new IncrementalStartingScanner(snapshotManager, 1L, 4L, ScanMode.CHANGELOG);
        StartingScanner.ScannedResult changeLogResult =
                (StartingScanner.ScannedResult) changeLogScanner.scan(snapshotReader);
        assertThat(changeLogResult.currentSnapshotId()).isEqualTo(4);
        assertThat(getResult(table.newRead(), toSplits(changeLogResult.splits())))
                .hasSameElementsAs(
                        Arrays.asList("+I 2|20|200", "+I 1|10|100", "+I 3|40|400", "+U 3|40|500"));

        write.close();
        commit.close();
    }

    @Override
    protected FileStoreTable createFileStoreTable() throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.LOOKUP);
        conf.set(CoreOptions.CHANGELOG_PRODUCER_ROW_DEDUPLICATE, true);
        return createFileStoreTable(conf);
    }
}
