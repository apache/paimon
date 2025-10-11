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
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN;
import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE;
import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IncrementalDeltaStartingScanner}. */
public class IncrementalDeltaStartingScannerTest extends ScannerTestBase {

    @Test
    public void testScanDeltaBySnapshotId() throws Exception {
        writeDataToTable();

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(INCREMENTAL_BETWEEN.key(), "1,4");
        List<Split> splits = table.copy(dynamicOptions).newScan().plan().splits();
        assertThat(getResult(table.newRead(), splits))
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 2|20|200",
                                "+I 1|10|100",
                                "+I 3|40|400",
                                "-U 3|40|400",
                                "+U 3|40|500"));

        dynamicOptions.put(INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta");
        splits = table.copy(dynamicOptions).newScan().plan().splits();
        assertThat(getResult(table.newRead(), splits))
                .hasSameElementsAs(Arrays.asList("+I 2|20|200", "+I 1|10|100", "+I 3|40|500"));
    }

    @Test
    public void testScanDeltaByTag() throws Exception {
        writeDataToTable();

        table.createTag("tag-from-snapshot-2", 2L);
        table.createTag("tag-from-snapshot-4", 4L);

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(INCREMENTAL_BETWEEN.key(), "tag-from-snapshot-2,tag-from-snapshot-4");
        dynamicOptions.put(INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(), "true");
        dynamicOptions.put(INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta");
        List<Split> splits = table.copy(dynamicOptions).newScan().plan().splits();
        assertThat(getResult(table.newRead(), splits))
                .hasSameElementsAs(Arrays.asList("+I 2|20|200", "+I 1|10|100", "+I 3|40|500"));
    }

    @Test
    public void testScanChangelogByTag() throws Exception {
        writeDataToTable();

        table.createTag("tag-from-snapshot-2", 2L);
        table.createTag("tag-from-snapshot-4", 4L);

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(INCREMENTAL_BETWEEN.key(), "tag-from-snapshot-2,tag-from-snapshot-4");
        dynamicOptions.put(INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(), "true");
        dynamicOptions.put(INCREMENTAL_BETWEEN_SCAN_MODE.key(), "changelog");
        List<Split> splits = table.copy(dynamicOptions).newScan().plan().splits();
        assertThat(getResult(table.newRead(), splits))
                .hasSameElementsAs(Arrays.asList("-U 3|40|400", "+U 3|40|500"));
    }

    @Test
    public void testIllegalScanSnapshotId() throws Exception {
        writeDataToTable();

        // Allowed starting snapshotId to be equal to the earliest snapshotId -1.
        assertThatNoException()
                .isThrownBy(
                        () ->
                                IncrementalDeltaStartingScanner.betweenSnapshotIds(
                                                0, 4, table.snapshotManager(), ScanMode.DELTA)
                                        .scan(snapshotReader));

        assertThatThrownBy(
                        () ->
                                IncrementalDeltaStartingScanner.betweenSnapshotIds(
                                                1, 5, table.snapshotManager(), ScanMode.DELTA)
                                        .scan(snapshotReader))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "The specified scan snapshotId range [1, 5] is out of available snapshotId range [1, 4]."));
    }

    private void writeDataToTable() throws Exception {
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

        write.close();
        commit.close();

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(4);
    }

    @Override
    protected FileStoreTable createFileStoreTable() throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.LOOKUP);
        conf.set(CoreOptions.CHANGELOG_PRODUCER_ROW_DEDUPLICATE, true);
        return createFileStoreTable(conf);
    }
}
