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
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StaticFromSnapshotStartingScanner}. */
public class StaticFromStartingScannerTest extends ScannerTestBase {

    @Test
    public void testScan() throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(2, 20, 200L));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(3, 30, 300L));
        commit.commit(2, write.prepareCommit(true, 2));

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(3);

        Map<String, String> dynamicOptions = new HashMap<>();

        dynamicOptions.put(SCAN_SNAPSHOT_ID.key(), "1");
        List<Split> splits = table.copy(dynamicOptions).newScan().plan().splits();
        assertThat(getResult(table.newRead(), splits))
                .hasSameElementsAs(Arrays.asList("+I 1|10|100"));

        dynamicOptions.put(SCAN_SNAPSHOT_ID.key(), "2");
        splits = table.copy(dynamicOptions).newScan().plan().splits();
        assertThat(getResult(table.newRead(), splits))
                .hasSameElementsAs(Arrays.asList("+I 1|10|100", "+I 2|20|200"));

        dynamicOptions.put(SCAN_SNAPSHOT_ID.key(), "3");
        splits = table.copy(dynamicOptions).newScan().plan().splits();
        assertThat(getResult(table.newRead(), splits))
                .hasSameElementsAs(Arrays.asList("+I 1|10|100", "+I 2|20|200", "+I 3|30|300"));
    }

    @Test
    public void testScanSnapshotIdOutOfAvailableSnapshotIdRange() throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        commit.commit(0, write.prepareCommit(true, 0));

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(1);

        assertThatThrownBy(
                        () ->
                                new StaticFromSnapshotStartingScanner(snapshotManager, 2)
                                        .scan(snapshotReader))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "The specified scan snapshotId 2 is out of available snapshotId range [1, 1]."));
    }
}
