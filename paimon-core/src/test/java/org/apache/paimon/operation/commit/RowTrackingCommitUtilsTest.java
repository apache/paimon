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

package org.apache.paimon.operation.commit;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowTrackingCommitUtils}. */
public class RowTrackingCommitUtilsTest {

    @Test
    public void testBlobStartsFromItsCommitMessageRange() {
        DataFileMeta normalOnly = file("normal-0.parquet", 1, null);
        DataFileMeta normalWithBlob = file("normal-1.parquet", 1, null);
        DataFileMeta blob = file("camera-0.video", 1, Collections.singletonList("camera"));
        ManifestEntryChanges changes = new ManifestEntryChanges(1);
        changes.collect(
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        1,
                        new DataIncrement(
                                Collections.singletonList(normalOnly),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement()));
        changes.collect(
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        1,
                        new DataIncrement(
                                Arrays.asList(normalWithBlob, blob),
                                Collections.emptyList(),
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement()));

        RowTrackingCommitUtils.RowTrackingAssigned assigned =
                RowTrackingCommitUtils.assignRowTracking(
                        1, 10, changes.appendTableFiles, changes.appendTableFileGroups);

        assertThat(assigned.nextRowIdStart).isEqualTo(12);
        assertThat(assigned.assignedEntries)
                .extracting(entry -> entry.file().firstRowId())
                .containsExactly(10L, 11L, 11L);
    }

    private static DataFileMeta file(String name, long rowCount, List<String> writeCols) {
        return DataFileMeta.forAppend(
                name,
                1,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                0,
                Collections.emptyList(),
                null,
                FileSource.APPEND,
                null,
                null,
                null,
                writeCols);
    }
}
