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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowTrackingCommitUtilsTest {
    @Test
    void testReplaysSequenceAndRowIdsWithoutCachingAssignedEntries() {
        List<ManifestEntry> input =
                Arrays.asList(
                        entry("copied.parquet", 2, 10L, 2, 3),
                        entry("new.parquet", 3, null, 0, 0),
                        entry("update.parquet", 2, 0L, 2, 0));
        RowTrackingCommitUtils.RowTrackingAssigned expected =
                RowTrackingCommitUtils.assignRowTracking(8, 5, input);
        RowTrackingCommitUtils.RowTrackingAssigned actual =
                RowTrackingCommitUtils.assignRowTracking(
                        8, 5, new ReplayableList<>(input.size(), input::iterator));
        assertThat(actual.nextRowIdStart).isEqualTo(15);
        assertThat(actual.assignedEntries).containsExactlyElementsOf(expected.assignedEntries);
        assertThat(actual.assignedEntries).containsExactlyElementsOf(expected.assignedEntries);
        assertThat(actual.assignedEntries.get(1)).isNotSameAs(actual.assignedEntries.get(1));
        assertThat(actual.assignedEntries.get(0).file().firstRowId()).isEqualTo(10);
        assertThat(actual.assignedEntries.get(1).file().firstRowId()).isEqualTo(12);
        assertThat(actual.assignedEntries.get(2).file().firstRowId()).isEqualTo(0);
        assertThat(actual.assignedEntries.get(2).file().minSequenceNumber()).isEqualTo(2);
        assertThat(actual.assignedEntries.get(2).file().maxSequenceNumber()).isEqualTo(8);
    }

    @Test
    void testDedicatedFilesKeepTheirParentRowIdRangesOnEveryReplay() {
        List<ManifestEntry> input =
                Arrays.asList(
                        entry("first.parquet", 4, null, 0, 0),
                        entry("a-1.blob", 2, null, 0, 0, Collections.singletonList("a")),
                        entry("a-2.blob", 2, null, 0, 0, Collections.singletonList("a")),
                        entry("b.blob", 4, null, 0, 0, Collections.singletonList("b")),
                        entry("first.vector.bin", 4, null, 0, 0),
                        entry("second.parquet", 3, null, 0, 0),
                        entry("a-3.blob", 3, null, 0, 0, Collections.singletonList("a")),
                        entry("second.vector.bin", 3, null, 0, 0));
        RowTrackingCommitUtils.RowTrackingAssigned actual =
                RowTrackingCommitUtils.assignRowTracking(
                        7, 100, new ReplayableList<>(input.size(), input::iterator));
        assertThat(actual.nextRowIdStart).isEqualTo(107);
        for (int pass = 0; pass < 2; pass++) {
            assertThat(actual.assignedEntries)
                    .extracting(e -> e.file().firstRowId())
                    .containsExactly(100L, 100L, 102L, 100L, 100L, 104L, 104L, 104L);
            assertThat(actual.assignedEntries)
                    .extracting(e -> e.file().maxSequenceNumber())
                    .containsOnly(7L);
        }
    }

    @Test
    void testRejectsDedicatedFileBeforeParentDuringPreparation() {
        List<ManifestEntry> input =
                Collections.singletonList(
                        entry("early.blob", 2, null, 0, 0, Collections.singletonList("a")));
        assertThatThrownBy(
                        () ->
                                RowTrackingCommitUtils.assignRowTracking(
                                        7,
                                        100,
                                        new ReplayableList<>(input.size(), input::iterator)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("blobStart");
    }

    @Test
    void testReplayListIteratorAndSubListContract() {
        List<Integer> list = new ReplayableList<>(3, () -> Arrays.asList(10, 20, 30).iterator());
        ListIterator<Integer> iterator = list.listIterator(1);
        assertThat(iterator.previous()).isEqualTo(10);
        assertThat(iterator.next()).isEqualTo(10);
        assertThat(iterator.next()).isEqualTo(20);
        assertThat(iterator.previous()).isEqualTo(20);
        assertThat(iterator.next()).isEqualTo(20);
        assertThat(iterator.next()).isEqualTo(30);
        assertThat(iterator.hasNext()).isFalse();
        assertThat(list.subList(1, 3)).containsExactly(20, 30);
        assertThat(list.toArray()).containsExactly(10, 20, 30);
        assertThatThrownBy(() -> list.get(-1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> list.get(3)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    private ManifestEntry entry(String name, long rows, Long rowId, long min, long max) {
        return entry(name, rows, rowId, min, max, null);
    }

    private ManifestEntry entry(
            String name, long rows, Long rowId, long min, long max, List<String> cols) {
        return ManifestEntry.create(
                FileKind.ADD,
                BinaryRow.EMPTY_ROW,
                0,
                -1,
                DataFileMeta.forAppend(
                        name,
                        10,
                        rows,
                        SimpleStats.EMPTY_STATS,
                        min,
                        max,
                        0,
                        Collections.emptyList(),
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        rowId,
                        cols));
    }
}
