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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for snapshot-scoped scored physical primary-key results. */
class PrimaryKeyScoredResultTest {

    @Test
    void testMaterializesDeterministicFileSplitsAndAlignedScores() {
        DataFileMeta fileB = dataFile("file-b");
        DataFileMeta fileA = dataFile("file-a");
        DeletionFile deletionB = new DeletionFile("dv-b", 10, 20, 1L);
        DeletionFile deletionA = new DeletionFile("dv-a", 10, 20, 1L);
        DataSplit source =
                DataSplit.builder()
                        .withSnapshot(7)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(2)
                        .withBucketPath("bucket-2")
                        .withTotalBuckets(3)
                        .withDataFiles(Arrays.asList(fileB, fileA))
                        .withDataDeletionFiles(Arrays.asList(deletionB, deletionA))
                        .build();
        List<PrimaryKeySearchPosition> positions =
                Arrays.asList(
                        new PrimaryKeySearchPosition(BinaryRow.EMPTY_ROW, 2, "file-b", 4, 0.25F),
                        new PrimaryKeySearchPosition(BinaryRow.EMPTY_ROW, 2, "file-a", 3, 0.5F),
                        new PrimaryKeySearchPosition(BinaryRow.EMPTY_ROW, 2, "file-a", 1, 0.75F));

        PrimaryKeyScoredResult result =
                new PrimaryKeyScoredResult(7, Collections.singletonList(source), positions);

        assertThat(result.snapshotId()).isEqualTo(7);
        assertThat(result.positions()).containsExactlyElementsOf(positions);
        assertThat(result.splits()).hasSize(2);
        IndexedSplit first = result.splits().get(0);
        assertThat(first.dataSplit().dataFiles()).containsExactly(fileA);
        assertThat(first.dataSplit().deletionFiles().get()).containsExactly(deletionA);
        assertThat(first.rowRanges()).containsExactly(new Range(1, 1), new Range(3, 3));
        assertThat(first.scores()).containsExactly(0.75F, 0.5F);
        IndexedSplit second = result.splits().get(1);
        assertThat(second.dataSplit().dataFiles()).containsExactly(fileB);
        assertThat(second.dataSplit().deletionFiles().get()).containsExactly(deletionB);
        assertThat(second.rowRanges()).containsExactly(new Range(4, 4));
        assertThat(second.scores()).containsExactly(0.25F);
    }

    private static DataFileMeta dataFile(String fileName) {
        return DataFileMeta.forAppend(
                fileName,
                100,
                5,
                SimpleStats.EMPTY_STATS,
                0,
                1,
                1,
                Collections.emptyList(),
                null,
                FileSource.COMPACT,
                null,
                null,
                null,
                null);
    }
}
