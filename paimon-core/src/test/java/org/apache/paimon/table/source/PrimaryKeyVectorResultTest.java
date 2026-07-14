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

/** Tests conversion from primary-key vector candidates to {@link IndexedSplit}s. */
class PrimaryKeyVectorResultTest {

    @Test
    void testGroupsCandidatesByFileAndAlignsScoresWithRanges() {
        DataFileMeta dataFile = dataFile("data-1");
        DeletionFile deletionFile = new DeletionFile("dv", 10, 20, 1L);
        DataSplit dataSplit = dataSplit(dataFile, deletionFile);
        PrimaryKeyVectorScan.Plan plan =
                PrimaryKeyVectorScan.plan(
                        11, Collections.singletonList(dataSplit), Collections.emptyList());
        List<PrimaryKeyVectorRead.Candidate> candidates =
                Arrays.asList(
                        new PrimaryKeyVectorRead.Candidate(
                                BinaryRow.EMPTY_ROW, 0, "data-1", 4, 0.1F),
                        new PrimaryKeyVectorRead.Candidate(BinaryRow.EMPTY_ROW, 0, "data-1", 1, 1F),
                        new PrimaryKeyVectorRead.Candidate(
                                BinaryRow.EMPTY_ROW, 0, "data-1", 2, 4F));

        List<IndexedSplit> splits = new PrimaryKeyVectorResult(plan, candidates, "l2").splits();

        assertThat(splits).hasSize(1);
        IndexedSplit split = splits.get(0);
        assertThat(split.dataSplit().snapshotId()).isEqualTo(11);
        assertThat(split.dataSplit().dataFiles()).containsExactly(dataFile);
        assertThat(split.dataSplit().deletionFiles().get()).containsExactly(deletionFile);
        assertThat(split.rowRanges()).containsExactly(new Range(1, 2), new Range(4, 4));
        assertThat(split.scores())
                .containsExactly(1F / (1F + 1F), 1F / (1F + 4F), 1F / (1F + 0.1F));
    }

    private static DataSplit dataSplit(DataFileMeta dataFile, DeletionFile deletionFile) {
        return DataSplit.builder()
                .withSnapshot(11)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("bucket-0")
                .withTotalBuckets(1)
                .withDataFiles(Collections.singletonList(dataFile))
                .withDataDeletionFiles(Collections.singletonList(deletionFile))
                .build();
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
