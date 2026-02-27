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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataEvolutionBatchScan}. */
public class DataEvolutionBatchScanTest {

    @Test
    public void testWrapToIndexSplitsRandomly() {
        Random random = new Random();
        for (int round = 0; round < 2000; round++) {
            int splitNum = 1 + random.nextInt(20);
            List<Split> splits = new ArrayList<>(splitNum);
            List<Range> splitRanges = new ArrayList<>(splitNum);

            long cursor = random.nextInt(10);
            for (int i = 0; i < splitNum; i++) {
                long start = cursor + random.nextInt(4);
                long rowCount = 30 + random.nextInt(31);
                long end = start + rowCount - 1;

                DataSplit split =
                        DataSplit.builder()
                                .withSnapshot(1L)
                                .withPartition(BinaryRow.EMPTY_ROW)
                                .withBucket(i)
                                .withBucketPath("bucket-" + i)
                                .withDataFiles(
                                        Collections.singletonList(
                                                newAppendFile(
                                                        start,
                                                        rowCount,
                                                        "round-" + round + "-split-" + i)))
                                .build();
                splits.add(split);
                splitRanges.add(new Range(start, end));
                cursor = end + 2 + random.nextInt(4);
            }

            List<Range> candidateRanges = new ArrayList<>();
            for (Range splitRange : splitRanges) {
                int fragmentNum = 5 + random.nextInt(6);
                candidateRanges.add(new Range(splitRange.from, splitRange.from));
                for (int i = 0; i < fragmentNum - 2; i++) {
                    long rowId = splitRange.from + 2L * (i + 1);
                    candidateRanges.add(new Range(rowId, rowId));
                }
                candidateRanges.add(new Range(splitRange.to, splitRange.to));
            }

            List<Range> rowRanges = Range.sortAndMergeOverlap(candidateRanges, true);
            List<Split> indexedSplits =
                    DataEvolutionBatchScan.wrapToIndexSplits(
                                    splits, RowRangeIndex.create(rowRanges), null)
                            .splits();

            assertThat(indexedSplits).hasSize(splits.size());
            for (int i = 0; i < indexedSplits.size(); i++) {
                DataSplit split = (DataSplit) splits.get(i);
                IndexedSplit indexedSplit = (IndexedSplit) indexedSplits.get(i);

                List<DataFileMeta> files = split.dataFiles();
                long min = files.get(0).nonNullFirstRowId();
                long max =
                        files.get(files.size() - 1).nonNullFirstRowId()
                                + files.get(files.size() - 1).rowCount()
                                - 1;
                List<Range> expected = expectedRanges(min, max, rowRanges);

                assertThat(expected).isNotEmpty();
                assertThat(expected.size()).isBetween(5, 10);
                assertThat(indexedSplit.dataSplit()).isEqualTo(split);
                assertThat(indexedSplit.rowRanges()).containsExactlyElementsOf(expected);
            }
        }
    }

    private static List<Range> expectedRanges(long min, long max, List<Range> rowRanges) {
        List<Range> expected = new ArrayList<>();
        for (Range range : rowRanges) {
            if (range.to < min) {
                continue;
            }
            if (range.from > max) {
                break;
            }
            expected.add(new Range(Math.max(min, range.from), Math.min(max, range.to)));
        }
        return expected;
    }

    private static DataFileMeta newAppendFile(long firstRowId, long rowCount, String name) {
        return DataFileMeta.forAppend(
                name,
                1024L,
                rowCount,
                EMPTY_STATS,
                0L,
                firstRowId + rowCount - 1,
                1L,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                firstRowId,
                null);
    }
}
