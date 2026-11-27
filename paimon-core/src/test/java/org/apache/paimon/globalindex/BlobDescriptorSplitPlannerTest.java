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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.assertj.core.api.Assertions.assertThat;

class BlobDescriptorSplitPlannerTest {
    @Test
    void testRangesPreserveGapsAndPruneFiles() {
        DataFileMeta base = file("base.parquet", 100, 100);
        DataFileMeta a = file("a.blob", 110, 10);
        DataFileMeta b = file("b.blob", 140, 10);
        DataFileMeta c = file("c.blob", 170, 10);
        DataSplit input = split(base, c, a, b);
        List<Split> result = BlobDescriptorSplitPlanner.split(input, 2);
        assertThat(result).hasSize(2);
        IndexedSplit first = (IndexedSplit) result.get(0);
        IndexedSplit second = (IndexedSplit) result.get(1);
        assertThat(first.rowRanges()).containsExactly(new Range(100, 169));
        assertThat(second.rowRanges()).containsExactly(new Range(170, 199));
        assertThat(first.dataSplit().dataFiles()).containsExactly(base, a, b);
        assertThat(second.dataSplit().dataFiles()).containsExactly(base, c);
        assertThat(first.dataSplit().snapshotId()).isEqualTo(5);
        assertThat(first.dataSplit().partition()).isEqualTo(input.partition());
        assertThat(first.dataSplit().rawConvertible()).isFalse();
        assertThat(input.dataFiles()).containsExactly(base, c, a, b);
    }

    @Test
    void testUnsupportedLayoutsKeepOriginalSplit() {
        DataFileMeta base = file("base.parquet", 100, 100);
        DataFileMeta a = file("a.blob", 110, 30);
        DataFileMeta overlapping = file("b.blob", 120, 30);
        DataFileMeta outside = file("outside.blob", 190, 20);
        for (DataSplit input :
                Arrays.asList(
                        split(base, a, overlapping), split(base, a, outside),
                        split(base, file("update.parquet", 100, 100), a), split(a, outside))) {
            assertThat(BlobDescriptorSplitPlanner.split(input, 1)).containsExactly(input);
        }
        DataSplit input = split(base, a);
        assertThat(BlobDescriptorSplitPlanner.split(input, 1)).containsExactly(input);
        assertThat(BlobDescriptorSplitPlanner.split(input, 0)).containsExactly(input);
        DataSplit streaming =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Arrays.asList(base, a, outside))
                        .isStreaming(true)
                        .build();
        assertThat(BlobDescriptorSplitPlanner.split(streaming, 1)).containsExactly(streaming);
        DataSplit withDv =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Arrays.asList(base, a, file("c.blob", 150, 10)))
                        .withDataDeletionFiles(
                                Arrays.asList(
                                        new org.apache.paimon.table.source.DeletionFile(
                                                "dv", 0, 10, 1L),
                                        null,
                                        null))
                        .build();
        assertThat(BlobDescriptorSplitPlanner.split(withDv, 1)).containsExactly(withDv);
    }

    private static DataSplit split(DataFileMeta... files) {
        return DataSplit.builder()
                .withSnapshot(5)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("bucket-0")
                .withDataFiles(Arrays.asList(files))
                .build();
    }

    private static DataFileMeta file(String name, long first, long count) {
        return DataFileMeta.forAppend(
                name,
                1024L,
                count,
                EMPTY_STATS,
                0L,
                first + count - 1,
                1L,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                first,
                null);
    }
}
