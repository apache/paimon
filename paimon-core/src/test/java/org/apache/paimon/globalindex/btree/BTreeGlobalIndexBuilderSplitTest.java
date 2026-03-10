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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.PojoDataFileMeta;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for split regrouping in {@link BTreeGlobalIndexBuilder}. */
public class BTreeGlobalIndexBuilderSplitTest {

    @Test
    public void testSplitByContiguousRowRangeFromDataFiles() {
        DataFileMeta file1 = createDataFileMeta(0L, 100L);
        DataFileMeta file2 = createDataFileMeta(300L, 100L);
        DataFileMeta file3 = createDataFileMeta(100L, 100L);
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Arrays.asList(file1, file2, file3))
                        .isStreaming(false)
                        .rawConvertible(false)
                        .build();

        List<DataSplit> rebuilt =
                BTreeGlobalIndexBuilder.splitByContiguousRowRange(Collections.singletonList(split));

        assertThat(rebuilt).hasSize(2);
        assertThat(rebuilt.get(0).dataFiles()).containsExactly(file1, file3);
        assertThat(rebuilt.get(1).dataFiles()).containsExactly(file2);
        assertThat(BTreeGlobalIndexBuilder.calcRowRange(rebuilt.get(0)))
                .isEqualTo(new Range(0, 199));
        assertThat(BTreeGlobalIndexBuilder.calcRowRange(rebuilt.get(1)))
                .isEqualTo(new Range(300, 399));
    }

    private static DataFileMeta createDataFileMeta(long firstRowId, long rowCount) {
        return new PojoDataFileMeta(
                "test-file-" + UUID.randomUUID(),
                1024L,
                rowCount,
                BinaryRow.EMPTY_ROW,
                BinaryRow.EMPTY_ROW,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                0L,
                0L,
                0L,
                0,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                null,
                firstRowId,
                null);
    }
}
