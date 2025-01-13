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

package org.apache.paimon.flink.source;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.StatsTestUtils;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.DataSplit;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileStoreSourceSplitGenerator}. */
public class FileStoreSourceSplitGeneratorTest {

    @Test
    public void test() {
        List<DataSplit> scanSplits =
                Arrays.asList(
                        dataSplit(1, 0, "f0", "f1"),
                        dataSplit(1, 1, "f2"),
                        dataSplit(2, 0, "f3", "f4", "f5"),
                        dataSplit(2, 1, "f6"),
                        dataSplit(3, 0, "f7"),
                        dataSplit(3, 1, "f8"),
                        dataSplit(4, 0, "f9"),
                        dataSplit(4, 1, "f10"),
                        dataSplit(5, 0, "f11"),
                        dataSplit(5, 1, "f12"),
                        dataSplit(6, 0, "f13"),
                        dataSplit(6, 1, "f14"));
        DataFilePlan tableScanPlan = new DataFilePlan(scanSplits);

        List<FileStoreSourceSplit> splits =
                new FileStoreSourceSplitGenerator().createSplits(tableScanPlan);
        assertThat(splits.size()).isEqualTo(12);
        splits.sort(
                Comparator.comparingInt(
                                o ->
                                        ((DataSplit) ((FileStoreSourceSplit) o).split())
                                                .partition()
                                                .getInt(0))
                        .thenComparing(
                                o -> ((DataSplit) ((FileStoreSourceSplit) o).split()).bucket()));

        // splitId should be the input order!
        assertSplit(splits.get(0), "0000000001", 1, 0, Arrays.asList("f0", "f1"));
        assertSplit(splits.get(1), "0000000002", 1, 1, Collections.singletonList("f2"));
        assertSplit(splits.get(2), "0000000003", 2, 0, Arrays.asList("f3", "f4", "f5"));
        assertSplit(splits.get(3), "0000000004", 2, 1, Collections.singletonList("f6"));
        assertSplit(splits.get(4), "0000000005", 3, 0, Collections.singletonList("f7"));
        assertSplit(splits.get(5), "0000000006", 3, 1, Collections.singletonList("f8"));
        assertSplit(splits.get(6), "0000000007", 4, 0, Collections.singletonList("f9"));
        assertSplit(splits.get(7), "0000000008", 4, 1, Collections.singletonList("f10"));
        assertSplit(splits.get(8), "0000000009", 5, 0, Collections.singletonList("f11"));
        assertSplit(splits.get(9), "0000000010", 5, 1, Collections.singletonList("f12"));
        assertSplit(splits.get(10), "0000000011", 6, 0, Collections.singletonList("f13"));
        assertSplit(splits.get(11), "0000000012", 6, 1, Collections.singletonList("f14"));
    }

    private void assertSplit(
            FileStoreSourceSplit split, String splitId, int part, int bucket, List<String> files) {
        assertThat(split.splitId()).isEqualTo(splitId);
        assertThat(((DataSplit) split.split()).partition().getInt(0)).isEqualTo(part);
        assertThat(((DataSplit) split.split()).bucket()).isEqualTo(bucket);
        assertThat(
                        ((DataSplit) split.split())
                                .dataFiles().stream()
                                        .map(DataFileMeta::fileName)
                                        .collect(Collectors.toList()))
                .isEqualTo(files);
    }

    private DataSplit dataSplit(int partition, int bucket, String... fileNames) {
        List<DataFileMeta> metas = new ArrayList<>();
        for (String fileName : fileNames) {
            metas.add(
                    new DataFileMeta(
                            fileName,
                            0, // not used
                            0, // not used
                            null, // not used
                            null, // not used
                            StatsTestUtils.newEmptySimpleStats(), // not used
                            StatsTestUtils.newEmptySimpleStats(), // not used
                            0, // not used
                            0, // not used
                            0, // not used
                            0, // not used
                            0L, // not used
                            null, // not used
                            FileSource.APPEND,
                            null));
        }
        return DataSplit.builder()
                .withSnapshot(1)
                .withPartition(row(partition))
                .withBucket(bucket)
                .isStreaming(false)
                .withDataFiles(metas)
                .rawConvertible(false)
                .withBucketPath("/") // not used
                .build();
    }
}
