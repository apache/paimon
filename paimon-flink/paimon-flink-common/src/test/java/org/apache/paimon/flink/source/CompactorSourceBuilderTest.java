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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.FlinkConnectorOptions.CompactionBucketDistributionStrategy;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.source.DataSplit;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompactorSourceBuilder}. */
public class CompactorSourceBuilderTest {

    @Test
    public void testSizeAwareBatchUsesSinkParallelismForSource() {
        Options options = optionsWithParallelism(4, 16);

        assertThat(
                        CompactorSourceBuilder.sourceParallelism(
                                options, CompactionBucketDistributionStrategy.SIZE_AWARE_BATCH))
                .isEqualTo(16);
    }

    @Test
    public void testNonSizeAwareBatchUsesScanParallelismForSource() {
        Options options = optionsWithParallelism(4, 16);

        assertThat(
                        CompactorSourceBuilder.sourceParallelism(
                                options, CompactionBucketDistributionStrategy.LINEAR))
                .isEqualTo(4);
    }

    @Test
    public void testSizeAwareBatchFallsBackToScanParallelismWithoutSinkParallelism() {
        Map<String, String> map = new HashMap<>();
        map.put(FlinkConnectorOptions.SCAN_PARALLELISM.key(), "4");
        Options options = Options.fromMap(map);

        assertThat(
                        CompactorSourceBuilder.sourceParallelism(
                                options, CompactionBucketDistributionStrategy.SIZE_AWARE_BATCH))
                .isEqualTo(4);
    }

    @Test
    public void testBucketFileSizeUsesTotalDataFileSize() {
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(
                                Arrays.asList(dataFile("file-1", 10L), dataFile("file-2", 25L)))
                        .build();

        assertThat(CompactorSourceBuilder.bucketFileSize(split)).isEqualTo(35L);
    }

    private static Options optionsWithParallelism(int scanParallelism, int sinkParallelism) {
        Map<String, String> map = new HashMap<>();
        map.put(FlinkConnectorOptions.SCAN_PARALLELISM.key(), String.valueOf(scanParallelism));
        map.put(FlinkConnectorOptions.SINK_PARALLELISM.key(), String.valueOf(sinkParallelism));
        return Options.fromMap(map);
    }

    private static DataFileMeta dataFile(String fileName, long fileSize) {
        return DataFileMeta.forAppend(
                fileName,
                fileSize,
                1L,
                null,
                0L,
                0L,
                0L,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                null);
    }
}
