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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.flink.lookup.partitioner.BucketIdExtractor;
import org.apache.paimon.flink.lookup.partitioner.BucketShufflePartitioner;
import org.apache.paimon.flink.lookup.partitioner.BucketShuffleStrategy;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** The test for {@link BucketShufflePartitioner}. */
public class BucketShufflePartitionerTest extends CatalogITCaseBase {

    @Test
    public void testBucketNumLessThanLookupJoinParallelism() throws Exception {
        int numBuckets = 100;
        int lookupJoinParallelism = 27;
        FileStoreTable table = createTestTable(numBuckets);
        BucketShuffleStrategy strategy = new BucketShuffleStrategy(numBuckets);
        BucketShufflePartitioner bucketShufflePartitioner =
                createBucketShufflePartitioner(table, numBuckets, strategy);
        List<Tuple2<RowData, Integer>> joinKeysList =
                getGroundTruthJoinKeysWithBucketId(table, numBuckets);
        Map<Integer, List<Tuple2<RowData, Integer>>> partitionResult = new HashMap<>();
        for (Tuple2<RowData, Integer> joinKeysWithBucketId : joinKeysList) {
            int subtaskId =
                    bucketShufflePartitioner.partition(
                            joinKeysWithBucketId.f0, lookupJoinParallelism);
            partitionResult.compute(
                    subtaskId,
                    (key, currentValue) -> {
                        List<Tuple2<RowData, Integer>> newValue =
                                (currentValue != null) ? currentValue : new ArrayList<>();
                        newValue.add(joinKeysWithBucketId);
                        return newValue;
                    });
        }
        for (int subtaskId = 0; subtaskId < lookupJoinParallelism; ++subtaskId) {
            List<Tuple2<RowData, Integer>> joinKeysWithBucketIdList =
                    partitionResult.get(subtaskId);
            Set<Integer> requiredCacheBucketIds =
                    strategy.getRequiredCacheBucketIds(subtaskId, lookupJoinParallelism);
            for (Tuple2<RowData, Integer> joinKeysWithBucketId : joinKeysWithBucketIdList) {
                assertThat(requiredCacheBucketIds).contains(joinKeysWithBucketId.f1);
                assertThat(joinKeysWithBucketId.f1 % lookupJoinParallelism).isEqualTo(subtaskId);
            }
        }
    }

    @Test
    public void testBucketNumEqualToLookupJoinParallelism() throws Exception {
        int numBuckets = 78;
        int lookupJoinParallelism = 78;
        FileStoreTable table = createTestTable(numBuckets);
        BucketShuffleStrategy strategy = new BucketShuffleStrategy(numBuckets);
        BucketShufflePartitioner bucketShufflePartitioner =
                createBucketShufflePartitioner(table, numBuckets, strategy);
        List<Tuple2<RowData, Integer>> joinKeysList =
                getGroundTruthJoinKeysWithBucketId(table, numBuckets);
        Map<Integer, List<Tuple2<RowData, Integer>>> partitionResult = new HashMap<>();
        for (Tuple2<RowData, Integer> joinKeysWithBucketId : joinKeysList) {
            int subtaskId =
                    bucketShufflePartitioner.partition(
                            joinKeysWithBucketId.f0, lookupJoinParallelism);
            partitionResult.compute(
                    subtaskId,
                    (key, currentValue) -> {
                        List<Tuple2<RowData, Integer>> newValue =
                                (currentValue != null) ? currentValue : new ArrayList<>();
                        newValue.add(joinKeysWithBucketId);
                        return newValue;
                    });
        }
        for (int subtaskId = 0; subtaskId < lookupJoinParallelism; ++subtaskId) {
            List<Tuple2<RowData, Integer>> joinKeysWithBucketIdList =
                    partitionResult.get(subtaskId);
            Set<Integer> requiredCacheBucketIds =
                    strategy.getRequiredCacheBucketIds(subtaskId, lookupJoinParallelism);
            for (Tuple2<RowData, Integer> joinKeysWithBucketId : joinKeysWithBucketIdList) {
                assertThat(requiredCacheBucketIds.size()).isOne();
                assertThat(requiredCacheBucketIds).contains(joinKeysWithBucketId.f1);
                assertThat(joinKeysWithBucketId.f1 % lookupJoinParallelism).isEqualTo(subtaskId);
            }
        }
    }

    @Test
    public void testBucketNumLargerThanLookupJoinParallelism() throws Exception {
        int numBuckets = 4;
        int lookupJoinParallelism = 15;
        FileStoreTable table = createTestTable(numBuckets);
        BucketShuffleStrategy strategy = new BucketShuffleStrategy(numBuckets);
        BucketShufflePartitioner bucketShufflePartitioner =
                createBucketShufflePartitioner(table, numBuckets, strategy);
        List<Tuple2<RowData, Integer>> joinKeysList =
                getGroundTruthJoinKeysWithBucketId(table, numBuckets);
        Map<Integer, List<Tuple2<RowData, Integer>>> partitionResult = new HashMap<>();
        for (Tuple2<RowData, Integer> joinKeysWithBucketId : joinKeysList) {
            int subtaskId =
                    bucketShufflePartitioner.partition(
                            joinKeysWithBucketId.f0, lookupJoinParallelism);
            partitionResult.compute(
                    subtaskId,
                    (key, currentValue) -> {
                        List<Tuple2<RowData, Integer>> newValue =
                                (currentValue != null) ? currentValue : new ArrayList<>();
                        newValue.add(joinKeysWithBucketId);
                        return newValue;
                    });
        }
        for (int subtaskId = 0; subtaskId < lookupJoinParallelism; ++subtaskId) {
            List<Tuple2<RowData, Integer>> joinKeysWithBucketIdList =
                    partitionResult.get(subtaskId);
            Set<Integer> requiredCacheBucketIds =
                    strategy.getRequiredCacheBucketIds(subtaskId, lookupJoinParallelism);
            for (Tuple2<RowData, Integer> joinKeysWithBucketId : joinKeysWithBucketIdList) {
                assertThat(requiredCacheBucketIds).contains(joinKeysWithBucketId.f1);
                assertThat(subtaskId % numBuckets).isEqualTo(joinKeysWithBucketId.f1);
            }
        }
    }

    private List<Tuple2<RowData, Integer>> getGroundTruthJoinKeysWithBucketId(
            FileStoreTable table, int numBuckets) throws IOException {
        List<Tuple2<RowData, Integer>> joinKeyRows = new ArrayList<>();
        Random random = new Random();
        for (int bucketId = 0; bucketId < numBuckets; ++bucketId) {
            ManifestEntry file = table.store().newScan().withBucket(bucketId).plan().files().get(0);
            DataSplit dataSplit =
                    DataSplit.builder()
                            .withPartition(file.partition())
                            .withBucket(file.bucket())
                            .withDataFiles(Collections.singletonList(file.file()))
                            .withBucketPath("not used")
                            .build();
            int bucket = bucketId;
            table.newReadBuilder()
                    .newRead()
                    .createReader(dataSplit)
                    .forEachRemaining(
                            internalRow -> {
                                joinKeyRows.add(
                                        Tuple2.of(
                                                GenericRowData.of(
                                                        String.valueOf(random.nextInt(numBuckets)),
                                                        internalRow.getInt(1)),
                                                bucket));
                            });
        }
        return joinKeyRows;
    }

    private BucketShufflePartitioner createBucketShufflePartitioner(
            FileStoreTable table, int numBuckets, BucketShuffleStrategy strategy) {
        BucketIdExtractor bucketIdExtractor =
                new BucketIdExtractor(
                        numBuckets,
                        table.schema(),
                        Arrays.asList("col1", "col2"),
                        Collections.singletonList("col2"));
        return new BucketShufflePartitioner(strategy, bucketIdExtractor);
    }

    private FileStoreTable createTestTable(int bucketNum) throws Exception {
        String tableName = "Test";
        String ddl =
                String.format(
                        "CREATE TABLE %s (col1 STRING, col2 INT, col3 FLOAT) WITH"
                                + " ('bucket'='%s', 'bucket-key' = 'col2')",
                        tableName, bucketNum);
        batchSql(ddl);
        StringBuilder dml = new StringBuilder(String.format("INSERT INTO %s VALUES ", tableName));
        for (int index = 1; index < 1000; ++index) {
            dml.append(String.format("('%s', %s, %s), ", index, index, 101.1F));
        }
        dml.append(String.format("('%s', %s, %s)", 1000, 1000, 101.1F));
        batchSql(dml.toString());
        return paimonTable(tableName);
    }
}
