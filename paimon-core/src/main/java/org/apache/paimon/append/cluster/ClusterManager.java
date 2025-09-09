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

package org.apache.paimon.append.cluster;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.snapshot.SnapshotReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** doc. */
public class ClusterManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterManager.class);

    private final SnapshotReader snapshotReader;

    private final ClusterStrategy clusterStrategy;
    private final CoreOptions.OrderType clusterCurve;
    private final List<String> clusterKeys;

    private int maxLevel;

    public ClusterManager(FileStoreTable table) {
        // drop stats to reduce memory usage
        this.snapshotReader = table.newSnapshotReader().dropStats();
        CoreOptions options = table.coreOptions();
        this.clusterStrategy =
                new ClusterStrategy(
                        table.schemaManager(),
                        options.liquidClusterColumns(),
                        options.maxSizeAmplificationPercent(),
                        options.sortedRunSizeRatio(),
                        options.numSortedRunCompactionTrigger());
        this.clusterCurve = options.clusteringStrategy(options.liquidClusterColumns().size());
        this.clusterKeys = options.liquidClusterColumns();
        this.maxLevel = options.numLevels();
    }

    public Map<BinaryRow, CompactUnit> prepareForCluster(boolean fullCompaction) {
        // 1. construct LSM structure for each partition
        Map<BinaryRow, List<LevelSortedRun>> partitionLevels = constructLevels();

        // 2. pick files to be clustered for each partition
        // TODO：consider the maxLevel in existed files
        Map<BinaryRow, Optional<CompactUnit>> units =
                partitionLevels.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry ->
                                                clusterStrategy.pick(
                                                        maxLevel,
                                                        entry.getValue(),
                                                        fullCompaction)));

        // 3. filter out empty units
        return units.entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get()));
    }

    public Map<BinaryRow, List<LevelSortedRun>> constructLevels() {
        List<DataSplit> dataSplits = snapshotReader.read().dataSplits();

        Map<BinaryRow, List<DataFileMeta>> partitionFiles = new HashMap<>();
        for (DataSplit dataSplit : dataSplits) {
            partitionFiles
                    .computeIfAbsent(dataSplit.partition(), k -> new ArrayList<>())
                    .addAll(dataSplit.dataFiles());
        }

        return partitionFiles.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> constructPartitionLevels(entry.getValue())));
    }

    public List<LevelSortedRun> constructPartitionLevels(List<DataFileMeta> partitionFiles) {
        List<LevelSortedRun> partitionLevels = new ArrayList<>();
        Map<Integer, List<DataFileMeta>> levelMap =
                partitionFiles.stream().collect(Collectors.groupingBy(DataFileMeta::level));

        for (Map.Entry<Integer, List<DataFileMeta>> entry : levelMap.entrySet()) {
            int level = entry.getKey();
            if (level == 0) {
                for (DataFileMeta level0File : entry.getValue()) {
                    partitionLevels.add(
                            new LevelSortedRun(level, SortedRun.fromSingle(level0File)));
                }
            } else {
                partitionLevels.add(
                        new LevelSortedRun(level, SortedRun.fromSorted(entry.getValue())));
            }
        }
        return partitionLevels;
    }

    public List<DataSplit> toSplits(BinaryRow partition, List<DataFileMeta> files) {
        List<DataSplit> splits = new ArrayList<>();

        DataSplit.Builder builder =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withTotalBuckets(1)
                        .isStreaming(false);

        SplitGenerator splitGenerator = snapshotReader.splitGenerator();
        List<SplitGenerator.SplitGroup> splitGroups = splitGenerator.splitForBatch(files);

        for (SplitGenerator.SplitGroup splitGroup : splitGroups) {
            List<DataFileMeta> dataFiles = splitGroup.files;
            String bucketPath = snapshotReader.pathFactory().bucketPath(partition, 0).toString();
            builder.withDataFiles(dataFiles)
                    .rawConvertible(splitGroup.rawConvertible)
                    .withBucketPath(bucketPath);

            splits.add(builder.build());
        }

        return splits;
    }

    public List<DataFileMeta> upgrade(List<DataFileMeta> filesAfterCluster, int outputLevel) {
        return filesAfterCluster.stream()
                .map(file -> file.upgrade(outputLevel))
                .collect(Collectors.toList());
    }

    public CoreOptions.OrderType clusterCurve() {
        return clusterCurve;
    }

    public List<String> clusterKeys() {
        return clusterKeys;
    }
}
