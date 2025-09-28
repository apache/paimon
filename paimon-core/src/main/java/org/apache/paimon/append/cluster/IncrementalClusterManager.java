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
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.snapshot.SnapshotReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.CLUSTERING_INCREMENTAL;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Manager for Incremental Clustering. */
public class IncrementalClusterManager {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalClusterManager.class);

    private final SnapshotReader snapshotReader;

    private final IncrementalClusterStrategy incrementalClusterStrategy;
    private final CoreOptions.OrderType clusterCurve;
    private final List<String> clusterKeys;

    private int maxLevel;

    public IncrementalClusterManager(FileStoreTable table) {
        checkArgument(
                table.bucketMode() == BucketMode.BUCKET_UNAWARE,
                "only append unaware-bucket table support incremental clustering.");
        // drop stats to reduce memory usage
        this.snapshotReader = table.newSnapshotReader().dropStats();
        CoreOptions options = table.coreOptions();
        checkArgument(
                options.clusteringIncrementalEnabled(),
                "Only support incremental clustering when '%s' is true.",
                CLUSTERING_INCREMENTAL.key());
        this.incrementalClusterStrategy =
                new IncrementalClusterStrategy(
                        table.schemaManager(),
                        options.clusteringColumns(),
                        options.maxSizeAmplificationPercent(),
                        options.sortedRunSizeRatio(),
                        options.numSortedRunCompactionTrigger());
        this.clusterCurve = options.clusteringStrategy(options.clusteringColumns().size());
        this.clusterKeys = options.clusteringColumns();
        this.maxLevel = options.numLevels();
    }

    public Map<BinaryRow, CompactUnit> prepareForCluster(boolean fullCompaction) {
        // 1. construct LSM structure for each partition
        Map<BinaryRow, List<LevelSortedRun>> partitionLevels = constructLevels();
        if (LOG.isDebugEnabled()) {
            partitionLevels.forEach(
                    (partition, levelSortedRuns) -> {
                        String runsInfo =
                                levelSortedRuns.stream()
                                        .map(
                                                lsr ->
                                                        String.format(
                                                                "level-%s:%s",
                                                                lsr.level(),
                                                                lsr.run().files().size()))
                                        .collect(Collectors.joining(","));
                        LOG.debug(
                                "Partition {} has {} runs: [{}]",
                                partition,
                                levelSortedRuns.size(),
                                runsInfo);
                    });
        }

        // 2. pick files to be clustered for each partition
        Map<BinaryRow, Optional<CompactUnit>> units =
                partitionLevels.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry ->
                                                incrementalClusterStrategy.pick(
                                                        maxLevel,
                                                        entry.getValue(),
                                                        fullCompaction)));

        // 3. filter out empty units
        Map<BinaryRow, CompactUnit> filteredUnits =
                units.entrySet().stream()
                        .filter(entry -> entry.getValue().isPresent())
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, entry -> entry.getValue().get()));
        if (LOG.isDebugEnabled()) {
            filteredUnits.forEach(
                    (partition, compactUnit) -> {
                        String filesInfo =
                                compactUnit.files().stream()
                                        .map(
                                                file ->
                                                        String.format(
                                                                "%s,%s,%s",
                                                                file.fileName(),
                                                                file.level(),
                                                                file.fileSize()))
                                        .collect(Collectors.joining(", "));
                        LOG.debug(
                                "Partition {}, outputLevel:{}, clustered with {} files: [{}]",
                                partition,
                                compactUnit.outputLevel(),
                                compactUnit.files().size(),
                                filesInfo);
                    });
        }
        return filteredUnits;
    }

    public Map<BinaryRow, List<LevelSortedRun>> constructLevels() {
        List<DataSplit> dataSplits = snapshotReader.read().dataSplits();

        maxLevel =
                Math.max(
                        maxLevel,
                        dataSplits.stream()
                                        .flatMap(split -> split.dataFiles().stream())
                                        .mapToInt(DataFileMeta::level)
                                        .max()
                                        .orElse(-1)
                                + 1);
        checkArgument(maxLevel > 1, "Number of levels must be at least 2.");

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
                // don't need to guarantee that the files within the same sorted run are
                // non-overlapping here, so we call SortedRun.fromSorted() to avoid sorting and
                // validation
                partitionLevels.add(
                        new LevelSortedRun(level, SortedRun.fromSorted(entry.getValue())));
            }
        }

        // sort by level
        partitionLevels.sort(Comparator.comparing(LevelSortedRun::level));
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
