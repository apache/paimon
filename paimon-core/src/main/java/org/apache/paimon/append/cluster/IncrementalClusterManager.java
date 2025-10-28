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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.CLUSTERING_INCREMENTAL;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Manager for Incremental Clustering. */
public class IncrementalClusterManager {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalClusterManager.class);
    private final InternalRowPartitionComputer partitionComputer;

    private final SnapshotReader snapshotReader;

    private final IncrementalClusterStrategy incrementalClusterStrategy;
    private final CoreOptions.OrderType clusterCurve;
    private final List<String> clusterKeys;
    @Nullable private final PartitionPredicate specifiedPartitions;

    @Nullable private final Duration historyPartitionIdleTime;
    private final int historyPartitionLimit;
    @Nullable private Filter<Integer> partitionLevelFilter = null;

    private int maxLevel;

    public IncrementalClusterManager(FileStoreTable table) {
        this(table, null);
    }

    public IncrementalClusterManager(
            FileStoreTable table, @Nullable PartitionPredicate specifiedPartitions) {
        checkArgument(
                table.bucketMode() == BucketMode.BUCKET_UNAWARE,
                "only append unaware-bucket table support incremental clustering.");
        CoreOptions options = table.coreOptions();
        checkArgument(
                options.clusteringIncrementalEnabled(),
                "Only support incremental clustering when '%s' is true.",
                CLUSTERING_INCREMENTAL.key());

        this.maxLevel = options.numLevels();
        this.partitionComputer =
                new InternalRowPartitionComputer(
                        table.coreOptions().partitionDefaultName(),
                        table.store().partitionType(),
                        table.partitionKeys().toArray(new String[0]),
                        table.coreOptions().legacyPartitionName());

        this.specifiedPartitions = specifiedPartitions;

        // config for history partition auto clustering
        this.historyPartitionIdleTime = options.clusteringHistoryPartitionIdleTime();
        this.historyPartitionLimit = options.clusteringHistoryPartitionLimit();

        if (historyPartitionIdleTime != null) {
            // (maxLevel + 1) / 2 is used to calculate the ceiling of maxLevel divided by 2
            partitionLevelFilter = partitionMinLevel -> partitionMinLevel < (maxLevel + 1) / 2;
            this.snapshotReader = table.newSnapshotReader().dropStats();
        } else {
            this.snapshotReader =
                    table.newSnapshotReader().dropStats().withPartitionFilter(specifiedPartitions);
        }
        this.incrementalClusterStrategy =
                new IncrementalClusterStrategy(
                        table.schemaManager(),
                        options.clusteringColumns(),
                        options.maxSizeAmplificationPercent(),
                        options.sortedRunSizeRatio(),
                        options.numSortedRunCompactionTrigger());
        this.clusterCurve = options.clusteringStrategy(options.clusteringColumns().size());
        this.clusterKeys = options.clusteringColumns();
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
                                partitionComputer.generatePartValues(partition),
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
                                        entry -> {
                                            if (fullCompaction) {
                                                return incrementalClusterStrategy.pick(
                                                        maxLevel, entry.getValue(), true);
                                            } else {
                                                // if clustering is not in full mode, then specified
                                                // partitions should perform incremental clustering
                                                // and history partitions should perform full
                                                // clustering
                                                if (specifiedPartitions != null
                                                        && (!specifiedPartitions.test(
                                                                entry.getKey()))) {
                                                    return incrementalClusterStrategy.pick(
                                                            maxLevel, entry.getValue(), true);
                                                } else {
                                                    return incrementalClusterStrategy.pick(
                                                            maxLevel, entry.getValue(), false);
                                                }
                                            }
                                        }));

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
                                partitionComputer.generatePartValues(partition),
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

        if (specifiedPartitions != null
                && historyPartitionIdleTime != null
                && historyPartitionLimit > 0) {
            List<PartitionEntry> partitionEntries =
                    snapshotReader.withManifestLevelFilter(partitionLevelFilter).partitionEntries();
            // sort by last file creation time, and we will pick the oldest N partitions
            partitionEntries.sort(Comparator.comparingLong(PartitionEntry::lastFileCreationTime));
            Set<BinaryRow> historyPartitions =
                    findHistoryPartitions(partitionEntries, partitionFiles);
            partitionFiles =
                    partitionFiles.entrySet().stream()
                            .filter(
                                    entry ->
                                            specifiedPartitions.test(entry.getKey())
                                                    || historyPartitions.contains(entry.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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

    @VisibleForTesting
    protected Set<BinaryRow> findHistoryPartitions(
            List<PartitionEntry> partitionEntries,
            Map<BinaryRow, List<DataFileMeta>> partitionFiles) {
        Set<BinaryRow> partitions = new HashSet<>();

        checkArgument(historyPartitionIdleTime != null);
        long historyMilli =
                LocalDateTime.now()
                        .minus(historyPartitionIdleTime)
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();
        // Partitions that meet all the following conditions will be full clustered:
        // 1. the partition is not specified in specifiedPartitions
        // 2. the last update time for history partition should be before (currentTime -
        // partitionIdleTime)
        // 3. the min file level in partition should be less than Math.ceil(maxLevel/2)
        for (PartitionEntry partitionEntry : partitionEntries) {
            BinaryRow partition = partitionEntry.partition();
            if (!specifiedPartitions.test(partition)
                    && partitionEntry.lastFileCreationTime() <= historyMilli) {
                List<DataFileMeta> files =
                        partitionFiles.getOrDefault(partition, Collections.emptyList());
                if (!files.isEmpty()) {
                    int partitionMinLevel = maxLevel + 1;
                    for (DataFileMeta file : files) {
                        partitionMinLevel = Math.min(partitionMinLevel, file.level());
                    }
                    if (partitionLevelFilter != null
                            && partitionLevelFilter.test(partitionMinLevel)) {
                        partitions.add(partition);
                        if (partitions.size() >= historyPartitionLimit) {
                            break;
                        }
                    }
                }
            }
        }
        LOG.info(
                "Find {} history partitions for full clustering, the history partitions are {}",
                partitions.size(),
                partitions.stream()
                        .map(partitionComputer::generatePartValues)
                        .collect(Collectors.toSet()));
        return partitions;
    }

    public static List<DataFileMeta> upgrade(
            List<DataFileMeta> filesAfterCluster, int outputLevel) {
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
