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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.BiFilter;
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

import static org.apache.paimon.append.cluster.IncrementalClusterManager.constructPartitionLevels;
import static org.apache.paimon.append.cluster.IncrementalClusterManager.logForPartitionLevel;

/** Handle historical partition for full clustering. */
public class HistoryPartitionCluster {

    private static final Logger LOG = LoggerFactory.getLogger(HistoryPartitionCluster.class);
    private final InternalRowPartitionComputer partitionComputer;

    private final FileStoreTable table;
    private final IncrementalClusterStrategy incrementalClusterStrategy;
    private final int maxLevel;

    private final int historyPartitionLimit;
    @Nullable private final PartitionPredicate specifiedPartitions;
    @Nullable private final Duration historyPartitionIdleTime;
    @Nullable private final BiFilter<Integer, Integer> partitionLevelFilter;

    public HistoryPartitionCluster(
            FileStoreTable table,
            IncrementalClusterStrategy incrementalClusterStrategy,
            InternalRowPartitionComputer partitionComputer,
            int maxLevel,
            int historyPartitionLimit,
            @Nullable PartitionPredicate specifiedPartitions,
            @Nullable Duration historyPartitionIdleTime) {
        this.table = table;
        this.incrementalClusterStrategy = incrementalClusterStrategy;
        this.partitionComputer = partitionComputer;
        this.maxLevel = maxLevel;
        this.historyPartitionLimit = historyPartitionLimit;
        this.specifiedPartitions = specifiedPartitions;
        this.historyPartitionIdleTime = historyPartitionIdleTime;
        // (maxLevel + 1) / 2 is used to calculate the ceiling of maxLevel divided by 2
        this.partitionLevelFilter =
                (partitionMinLevel, partitionMaxLevel) -> partitionMinLevel < (maxLevel + 1) / 2;
    }

    public Map<BinaryRow, Optional<CompactUnit>> pickForHistoryPartitions() {
        Map<BinaryRow, List<LevelSortedRun>> partitionLevels =
                constructLevelsForHistoryPartitions();
        logForPartitionLevel(partitionLevels, partitionComputer);

        return partitionLevels.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry ->
                                        incrementalClusterStrategy.pick(
                                                maxLevel, entry.getValue(), true)));
    }

    @VisibleForTesting
    public Map<BinaryRow, List<LevelSortedRun>> constructLevelsForHistoryPartitions() {
        if (specifiedPartitions == null
                || historyPartitionIdleTime == null
                || historyPartitionLimit <= 0) {
            return Collections.emptyMap();
        }

        long historyMilli =
                LocalDateTime.now()
                        .minus(historyPartitionIdleTime)
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();
        // read partitionEntries filter by partitionLevelFilter historyPartitionIdleTime
        // sort partitionEntries by lastFileCreation time, and we will pick the oldest N partitions
        List<BinaryRow> historyPartitions =
                table.newSnapshotReader().withManifestLevelFilter(partitionLevelFilter)
                        .partitionEntries().stream()
                        .filter(entry -> entry.lastFileCreationTime() < historyMilli)
                        .sorted(Comparator.comparingLong(PartitionEntry::lastFileCreationTime))
                        .map(PartitionEntry::partition)
                        .collect(Collectors.toList());

        // read dataFileMeta for history partitions
        List<DataSplit> historyDataSplits =
                table.newSnapshotReader()
                        .withPartitionFilter(historyPartitions)
                        .read()
                        .dataSplits();
        Map<BinaryRow, List<DataFileMeta>> historyPartitionFiles = new HashMap<>();
        for (DataSplit dataSplit : historyDataSplits) {
            historyPartitionFiles
                    .computeIfAbsent(dataSplit.partition(), k -> new ArrayList<>())
                    .addAll(dataSplit.dataFiles());
        }

        // find history partitions which have low-level files
        Set<BinaryRow> selectedHistoryPartitions =
                findLowLevelPartitions(historyPartitions, historyPartitionFiles);
        historyPartitionFiles =
                historyPartitionFiles.entrySet().stream()
                        .filter(entry -> selectedHistoryPartitions.contains(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return historyPartitionFiles.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> constructPartitionLevels(entry.getValue())));
    }

    @VisibleForTesting
    protected Set<BinaryRow> findLowLevelPartitions(
            List<BinaryRow> historyPartitions, Map<BinaryRow, List<DataFileMeta>> partitionFiles) {
        Set<BinaryRow> partitions = new HashSet<>();
        // 1. the partition is not specified in specifiedPartitions
        // 2. the min file level in partition should be less than Math.ceil(maxLevel/2)
        for (BinaryRow historyPartition : historyPartitions) {
            if (specifiedPartitions != null && !specifiedPartitions.test(historyPartition)) {
                List<DataFileMeta> files =
                        partitionFiles.getOrDefault(historyPartition, Collections.emptyList());
                if (!files.isEmpty()) {
                    int partitionMinLevel = maxLevel + 1;
                    for (DataFileMeta file : files) {
                        partitionMinLevel = Math.min(partitionMinLevel, file.level());
                    }
                    if (partitionLevelFilter != null
                            && partitionLevelFilter.test(partitionMinLevel, maxLevel)) {
                        partitions.add(historyPartition);
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
}
