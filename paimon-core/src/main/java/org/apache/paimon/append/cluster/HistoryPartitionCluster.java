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

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.append.cluster.IncrementalClusterManager.constructPartitionLevels;
import static org.apache.paimon.append.cluster.IncrementalClusterManager.logForPartitionLevel;

/** Handle historical partition for full clustering. */
public class HistoryPartitionCluster {

    private static final Logger LOG = LoggerFactory.getLogger(HistoryPartitionCluster.class);

    private final FileStoreTable table;
    private final Snapshot snapshot;
    private final IncrementalClusterStrategy incrementalClusterStrategy;
    private final InternalRowPartitionComputer partitionComputer;
    private final PartitionPredicate specifiedPartitions;
    private final Duration historyPartitionIdleTime;
    private final int historyPartitionLimit;
    private final int maxLevel;

    public HistoryPartitionCluster(
            FileStoreTable table,
            Snapshot snapshot,
            IncrementalClusterStrategy incrementalClusterStrategy,
            InternalRowPartitionComputer partitionComputer,
            PartitionPredicate specifiedPartitions,
            Duration historyPartitionIdleTime,
            int historyPartitionLimit) {
        this.table = table;
        this.snapshot = snapshot;
        this.incrementalClusterStrategy = incrementalClusterStrategy;
        this.partitionComputer = partitionComputer;
        this.specifiedPartitions = specifiedPartitions;
        this.historyPartitionIdleTime = historyPartitionIdleTime;
        this.historyPartitionLimit = historyPartitionLimit;
        this.maxLevel = table.coreOptions().numLevels() - 1;
    }

    @Nullable
    public static HistoryPartitionCluster create(
            FileStoreTable table,
            Snapshot snapshot,
            IncrementalClusterStrategy incrementalClusterStrategy,
            InternalRowPartitionComputer partitionComputer,
            @Nullable PartitionPredicate specifiedPartitions) {
        if (table.schema().partitionKeys().isEmpty()) {
            return null;
        }
        if (specifiedPartitions == null) {
            return null;
        }

        Duration idleTime = table.coreOptions().clusteringHistoryPartitionIdleTime();
        if (idleTime == null) {
            return null;
        }

        int limit = table.coreOptions().clusteringHistoryPartitionLimit();
        return new HistoryPartitionCluster(
                table,
                snapshot,
                incrementalClusterStrategy,
                partitionComputer,
                specifiedPartitions,
                idleTime,
                limit);
    }

    public Map<BinaryRow, CompactUnit> pickForHistoryPartitions() {
        Map<BinaryRow, List<LevelSortedRun>> partitionLevels =
                constructLevelsForHistoryPartitions();
        logForPartitionLevel(partitionLevels, partitionComputer);

        Map<BinaryRow, CompactUnit> units = new HashMap<>();
        partitionLevels.forEach(
                (k, v) -> {
                    Optional<CompactUnit> pick =
                            incrementalClusterStrategy.pick(maxLevel + 1, v, true);
                    pick.ifPresent(compactUnit -> units.put(k, compactUnit));
                });
        return units;
    }

    @VisibleForTesting
    public Map<BinaryRow, List<LevelSortedRun>> constructLevelsForHistoryPartitions() {
        long historyMilli =
                LocalDateTime.now()
                        .minus(historyPartitionIdleTime)
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();

        List<BinaryRow> historyPartitions =
                table.newSnapshotReader().withSnapshot(snapshot)
                        .withLevelMinMaxFilter((min, max) -> min < maxLevel)
                        .withLevelFilter(level -> level < maxLevel).partitionEntries().stream()
                        .filter(entry -> entry.lastFileCreationTime() < historyMilli)
                        .sorted(Comparator.comparingLong(PartitionEntry::lastFileCreationTime))
                        .map(PartitionEntry::partition)
                        .collect(Collectors.toList());

        // read dataFileMeta for history partitions
        List<DataSplit> historyDataSplits =
                table.newSnapshotReader()
                        .withSnapshot(snapshot)
                        .withPartitionFilter(historyPartitions)
                        .read()
                        .dataSplits();

        Map<BinaryRow, List<DataFileMeta>> historyPartitionFiles = new HashMap<>();
        for (DataSplit dataSplit : historyDataSplits) {
            historyPartitionFiles
                    .computeIfAbsent(dataSplit.partition(), k -> new ArrayList<>())
                    .addAll(dataSplit.dataFiles());
        }

        return filterPartitions(historyPartitionFiles).entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> constructPartitionLevels(entry.getValue())));
    }

    private Map<BinaryRow, List<DataFileMeta>> filterPartitions(
            Map<BinaryRow, List<DataFileMeta>> partitionFiles) {
        Map<BinaryRow, List<DataFileMeta>> result = new HashMap<>();
        partitionFiles.forEach(
                (part, files) -> {
                    if (specifiedPartitions.test(part)) {
                        // already contain in specified partitions
                        return;
                    }

                    if (result.size() < historyPartitionLimit) {
                        // in limit, can be picked
                        result.put(part, files);
                    }
                });
        LOG.info(
                "Find {} history partitions for full clustering, the history partitions are {}",
                result.size(),
                result.keySet().stream()
                        .map(partitionComputer::generatePartValues)
                        .collect(Collectors.toSet()));
        return result;
    }
}
