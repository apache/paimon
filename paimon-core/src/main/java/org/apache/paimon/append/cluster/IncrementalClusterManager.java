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
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
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

    private final FileStoreTable table;
    private final InternalRowPartitionComputer partitionComputer;
    private final Snapshot snapshot;
    private final SnapshotReader snapshotReader;
    private final IncrementalClusterStrategy incrementalClusterStrategy;
    private final CoreOptions.OrderType clusterCurve;
    private final List<String> clusterKeys;
    private final int numLevels;
    private final @Nullable HistoryPartitionCluster historyPartitionCluster;

    public IncrementalClusterManager(FileStoreTable table) {
        this(table, null);
    }

    public IncrementalClusterManager(
            FileStoreTable table, @Nullable PartitionPredicate specifiedPartitions) {
        checkArgument(
                table.bucketMode() == BucketMode.BUCKET_UNAWARE,
                "only append unaware-bucket table support incremental clustering.");
        this.table = table;
        CoreOptions options = table.coreOptions();
        checkArgument(
                options.clusteringIncrementalEnabled(),
                "Only support incremental clustering when '%s' is true.",
                CLUSTERING_INCREMENTAL.key());
        this.numLevels = options.numLevels();
        this.partitionComputer =
                new InternalRowPartitionComputer(
                        table.coreOptions().partitionDefaultName(),
                        table.store().partitionType(),
                        table.partitionKeys().toArray(new String[0]),
                        table.coreOptions().legacyPartitionName());
        this.snapshot = table.snapshotManager().latestSnapshot();
        this.snapshotReader =
                table.newSnapshotReader()
                        .dropStats()
                        .withPartitionFilter(specifiedPartitions)
                        .withSnapshot(snapshot);
        this.incrementalClusterStrategy =
                new IncrementalClusterStrategy(
                        table.schemaManager(),
                        options.clusteringColumns(),
                        options.maxSizeAmplificationPercent(),
                        options.sortedRunSizeRatio(),
                        options.numSortedRunCompactionTrigger());
        this.clusterCurve = options.clusteringStrategy(options.clusteringColumns().size());
        this.clusterKeys = options.clusteringColumns();
        this.historyPartitionCluster =
                HistoryPartitionCluster.create(
                        table,
                        snapshot,
                        incrementalClusterStrategy,
                        partitionComputer,
                        specifiedPartitions);
    }

    public Map<BinaryRow, CompactUnit> createCompactUnits(boolean fullCompaction) {
        // 1. construct LSM structure for each partition
        Map<BinaryRow, List<LevelSortedRun>> partitionLevels = constructLevels();
        logForPartitionLevel(partitionLevels, partitionComputer);

        // 2. pick files to be clustered for each partition
        Map<BinaryRow, CompactUnit> units = new HashMap<>();
        partitionLevels.forEach(
                (k, v) -> {
                    Optional<CompactUnit> pick =
                            incrementalClusterStrategy.pick(numLevels, v, fullCompaction);
                    pick.ifPresent(compactUnit -> units.put(k, compactUnit));
                });

        if (historyPartitionCluster != null) {
            units.putAll(historyPartitionCluster.pickForHistoryPartitions());
        }

        if (LOG.isDebugEnabled()) {
            units.forEach(
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
        return units;
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

    public static List<LevelSortedRun> constructPartitionLevels(List<DataFileMeta> partitionFiles) {
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

    public Map<BinaryRow, Pair<List<DataSplit>, CommitMessage>> toSplitsAndRewriteDvFiles(
            Map<BinaryRow, CompactUnit> compactUnits) {
        Map<BinaryRow, Pair<List<DataSplit>, CommitMessage>> result = new HashMap<>();
        boolean dvEnabled = table.coreOptions().deletionVectorsEnabled();
        for (BinaryRow partition : compactUnits.keySet()) {
            CompactUnit unit = compactUnits.get(partition);
            AppendDeleteFileMaintainer dvMaintainer =
                    dvEnabled
                            ? BaseAppendDeleteFileMaintainer.forUnawareAppend(
                                    table.store().newIndexFileHandler(), snapshot, partition)
                            : null;
            List<DataSplit> splits = new ArrayList<>();

            DataSplit.Builder builder =
                    DataSplit.builder()
                            .withPartition(partition)
                            .withBucket(0)
                            .withTotalBuckets(1)
                            .isStreaming(false);

            SplitGenerator splitGenerator = snapshotReader.splitGenerator();
            List<SplitGenerator.SplitGroup> splitGroups =
                    splitGenerator.splitForBatch(unit.files());

            for (SplitGenerator.SplitGroup splitGroup : splitGroups) {
                List<DataFileMeta> dataFiles = splitGroup.files;

                String bucketPath =
                        snapshotReader.pathFactory().bucketPath(partition, 0).toString();
                builder.withDataFiles(dataFiles)
                        .rawConvertible(splitGroup.rawConvertible)
                        .withBucketPath(bucketPath);

                if (dvMaintainer != null) {
                    List<DeletionFile> dataDeletionFiles = new ArrayList<>();
                    for (DataFileMeta file : dataFiles) {
                        DeletionFile deletionFile =
                                dvMaintainer.notifyRemovedDeletionVector(file.fileName());
                        dataDeletionFiles.add(deletionFile);
                    }
                    builder.withDataDeletionFiles(dataDeletionFiles);
                }
                splits.add(builder.build());
            }

            // generate delete dv index meta
            CommitMessage dvCommitMessage = null;
            if (dvMaintainer != null) {
                List<IndexFileMeta> newIndexFiles = new ArrayList<>();
                List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
                List<IndexManifestEntry> indexEntries = dvMaintainer.persist();
                for (IndexManifestEntry entry : indexEntries) {
                    if (entry.kind() == FileKind.ADD) {
                        newIndexFiles.add(entry.indexFile());
                    } else {
                        deletedIndexFiles.add(entry.indexFile());
                    }
                }
                dvCommitMessage =
                        new CommitMessageImpl(
                                dvMaintainer.getPartition(),
                                0,
                                table.coreOptions().bucket(),
                                DataIncrement.emptyIncrement(),
                                new CompactIncrement(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        newIndexFiles,
                                        deletedIndexFiles));
            }

            result.put(partition, Pair.of(splits, dvCommitMessage));
        }

        return result;
    }

    public static List<DataFileMeta> upgrade(
            List<DataFileMeta> filesAfterCluster, int outputLevel) {
        return filesAfterCluster.stream()
                .map(file -> file.upgrade(outputLevel))
                .collect(Collectors.toList());
    }

    public static void logForPartitionLevel(
            Map<BinaryRow, List<LevelSortedRun>> partitionLevels,
            InternalRowPartitionComputer partitionComputer) {
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
    }

    public CoreOptions.OrderType clusterCurve() {
        return clusterCurve;
    }

    public List<String> clusterKeys() {
        return clusterKeys;
    }

    public Snapshot snapshot() {
        return snapshot;
    }

    @VisibleForTesting
    HistoryPartitionCluster historyPartitionCluster() {
        return historyPartitionCluster;
    }
}
