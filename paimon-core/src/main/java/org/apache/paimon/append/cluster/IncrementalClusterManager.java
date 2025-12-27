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
                table.primaryKeys().isEmpty(), "only append table support incremental clustering.");
        if (table.bucketMode() == BucketMode.HASH_FIXED) {
            checkArgument(
                    !table.coreOptions().bucketAppendOrdered(),
                    "%s must be false for incremental clustering table.",
                    CoreOptions.BUCKET_APPEND_ORDERED.key());
        }
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

    public Map<BinaryRow, Map<Integer, CompactUnit>> createCompactUnits(boolean fullCompaction) {
        // 1. construct LSM structure for each bucket
        Map<BinaryRow, Map<Integer, List<LevelSortedRun>>> partitionLevels =
                constructPartitionLevels();
        if (LOG.isDebugEnabled()) {
            logForLevels(partitionLevels, partitionComputer);
        }

        // 2. pick files to be clustered for each bucket
        Map<BinaryRow, Map<Integer, CompactUnit>> units = new HashMap<>();
        partitionLevels.forEach(
                (partition, bucketLevels) -> {
                    Map<Integer, CompactUnit> bucketUnits = new HashMap<>();
                    bucketLevels.forEach(
                            (bucket, levels) -> {
                                Optional<CompactUnit> pick =
                                        incrementalClusterStrategy.pick(
                                                numLevels, levels, fullCompaction);
                                pick.ifPresent(
                                        compactUnit -> {
                                            bucketUnits.put(bucket, compactUnit);
                                            if (LOG.isDebugEnabled()) {
                                                logForCompactUnits(
                                                        partitionComputer.generatePartValues(
                                                                partition),
                                                        bucket,
                                                        compactUnit);
                                            }
                                        });
                            });
                    units.put(partition, bucketUnits);
                });

        if (historyPartitionCluster != null) {
            units.putAll(historyPartitionCluster.createHistoryCompactUnits());
        }

        return units;
    }

    public Map<BinaryRow, Map<Integer, List<LevelSortedRun>>> constructPartitionLevels() {
        List<DataSplit> dataSplits = snapshotReader.read().dataSplits();
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> partitionLevels =
                groupByPtAndBucket(dataSplits);

        return partitionLevels.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> constructBucketLevels(entry.getValue())));
    }

    public static Map<Integer, List<LevelSortedRun>> constructBucketLevels(
            Map<Integer, List<DataFileMeta>> bucketLevels) {
        return bucketLevels.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey, entry -> constructLevels(entry.getValue())));
    }

    public static List<LevelSortedRun> constructLevels(List<DataFileMeta> files) {
        List<LevelSortedRun> levels = new ArrayList<>();
        Map<Integer, List<DataFileMeta>> levelMap =
                files.stream().collect(Collectors.groupingBy(DataFileMeta::level));

        for (Map.Entry<Integer, List<DataFileMeta>> entry : levelMap.entrySet()) {
            int level = entry.getKey();
            if (level == 0) {
                for (DataFileMeta level0File : entry.getValue()) {
                    levels.add(new LevelSortedRun(level, SortedRun.fromSingle(level0File)));
                }
            } else {
                // don't need to guarantee that the files within the same sorted run are
                // non-overlapping here, so we call SortedRun.fromSorted() to avoid sorting and
                // validation
                levels.add(new LevelSortedRun(level, SortedRun.fromSorted(entry.getValue())));
            }
        }

        // sort by level
        levels.sort(Comparator.comparing(LevelSortedRun::level));
        return levels;
    }

    public static Map<BinaryRow, Map<Integer, List<DataFileMeta>>> groupByPtAndBucket(
            List<DataSplit> dataSplits) {
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> partitionLevels = new HashMap<>();
        for (DataSplit dataSplit : dataSplits) {
            BinaryRow partition = dataSplit.partition();
            Map<Integer, List<DataFileMeta>> bucketLevels = partitionLevels.get(partition);
            if (bucketLevels == null) {
                bucketLevels = new HashMap<>();
                partitionLevels.put(partition.copy(), bucketLevels);
            }
            bucketLevels
                    .computeIfAbsent(dataSplit.bucket(), k -> new ArrayList<>())
                    .addAll(dataSplit.dataFiles());
        }
        return partitionLevels;
    }

    public Map<BinaryRow, Map<Integer, Pair<List<DataSplit>, CommitMessage>>>
            toSplitsAndRewriteDvFiles(
                    Map<BinaryRow, Map<Integer, CompactUnit>> compactUnits, BucketMode bucketMode) {
        Map<BinaryRow, Map<Integer, Pair<List<DataSplit>, CommitMessage>>> result = new HashMap<>();
        boolean dvEnabled = table.coreOptions().deletionVectorsEnabled();
        if (bucketMode == BucketMode.HASH_FIXED) {
            checkArgument(
                    !dvEnabled,
                    "Clustering is not supported for fixed-bucket table enabled deletion-vector currently.");
        }

        for (BinaryRow partition : compactUnits.keySet()) {
            Map<Integer, CompactUnit> bucketUnits = compactUnits.get(partition);
            Map<Integer, Pair<List<DataSplit>, CommitMessage>> bucketSplits = new HashMap<>();
            for (Integer bucket : bucketUnits.keySet()) {
                CompactUnit unit = bucketUnits.get(bucket);
                BaseAppendDeleteFileMaintainer dvMaintainer =
                        dvEnabled ? getDvMaintainer(bucketMode, partition, bucket) : null;
                List<DataSplit> splits = new ArrayList<>();

                DataSplit.Builder builder =
                        DataSplit.builder()
                                .withPartition(partition)
                                .withBucket(bucket)
                                .withTotalBuckets(table.coreOptions().bucket())
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
                                    ((AppendDeleteFileMaintainer) dvMaintainer)
                                            .notifyRemovedDeletionVector(file.fileName());
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
                                    bucket,
                                    table.coreOptions().bucket(),
                                    DataIncrement.emptyIncrement(),
                                    new CompactIncrement(
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            newIndexFiles,
                                            deletedIndexFiles));
                }

                bucketSplits.put(bucket, Pair.of(splits, dvCommitMessage));
            }

            result.put(partition, bucketSplits);
        }

        return result;
    }

    public BaseAppendDeleteFileMaintainer getDvMaintainer(
            BucketMode bucketMode, BinaryRow partition, int bucket) {
        switch (bucketMode) {
            case HASH_FIXED:
                // TODO: support dv for hash fixed bucket table
                return null;
            case BUCKET_UNAWARE:
                return BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        table.store().newIndexFileHandler(), snapshot, partition);
            default:
                throw new UnsupportedOperationException("Unsupported bucket mode: " + bucketMode);
        }
    }

    public static List<DataFileMeta> upgrade(
            List<DataFileMeta> filesAfterCluster, int outputLevel) {
        return filesAfterCluster.stream()
                .map(file -> file.upgrade(outputLevel))
                .collect(Collectors.toList());
    }

    public static void logForLevels(
            Map<BinaryRow, Map<Integer, List<LevelSortedRun>>> partitionLevels,
            InternalRowPartitionComputer partitionComputer) {
        partitionLevels.forEach(
                (partition, bucketLevels) -> {
                    bucketLevels.forEach(
                            (bucket, levels) -> {
                                String runsInfo =
                                        levels.stream()
                                                .map(
                                                        lsr ->
                                                                String.format(
                                                                        "level-%s:%s",
                                                                        lsr.level(),
                                                                        lsr.run().files().size()))
                                                .collect(Collectors.joining(","));
                                LOG.debug(
                                        "Partition {}, bucket {} has {} runs: [{}]",
                                        partitionComputer.generatePartValues(partition),
                                        bucket,
                                        levels.size(),
                                        runsInfo);
                            });
                });
    }

    public static void logForCompactUnits(
            Map<String, String> partition, int bucket, CompactUnit compactUnit) {
        String filesInfo =
                compactUnit.files().stream()
                        .map(
                                file ->
                                        String.format(
                                                "%s,%s,%s",
                                                file.fileName(), file.level(), file.fileSize()))
                        .collect(Collectors.joining(", "));
        LOG.debug(
                "Partition {}, bucket {}, outputLevel:{}, clustered with {} files: [{}]",
                partition,
                bucket,
                compactUnit.outputLevel(),
                compactUnit.files().size(),
                filesInfo);
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
