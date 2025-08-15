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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.PlanImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.paimon.Snapshot.FIRST_SNAPSHOT_ID;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.operation.FileStoreScan.Plan.groupByPartFiles;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/** Implementation of {@link SnapshotReader}. */
public class SnapshotReaderImpl implements SnapshotReader {

    private final FileStoreScan scan;
    private final TableSchema tableSchema;
    private final CoreOptions options;
    private final boolean deletionVectors;
    private final SnapshotManager snapshotManager;
    private final ChangelogManager changelogManager;
    private final ConsumerManager consumerManager;
    private final SplitGenerator splitGenerator;
    private final BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer;
    private final FileStorePathFactory pathFactory;
    private final String tableName;
    private final IndexFileHandler indexFileHandler;

    private ScanMode scanMode = ScanMode.ALL;
    private RecordComparator lazyPartitionComparator;

    public SnapshotReaderImpl(
            FileStoreScan scan,
            TableSchema tableSchema,
            CoreOptions options,
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            SplitGenerator splitGenerator,
            BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer,
            FileStorePathFactory pathFactory,
            String tableName,
            IndexFileHandler indexFileHandler) {
        this.scan = scan;
        this.tableSchema = tableSchema;
        this.options = options;
        this.deletionVectors = options.deletionVectorsEnabled();
        this.snapshotManager = snapshotManager;
        this.changelogManager = changelogManager;
        this.consumerManager =
                new ConsumerManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch());
        this.splitGenerator = splitGenerator;
        this.nonPartitionFilterConsumer = nonPartitionFilterConsumer;
        this.pathFactory = pathFactory;

        this.tableName = tableName;
        this.indexFileHandler = indexFileHandler;
    }

    @Override
    public Integer parallelism() {
        return scan.parallelism();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return snapshotManager;
    }

    @Override
    public ChangelogManager changelogManager() {
        return changelogManager;
    }

    @Override
    public ManifestsReader manifestsReader() {
        return scan.manifestsReader();
    }

    @Override
    public List<ManifestEntry> readManifest(ManifestFileMeta manifest) {
        return scan.readManifest(manifest);
    }

    @Override
    public ConsumerManager consumerManager() {
        return consumerManager;
    }

    @Override
    public SplitGenerator splitGenerator() {
        return splitGenerator;
    }

    @Override
    public FileStorePathFactory pathFactory() {
        return pathFactory;
    }

    @Override
    public SnapshotReader withSnapshot(long snapshotId) {
        scan.withSnapshot(snapshotId);
        return this;
    }

    @Override
    public SnapshotReader withSnapshot(Snapshot snapshot) {
        scan.withSnapshot(snapshot);
        return this;
    }

    @Override
    public SnapshotReader withPartitionFilter(Map<String, String> partitionSpec) {
        if (partitionSpec != null) {
            Predicate partitionPredicate =
                    createPartitionPredicate(
                            partitionSpec,
                            tableSchema.logicalPartitionType(),
                            options.partitionDefaultName());
            scan.withPartitionFilter(partitionPredicate);
        }
        return this;
    }

    @Override
    public SnapshotReader withPartitionFilter(Predicate predicate) {
        scan.withPartitionFilter(predicate);
        return this;
    }

    @Override
    public SnapshotReader withPartitionFilter(List<BinaryRow> partitions) {
        scan.withPartitionFilter(partitions);
        return this;
    }

    @Override
    public SnapshotReader withPartitionFilter(PartitionPredicate partitionPredicate) {
        if (partitionPredicate != null) {
            scan.withPartitionFilter(partitionPredicate);
        }
        return this;
    }

    @Override
    public SnapshotReader withPartitionsFilter(List<Map<String, String>> partitions) {
        if (partitions != null) {
            scan.withPartitionsFilter(partitions);
        }
        return this;
    }

    @Override
    public SnapshotReader withFilter(Predicate predicate) {
        int[] fieldIdxToPartitionIdx =
                PredicateBuilder.fieldIdxToPartitionIdx(
                        tableSchema.logicalRowType(), tableSchema.partitionKeys());

        List<Predicate> partitionFilters = new ArrayList<>();
        List<Predicate> nonPartitionFilters = new ArrayList<>();
        for (Predicate p : PredicateBuilder.splitAnd(predicate)) {
            Optional<Predicate> mapped = transformFieldMapping(p, fieldIdxToPartitionIdx);
            if (mapped.isPresent()) {
                partitionFilters.add(mapped.get());
            } else {
                nonPartitionFilters.add(p);
            }
        }

        if (partitionFilters.size() > 0) {
            scan.withPartitionFilter(PredicateBuilder.and(partitionFilters));
        }

        if (nonPartitionFilters.size() > 0) {
            nonPartitionFilterConsumer.accept(scan, PredicateBuilder.and(nonPartitionFilters));
        }
        return this;
    }

    @Override
    public SnapshotReader withMode(ScanMode scanMode) {
        this.scanMode = scanMode;
        scan.withKind(scanMode);
        return this;
    }

    @Override
    public SnapshotReader withLevel(int level) {
        scan.withLevel(level);
        return this;
    }

    @Override
    public SnapshotReader withLevelFilter(Filter<Integer> levelFilter) {
        scan.withLevelFilter(levelFilter);
        return this;
    }

    @Override
    public SnapshotReader enableValueFilter() {
        scan.enableValueFilter();
        return this;
    }

    @Override
    public SnapshotReader withManifestEntryFilter(Filter<ManifestEntry> filter) {
        scan.withManifestEntryFilter(filter);
        return this;
    }

    @Override
    public SnapshotReader withBucket(int bucket) {
        scan.withBucket(bucket);
        return this;
    }

    @Override
    public SnapshotReader onlyReadRealBuckets() {
        scan.onlyReadRealBuckets();
        return this;
    }

    @Override
    public SnapshotReader withBucketFilter(Filter<Integer> bucketFilter) {
        scan.withBucketFilter(bucketFilter);
        return this;
    }

    @Override
    public SnapshotReader withMetricRegistry(MetricRegistry registry) {
        scan.withMetrics(new ScanMetrics(registry, tableName));
        return this;
    }

    @Override
    public SnapshotReader withDataFileNameFilter(Filter<String> fileNameFilter) {
        scan.withDataFileNameFilter(fileNameFilter);
        return this;
    }

    @Override
    public SnapshotReader dropStats() {
        scan.dropStats();
        return this;
    }

    @Override
    public SnapshotReader withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        if (splitGenerator.alwaysRawConvertible()) {
            withDataFileNameFilter(
                    file ->
                            Math.abs(file.hashCode() % numberOfParallelSubtasks)
                                    == indexOfThisSubtask);
        } else {
            withBucketFilter(bucket -> bucket % numberOfParallelSubtasks == indexOfThisSubtask);
        }
        return this;
    }

    /** Get splits from {@link FileKind#ADD} files. */
    @Override
    public Plan read() {
        FileStoreScan.Plan plan = scan.plan();
        @Nullable Snapshot snapshot = plan.snapshot();

        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> grouped =
                groupByPartFiles(plan.files(FileKind.ADD));
        if (options.scanPlanSortPartition()) {
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> sorted = new LinkedHashMap<>();
            grouped.entrySet().stream()
                    .sorted((o1, o2) -> partitionComparator().compare(o1.getKey(), o2.getKey()))
                    .forEach(entry -> sorted.put(entry.getKey(), entry.getValue()));
            grouped = sorted;
        }
        List<DataSplit> splits =
                generateSplits(snapshot, scanMode != ScanMode.ALL, splitGenerator, grouped);
        return new PlanImpl(
                plan.watermark(), snapshot == null ? null : snapshot.id(), (List) splits);
    }

    private List<DataSplit> generateSplits(
            @Nullable Snapshot snapshot,
            boolean isStreaming,
            SplitGenerator splitGenerator,
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> groupedManifestEntries) {
        List<DataSplit> splits = new ArrayList<>();
        // Read deletion indexes at once to reduce file IO
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> deletionIndexFilesMap = null;
        if (!isStreaming) {
            deletionIndexFilesMap =
                    deletionVectors && snapshot != null
                            ? indexFileHandler.scan(
                                    snapshot,
                                    DELETION_VECTORS_INDEX,
                                    groupedManifestEntries.keySet())
                            : Collections.emptyMap();
        }
        for (Map.Entry<BinaryRow, Map<Integer, List<ManifestEntry>>> entry :
                groupedManifestEntries.entrySet()) {
            BinaryRow partition = entry.getKey();
            Map<Integer, List<ManifestEntry>> buckets = entry.getValue();
            for (Map.Entry<Integer, List<ManifestEntry>> bucketEntry : buckets.entrySet()) {
                int bucket = bucketEntry.getKey();
                List<DataFileMeta> bucketFiles =
                        bucketEntry.getValue().stream()
                                .map(ManifestEntry::file)
                                .collect(Collectors.toList());
                DataSplit.Builder builder =
                        DataSplit.builder()
                                .withSnapshot(
                                        snapshot == null ? FIRST_SNAPSHOT_ID - 1 : snapshot.id())
                                .withPartition(partition)
                                .withBucket(bucket)
                                .withTotalBuckets(bucketEntry.getValue().get(0).totalBuckets())
                                .isStreaming(isStreaming);
                List<SplitGenerator.SplitGroup> splitGroups =
                        isStreaming
                                ? splitGenerator.splitForStreaming(bucketFiles)
                                : splitGenerator.splitForBatch(bucketFiles);
                for (SplitGenerator.SplitGroup splitGroup : splitGroups) {
                    List<DataFileMeta> dataFiles = splitGroup.files;
                    String bucketPath = pathFactory.bucketPath(partition, bucket).toString();
                    builder.withDataFiles(dataFiles)
                            .rawConvertible(splitGroup.rawConvertible)
                            .withBucketPath(bucketPath);
                    if (deletionVectors && deletionIndexFilesMap != null) {
                        builder.withDataDeletionFiles(
                                getDeletionFiles(
                                        dataFiles,
                                        deletionIndexFilesMap.getOrDefault(
                                                Pair.of(partition, bucket),
                                                Collections.emptyList())));
                    }

                    splits.add(builder.build());
                }
            }
        }
        return splits;
    }

    @Override
    public List<BinaryRow> partitions() {
        return scan.listPartitions();
    }

    @Override
    public List<PartitionEntry> partitionEntries() {
        return scan.readPartitionEntries();
    }

    @Override
    public List<BucketEntry> bucketEntries() {
        return scan.readBucketEntries();
    }

    @Override
    public Iterator<ManifestEntry> readFileIterator() {
        return scan.readFileIterator();
    }

    @Override
    public Plan readChanges() {
        withMode(ScanMode.DELTA);
        FileStoreScan.Plan plan = scan.plan();

        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> beforeFiles =
                groupByPartFiles(plan.files(FileKind.DELETE));
        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> dataFiles =
                groupByPartFiles(plan.files(FileKind.ADD));
        LazyField<Snapshot> beforeSnapshot =
                new LazyField<>(() -> snapshotManager.snapshot(plan.snapshot().id() - 1));
        return toChangesPlan(true, plan, beforeSnapshot, beforeFiles, dataFiles);
    }

    private Plan toChangesPlan(
            boolean isStreaming,
            FileStoreScan.Plan plan,
            LazyField<Snapshot> beforeSnapshot,
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> beforeFiles,
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> dataFiles) {
        Snapshot snapshot = plan.snapshot();
        List<DataSplit> splits = new ArrayList<>();
        Map<BinaryRow, Set<Integer>> buckets = new HashMap<>();
        beforeFiles.forEach(
                (part, bucketMap) ->
                        buckets.computeIfAbsent(part, k -> new HashSet<>())
                                .addAll(bucketMap.keySet()));
        dataFiles.forEach(
                (part, bucketMap) ->
                        buckets.computeIfAbsent(part, k -> new HashSet<>())
                                .addAll(bucketMap.keySet()));
        // Read deletion indexes at once to reduce file IO
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> beforDeletionIndexFilesMap = null;
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> deletionIndexFilesMap = null;
        if (!isStreaming) {
            beforDeletionIndexFilesMap =
                    deletionVectors
                            ? indexFileHandler.scan(
                                    beforeSnapshot.get(),
                                    DELETION_VECTORS_INDEX,
                                    beforeFiles.keySet())
                            : Collections.emptyMap();
            deletionIndexFilesMap =
                    deletionVectors
                            ? indexFileHandler.scan(
                                    snapshot, DELETION_VECTORS_INDEX, dataFiles.keySet())
                            : Collections.emptyMap();
        }

        for (Map.Entry<BinaryRow, Set<Integer>> entry : buckets.entrySet()) {
            BinaryRow part = entry.getKey();
            for (Integer bucket : entry.getValue()) {
                List<ManifestEntry> beforeEntries =
                        beforeFiles
                                .getOrDefault(part, Collections.emptyMap())
                                .getOrDefault(bucket, Collections.emptyList());
                List<ManifestEntry> dataEntries =
                        dataFiles
                                .getOrDefault(part, Collections.emptyMap())
                                .getOrDefault(bucket, Collections.emptyList());

                // deduplicate
                beforeEntries.removeIf(dataEntries::remove);

                Integer totalBuckets = null;
                if (!dataEntries.isEmpty()) {
                    totalBuckets = dataEntries.get(0).totalBuckets();
                } else if (!beforeEntries.isEmpty()) {
                    totalBuckets = beforeEntries.get(0).totalBuckets();
                }

                List<DataFileMeta> before =
                        beforeEntries.stream()
                                .map(ManifestEntry::file)
                                .collect(Collectors.toList());
                List<DataFileMeta> data =
                        dataEntries.stream().map(ManifestEntry::file).collect(Collectors.toList());

                DataSplit.Builder builder =
                        DataSplit.builder()
                                .withSnapshot(snapshot.id())
                                .withPartition(part)
                                .withBucket(bucket)
                                .withTotalBuckets(totalBuckets)
                                .withBeforeFiles(before)
                                .withDataFiles(data)
                                .isStreaming(isStreaming)
                                .withBucketPath(pathFactory.bucketPath(part, bucket).toString());
                if (deletionVectors
                        && beforDeletionIndexFilesMap != null
                        && deletionIndexFilesMap != null) {
                    builder.withBeforeDeletionFiles(
                            getDeletionFiles(
                                    before,
                                    beforDeletionIndexFilesMap.getOrDefault(
                                            Pair.of(part, bucket), Collections.emptyList())));
                    builder.withDataDeletionFiles(
                            getDeletionFiles(
                                    data,
                                    deletionIndexFilesMap.getOrDefault(
                                            Pair.of(part, bucket), Collections.emptyList())));
                }
                splits.add(builder.build());
            }
        }

        return new PlanImpl(
                plan.watermark(), snapshot == null ? null : snapshot.id(), (List) splits);
    }

    @Override
    public Plan readIncrementalDiff(Snapshot before) {
        withMode(ScanMode.ALL);
        FileStoreScan.Plan plan = scan.plan();
        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> dataFiles =
                groupByPartFiles(plan.files(FileKind.ADD));
        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> beforeFiles =
                groupByPartFiles(scan.withSnapshot(before).plan().files(FileKind.ADD));
        return toChangesPlan(false, plan, new LazyField<>(() -> before), beforeFiles, dataFiles);
    }

    private RecordComparator partitionComparator() {
        if (lazyPartitionComparator == null) {
            lazyPartitionComparator =
                    CodeGenUtils.newRecordComparator(
                            tableSchema.logicalPartitionType().getFieldTypes());
        }
        return lazyPartitionComparator;
    }

    private List<DeletionFile> getDeletionFiles(
            List<DataFileMeta> dataFiles, List<IndexFileMeta> indexFileMetas) {
        List<DeletionFile> deletionFiles = new ArrayList<>(dataFiles.size());
        Map<String, IndexFileMeta> dataFileToIndexFileMeta = new HashMap<>();
        for (IndexFileMeta indexFileMeta : indexFileMetas) {
            if (indexFileMeta.deletionVectorMetas() != null) {
                for (DeletionVectorMeta dvMeta : indexFileMeta.deletionVectorMetas().values()) {
                    dataFileToIndexFileMeta.put(dvMeta.dataFileName(), indexFileMeta);
                }
            }
        }
        for (DataFileMeta file : dataFiles) {
            IndexFileMeta indexFileMeta = dataFileToIndexFileMeta.get(file.fileName());
            if (indexFileMeta != null) {
                LinkedHashMap<String, DeletionVectorMeta> dvMetas =
                        indexFileMeta.deletionVectorMetas();
                if (dvMetas != null && dvMetas.containsKey(file.fileName())) {
                    deletionFiles.add(
                            new DeletionFile(
                                    indexFileHandler.filePath(indexFileMeta).toString(),
                                    dvMetas.get(file.fileName()).offset(),
                                    dvMetas.get(file.fileName()).length(),
                                    dvMetas.get(file.fileName()).cardinality()));
                    continue;
                }
            }
            deletionFiles.add(null);
        }

        return deletionFiles;
    }
}
