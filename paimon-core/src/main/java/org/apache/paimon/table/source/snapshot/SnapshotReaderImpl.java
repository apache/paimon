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
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.Snapshot;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.PlanImpl;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.operation.FileStoreScan.Plan.groupByPartFiles;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/** Implementation of {@link SnapshotReader}. */
public class SnapshotReaderImpl implements SnapshotReader {

    private final FileStoreScan scan;
    private final TableSchema tableSchema;
    private final CoreOptions options;
    private final MergeEngine mergeEngine;
    private final boolean deletionVectors;
    private final SnapshotManager snapshotManager;
    private final ConsumerManager consumerManager;
    private final SplitGenerator splitGenerator;
    private final BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer;
    private final DefaultValueAssigner defaultValueAssigner;
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
            SplitGenerator splitGenerator,
            BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer,
            DefaultValueAssigner defaultValueAssigner,
            FileStorePathFactory pathFactory,
            String tableName,
            IndexFileHandler indexFileHandler) {
        this.scan = scan;
        this.tableSchema = tableSchema;
        this.options = options;
        this.mergeEngine = options.mergeEngine();
        this.deletionVectors = options.deletionVectorsEnabled();
        this.snapshotManager = snapshotManager;
        this.consumerManager =
                new ConsumerManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        this.splitGenerator = splitGenerator;
        this.nonPartitionFilterConsumer = nonPartitionFilterConsumer;
        this.defaultValueAssigner = defaultValueAssigner;
        this.pathFactory = pathFactory;

        this.tableName = tableName;
        this.indexFileHandler = indexFileHandler;
    }

    @Override
    public SnapshotManager snapshotManager() {
        return snapshotManager;
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
            List<String> partitionKeys = tableSchema.partitionKeys();
            RowType rowType = tableSchema.logicalPartitionType();
            PredicateBuilder predicateBuilder = new PredicateBuilder(rowType);
            List<Predicate> partitionFilters =
                    partitionSpec.entrySet().stream()
                            .map(
                                    m -> {
                                        int index = partitionKeys.indexOf(m.getKey());
                                        Object value =
                                                TypeUtils.castFromStringInternal(
                                                        m.getValue(),
                                                        rowType.getTypeAt(index),
                                                        false);
                                        return predicateBuilder.equal(index, value);
                                    })
                            .collect(Collectors.toList());
            scan.withPartitionFilter(PredicateBuilder.and(partitionFilters));
        }
        return this;
    }

    @Override
    public SnapshotReader withFilter(Predicate predicate) {
        List<String> partitionKeys = tableSchema.partitionKeys();
        int[] fieldIdxToPartitionIdx =
                tableSchema.fields().stream()
                        .mapToInt(f -> partitionKeys.indexOf(f.name()))
                        .toArray();

        List<Predicate> partitionFilters = new ArrayList<>();
        List<Predicate> nonPartitionFilters = new ArrayList<>();
        for (Predicate p :
                PredicateBuilder.splitAnd(defaultValueAssigner.handlePredicate(predicate))) {
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
    public SnapshotReader withLevelFilter(Filter<Integer> levelFilter) {
        scan.withLevelFilter(levelFilter);
        return this;
    }

    @Override
    public SnapshotReader withDataFileTimeMills(long dataFileTimeMills) {
        scan.withDataFileTimeMills(dataFileTimeMills);
        return this;
    }

    @Override
    public SnapshotReader withBucket(int bucket) {
        scan.withBucket(bucket);
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

    /** Get splits from {@link FileKind#ADD} files. */
    @Override
    public Plan read() {
        FileStoreScan.Plan plan = scan.plan();
        Long snapshotId = plan.snapshotId();

        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> files =
                groupByPartFiles(plan.files(FileKind.ADD));
        if (options.scanPlanSortPartition()) {
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> newFiles = new LinkedHashMap<>();
            files.entrySet().stream()
                    .sorted((o1, o2) -> partitionComparator().compare(o1.getKey(), o2.getKey()))
                    .forEach(entry -> newFiles.put(entry.getKey(), entry.getValue()));
            files = newFiles;
        }
        List<DataSplit> splits =
                generateSplits(
                        snapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID - 1 : snapshotId,
                        scanMode != ScanMode.ALL,
                        splitGenerator,
                        files);
        return new PlanImpl(plan.watermark(), plan.snapshotId(), (List) splits);
    }

    private List<DataSplit> generateSplits(
            long snapshotId,
            boolean isStreaming,
            SplitGenerator splitGenerator,
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> groupedDataFiles) {
        List<DataSplit> splits = new ArrayList<>();
        for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> entry :
                groupedDataFiles.entrySet()) {
            BinaryRow partition = entry.getKey();
            Map<Integer, List<DataFileMeta>> buckets = entry.getValue();
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry : buckets.entrySet()) {
                int bucket = bucketEntry.getKey();
                List<DataFileMeta> bucketFiles = bucketEntry.getValue();
                DataSplit.Builder builder =
                        DataSplit.builder()
                                .withSnapshot(snapshotId)
                                .withPartition(partition)
                                .withBucket(bucket)
                                .isStreaming(isStreaming);
                List<SplitGenerator.SplitGroup> splitGroups =
                        isStreaming
                                ? splitGenerator.splitForStreaming(bucketFiles)
                                : splitGenerator.splitForBatch(bucketFiles);

                IndexFileMeta deletionIndexFile =
                        deletionVectors
                                ? indexFileHandler
                                        .scan(snapshotId, DELETION_VECTORS_INDEX, partition, bucket)
                                        .orElse(null)
                                : null;
                for (SplitGenerator.SplitGroup splitGroup : splitGroups) {
                    List<DataFileMeta> dataFiles = splitGroup.files;
                    builder.withDataFiles(dataFiles);
                    builder.rawFiles(
                            splitGroup.rawConvertible
                                    ? convertToRawFiles(partition, bucket, dataFiles)
                                    : Collections.emptyList());
                    if (deletionVectors) {
                        builder.withDataDeletionFiles(
                                getDeletionFiles(dataFiles, deletionIndexFile));
                    }

                    splits.add(builder.build());
                }
            }
        }
        return splits;
    }

    @Override
    public List<BinaryRow> partitions() {
        return scan.readSimpleEntries().stream()
                .map(SimpleFileEntry::partition)
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public Plan readChanges() {
        withMode(ScanMode.DELTA);
        FileStoreScan.Plan plan = scan.plan();

        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> beforeFiles =
                groupByPartFiles(plan.files(FileKind.DELETE));
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> dataFiles =
                groupByPartFiles(plan.files(FileKind.ADD));

        return toChangesPlan(true, plan, plan.snapshotId() - 1, beforeFiles, dataFiles);
    }

    private Plan toChangesPlan(
            boolean isStreaming,
            FileStoreScan.Plan plan,
            long beforeSnapshotId,
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> beforeFiles,
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> dataFiles) {
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

        for (Map.Entry<BinaryRow, Set<Integer>> entry : buckets.entrySet()) {
            BinaryRow part = entry.getKey();
            for (Integer bucket : entry.getValue()) {
                List<DataFileMeta> before =
                        beforeFiles
                                .getOrDefault(part, Collections.emptyMap())
                                .getOrDefault(bucket, Collections.emptyList());
                List<DataFileMeta> data =
                        dataFiles
                                .getOrDefault(part, Collections.emptyMap())
                                .getOrDefault(bucket, Collections.emptyList());

                // deduplicate
                before.removeIf(data::remove);

                DataSplit.Builder builder =
                        DataSplit.builder()
                                .withSnapshot(plan.snapshotId())
                                .withPartition(part)
                                .withBucket(bucket)
                                .withBeforeFiles(before)
                                .withDataFiles(data)
                                .isStreaming(isStreaming);
                if (deletionVectors) {
                    IndexFileMeta beforeDeletionIndex =
                            indexFileHandler
                                    .scan(beforeSnapshotId, DELETION_VECTORS_INDEX, part, bucket)
                                    .orElse(null);
                    IndexFileMeta deletionIndex =
                            indexFileHandler
                                    .scan(plan.snapshotId(), DELETION_VECTORS_INDEX, part, bucket)
                                    .orElse(null);
                    builder.withBeforeDeletionFiles(getDeletionFiles(before, beforeDeletionIndex));
                    builder.withDataDeletionFiles(getDeletionFiles(data, deletionIndex));
                }
                splits.add(builder.build());
            }
        }

        return new PlanImpl(plan.watermark(), plan.snapshotId(), (List) splits);
    }

    @Override
    public Plan readIncrementalDiff(Snapshot before) {
        withMode(ScanMode.ALL);
        FileStoreScan.Plan plan = scan.plan();
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> dataFiles =
                groupByPartFiles(plan.files(FileKind.ADD));
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> beforeFiles =
                groupByPartFiles(scan.withSnapshot(before).plan().files(FileKind.ADD));
        return toChangesPlan(false, plan, before.id(), beforeFiles, dataFiles);
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
            List<DataFileMeta> dataFiles, @Nullable IndexFileMeta indexFileMeta) {
        List<DeletionFile> deletionFiles = new ArrayList<>(dataFiles.size());
        Map<String, Pair<Integer, Integer>> deletionRanges =
                indexFileMeta == null ? null : indexFileMeta.deletionVectorsRanges();
        for (DataFileMeta file : dataFiles) {
            if (deletionRanges != null) {
                Pair<Integer, Integer> range = deletionRanges.get(file.fileName());
                if (range != null) {
                    deletionFiles.add(
                            new DeletionFile(
                                    indexFileHandler.filePath(indexFileMeta).toString(),
                                    range.getKey(),
                                    range.getValue()));
                    continue;
                }
            }
            deletionFiles.add(null);
        }

        return deletionFiles;
    }

    private List<RawFile> convertToRawFiles(
            BinaryRow partition, int bucket, List<DataFileMeta> dataFiles) {
        String bucketPath = pathFactory.bucketPath(partition, bucket).toString();
        return dataFiles.stream()
                .map(f -> makeRawTableFile(bucketPath, f))
                .collect(Collectors.toList());
    }

    private RawFile makeRawTableFile(String bucketPath, DataFileMeta meta) {
        return new RawFile(
                bucketPath + "/" + meta.fileName(),
                0,
                meta.fileSize(),
                meta.fileFormat()
                        .map(t -> t.toString().toLowerCase())
                        .orElse(
                                new CoreOptions(tableSchema.options())
                                        .formatType()
                                        .toString()
                                        .toLowerCase()),
                meta.schemaId(),
                meta.rowCount());
    }
}
