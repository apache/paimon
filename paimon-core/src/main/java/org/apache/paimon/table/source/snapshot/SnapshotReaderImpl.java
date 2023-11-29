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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
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

import static org.apache.paimon.operation.FileStoreScan.Plan.groupByPartFiles;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/** Implementation of {@link SnapshotReader}. */
public class SnapshotReaderImpl implements SnapshotReader {

    private final FileStoreScan scan;
    private final TableSchema tableSchema;
    private final CoreOptions options;
    private final SnapshotManager snapshotManager;
    private final ConsumerManager consumerManager;
    private final SplitGenerator splitGenerator;
    private final BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer;
    private final DefaultValueAssigner defaultValueAssigner;
    private final FileStorePathFactory pathFactory;

    private ScanMode scanMode = ScanMode.ALL;
    private RecordComparator lazyPartitionComparator;

    private final String tableName;

    public SnapshotReaderImpl(
            FileStoreScan scan,
            TableSchema tableSchema,
            CoreOptions options,
            SnapshotManager snapshotManager,
            SplitGenerator splitGenerator,
            BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer,
            DefaultValueAssigner defaultValueAssigner,
            FileStorePathFactory pathFactory,
            String tableName) {
        this.scan = scan;
        this.tableSchema = tableSchema;
        this.options = options;
        this.snapshotManager = snapshotManager;
        this.consumerManager =
                new ConsumerManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        this.splitGenerator = splitGenerator;
        this.nonPartitionFilterConsumer = nonPartitionFilterConsumer;
        this.defaultValueAssigner = defaultValueAssigner;
        this.pathFactory = pathFactory;

        this.tableName = tableName;
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
        return new Plan() {
            @Nullable
            @Override
            public Long watermark() {
                return plan.watermark();
            }

            @Nullable
            @Override
            public Long snapshotId() {
                return plan.snapshotId();
            }

            @Override
            public List<Split> splits() {
                return (List) splits;
            }
        };
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
                List<List<DataFileMeta>> splitGroups =
                        isStreaming
                                ? splitGenerator.splitForStreaming(bucketFiles)
                                : splitGenerator.splitForBatch(bucketFiles);
                for (List<DataFileMeta> dataFiles : splitGroups) {
                    splits.add(
                            builder.withDataFiles(dataFiles)
                                    .rawFiles(convertToRawFiles(partition, bucket, dataFiles))
                                    .build());
                }
            }
        }
        return splits;
    }

    @Override
    public List<BinaryRow> partitions() {
        List<ManifestEntry> entryList = scan.plan().files();

        return entryList.stream()
                .collect(
                        Collectors.groupingBy(
                                ManifestEntry::partition,
                                LinkedHashMap::new,
                                Collectors.reducing((a, b) -> b)))
                .values()
                .stream()
                .map(Optional::get)
                .map(ManifestEntry::partition)
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

        return toChangesPlan(true, plan, beforeFiles, dataFiles);
    }

    private Plan toChangesPlan(
            boolean isStreaming,
            FileStoreScan.Plan plan,
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

                DataSplit split =
                        DataSplit.builder()
                                .withSnapshot(plan.snapshotId())
                                .withPartition(part)
                                .withBucket(bucket)
                                .withBeforeFiles(before)
                                .withDataFiles(data)
                                .isStreaming(isStreaming)
                                .rawFiles(convertToRawFiles(part, bucket, data))
                                .build();
                splits.add(split);
            }
        }

        return new Plan() {
            @Nullable
            @Override
            public Long watermark() {
                return plan.watermark();
            }

            @Nullable
            @Override
            public Long snapshotId() {
                return plan.snapshotId();
            }

            @Override
            public List<Split> splits() {
                return (List) splits;
            }
        };
    }

    @Override
    public Plan readIncrementalDiff(Snapshot before) {
        withMode(ScanMode.ALL);
        FileStoreScan.Plan plan = scan.plan();
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> dataFiles =
                groupByPartFiles(plan.files(FileKind.ADD));
        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> beforeFiles =
                groupByPartFiles(scan.withSnapshot(before).plan().files(FileKind.ADD));
        return toChangesPlan(false, plan, beforeFiles, dataFiles);
    }

    private RecordComparator partitionComparator() {
        if (lazyPartitionComparator == null) {
            lazyPartitionComparator =
                    CodeGenUtils.newRecordComparator(
                            tableSchema.logicalPartitionType().getFieldTypes(),
                            "PartitionComparator");
        }
        return lazyPartitionComparator;
    }

    private List<RawFile> convertToRawFiles(
            BinaryRow partition, int bucket, List<DataFileMeta> dataFiles) {
        String bucketPath = pathFactory.bucketPath(partition, bucket).toString();

        // bucket with only one file can be returned
        if (dataFiles.size() == 1) {
            return Collections.singletonList(makeRawTableFile(bucketPath, dataFiles.get(0)));
        }

        // append only files can be returned
        if (tableSchema.primaryKeys().isEmpty()) {
            return makeRawTableFiles(bucketPath, dataFiles);
        }

        // bucket containing only one level (except level 0) can be returned
        Set<Integer> levels =
                dataFiles.stream().map(DataFileMeta::level).collect(Collectors.toSet());
        if (levels.size() == 1 && !levels.contains(0)) {
            return makeRawTableFiles(bucketPath, dataFiles);
        }

        return Collections.emptyList();
    }

    private List<RawFile> makeRawTableFiles(String bucketPath, List<DataFileMeta> dataFiles) {
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
                meta.schemaId());
    }
}
