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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.ScanKind;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

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

    private ScanKind scanKind = ScanKind.ALL;
    private RecordComparator lazyPartitionComparator;

    public SnapshotReaderImpl(
            FileStoreScan scan,
            TableSchema tableSchema,
            CoreOptions options,
            SnapshotManager snapshotManager,
            SplitGenerator splitGenerator,
            BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer) {
        this.scan = scan;
        this.tableSchema = tableSchema;
        this.options = options;
        this.snapshotManager = snapshotManager;
        this.consumerManager =
                new ConsumerManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        this.splitGenerator = splitGenerator;
        this.nonPartitionFilterConsumer = nonPartitionFilterConsumer;
    }

    @Override
    public ConsumerManager consumerManager() {
        return consumerManager;
    }

    @Override
    public SnapshotReader withSnapshot(long snapshotId) {
        scan.withSnapshot(snapshotId);
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
    public SnapshotReader withKind(ScanKind scanKind) {
        this.scanKind = scanKind;
        scan.withKind(scanKind);
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

    /** Get splits from {@link FileKind#ADD} files. */
    @Override
    public Plan read() {
        FileStoreScan.Plan plan = scan.plan();
        Long snapshotId = plan.snapshotId();

        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> files =
                FileStoreScan.Plan.groupByPartFiles(plan.files(FileKind.ADD));
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
                        scanKind != ScanKind.ALL,
                        false,
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

    /**
     * Get splits from an overwrite snapshot files. The {@link FileKind#DELETE} part will be marked
     * with reverseRowKind = true (see {@link DataSplit}).
     */
    @Override
    public Plan readOverwrittenChanges() {
        withKind(ScanKind.DELTA);
        FileStoreScan.Plan plan = scan.plan();
        long snapshotId = plan.snapshotId();

        Snapshot snapshot = snapshotManager.snapshot(snapshotId);
        if (snapshot.commitKind() != Snapshot.CommitKind.OVERWRITE) {
            throw new IllegalStateException(
                    "Cannot read overwrite splits from a non-overwrite snapshot.");
        }

        List<DataSplit> splits = new ArrayList<>();

        splits.addAll(
                generateSplits(
                        snapshotId,
                        true,
                        true,
                        splitGenerator,
                        FileStoreScan.Plan.groupByPartFiles(plan.files(FileKind.DELETE))));

        splits.addAll(
                generateSplits(
                        snapshotId,
                        true,
                        false,
                        splitGenerator,
                        FileStoreScan.Plan.groupByPartFiles(plan.files(FileKind.ADD))));

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

    private RecordComparator partitionComparator() {
        if (lazyPartitionComparator == null) {
            lazyPartitionComparator =
                    CodeGenUtils.newRecordComparator(
                            tableSchema.logicalPartitionType().getFieldTypes(),
                            "PartitionComparator");
        }
        return lazyPartitionComparator;
    }

    @VisibleForTesting
    public static List<DataSplit> generateSplits(
            long snapshotId,
            boolean isIncremental,
            boolean reverseRowKind,
            SplitGenerator splitGenerator,
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> groupedDataFiles) {
        List<DataSplit> splits = new ArrayList<>();
        for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> entry :
                groupedDataFiles.entrySet()) {
            BinaryRow partition = entry.getKey();
            Map<Integer, List<DataFileMeta>> buckets = entry.getValue();
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry : buckets.entrySet()) {
                int bucket = bucketEntry.getKey();
                if (isIncremental) {
                    // Don't split when incremental
                    splits.add(
                            new DataSplit(
                                    snapshotId,
                                    partition,
                                    bucket,
                                    bucketEntry.getValue(),
                                    true,
                                    reverseRowKind));
                } else {
                    splitGenerator.split(bucketEntry.getValue()).stream()
                            .map(
                                    files ->
                                            new DataSplit(
                                                    snapshotId,
                                                    partition,
                                                    bucket,
                                                    files,
                                                    false,
                                                    reverseRowKind))
                            .forEach(splits::add);
                }
            }
        }
        return splits;
    }
}
