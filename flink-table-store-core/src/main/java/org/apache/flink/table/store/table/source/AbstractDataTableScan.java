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

package org.apache.flink.table.store.table.source;

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.annotation.VisibleForTesting;
import org.apache.flink.table.store.codegen.CodeGenUtils;
import org.apache.flink.table.store.codegen.RecordComparator;
import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.manifest.FileKind;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.operation.ScanKind;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.utils.Filter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.store.file.predicate.PredicateBuilder.transformFieldMapping;

/** An abstraction layer above {@link FileStoreScan} to provide input split generation. */
public abstract class AbstractDataTableScan implements DataTableScan {

    protected final FileIO fileIO;
    private final FileStoreScan scan;
    private final TableSchema tableSchema;
    private final FileStorePathFactory pathFactory;
    private final CoreOptions options;

    private ScanKind scanKind = ScanKind.ALL;

    private RecordComparator lazyPartitionComparator;

    protected AbstractDataTableScan(
            FileIO fileIO,
            FileStoreScan scan,
            TableSchema tableSchema,
            FileStorePathFactory pathFactory,
            CoreOptions options) {
        this.fileIO = fileIO;
        this.scan = scan;
        this.tableSchema = tableSchema;
        this.pathFactory = pathFactory;
        this.options = options;
    }

    @Override
    public AbstractDataTableScan withSnapshot(long snapshotId) {
        scan.withSnapshot(snapshotId);
        return this;
    }

    @Override
    public AbstractDataTableScan withFilter(Predicate predicate) {
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
            withNonPartitionFilter(PredicateBuilder.and(nonPartitionFilters));
        }
        return this;
    }

    @Override
    public AbstractDataTableScan withKind(ScanKind scanKind) {
        this.scanKind = scanKind;
        scan.withKind(scanKind);
        return this;
    }

    @Override
    public AbstractDataTableScan withLevelFilter(Filter<Integer> levelFilter) {
        scan.withLevelFilter(levelFilter);
        return this;
    }

    @VisibleForTesting
    public AbstractDataTableScan withBucket(int bucket) {
        scan.withBucket(bucket);
        return this;
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

    @Override
    public DataFilePlan plan() {
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
                        splitGenerator(pathFactory),
                        files);
        return new DataFilePlan(snapshotId, splits);
    }

    @Override
    public DataFilePlan planOverwriteChanges() {
        withKind(ScanKind.DELTA);
        FileStoreScan.Plan plan = scan.plan();
        long snapshotId = plan.snapshotId();

        SnapshotManager snapshotManager = new SnapshotManager(fileIO, pathFactory.root());
        Snapshot snapshot = snapshotManager.snapshot(snapshotId);
        if (snapshot.commitKind() != Snapshot.CommitKind.OVERWRITE) {
            throw new IllegalStateException(
                    "Cannot plan overwrite changes from a non-overwrite snapshot.");
        }

        List<DataSplit> splits = new ArrayList<>();

        splits.addAll(
                generateSplits(
                        snapshotId,
                        true,
                        true,
                        splitGenerator(pathFactory),
                        FileStoreScan.Plan.groupByPartFiles(plan.files(FileKind.DELETE))));

        splits.addAll(
                generateSplits(
                        snapshotId,
                        true,
                        false,
                        splitGenerator(pathFactory),
                        FileStoreScan.Plan.groupByPartFiles(plan.files(FileKind.ADD))));
        return new DataFilePlan(snapshotId, splits);
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

    protected abstract SplitGenerator splitGenerator(FileStorePathFactory pathFactory);

    protected abstract void withNonPartitionFilter(Predicate predicate);

    public CoreOptions options() {
        return options;
    }

    public SnapshotManager snapshotManager() {
        return new SnapshotManager(fileIO, pathFactory.root());
    }
}
