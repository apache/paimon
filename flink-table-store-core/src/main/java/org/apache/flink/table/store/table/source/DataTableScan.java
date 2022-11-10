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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.store.file.predicate.PredicateBuilder.transformFieldMapping;

/** An abstraction layer above {@link FileStoreScan} to provide input split generation. */
public abstract class DataTableScan implements TableScan {

    private final FileStoreScan scan;
    private final TableSchema tableSchema;
    private final FileStorePathFactory pathFactory;

    private boolean isIncremental = false;

    protected DataTableScan(
            FileStoreScan scan, TableSchema tableSchema, FileStorePathFactory pathFactory) {
        this.scan = scan;
        this.tableSchema = tableSchema;
        this.pathFactory = pathFactory;
    }

    public DataTableScan withSnapshot(long snapshotId) {
        scan.withSnapshot(snapshotId);
        return this;
    }

    @Override
    public DataTableScan withFilter(Predicate predicate) {
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

    public DataTableScan withIncremental(boolean isIncremental) {
        this.isIncremental = isIncremental;
        scan.withIncremental(isIncremental);
        return this;
    }

    public DataTableScan withLevel(int level) {
        scan.withLevel(level);
        return this;
    }

    @VisibleForTesting
    public DataTableScan withBucket(int bucket) {
        scan.withBucket(bucket);
        return this;
    }

    @Override
    public DataFilePlan plan() {
        FileStoreScan.Plan plan = scan.plan();
        return new DataFilePlan(plan.snapshotId(), generateSplits(plan.groupByPartFiles()));
    }

    private List<DataSplit> generateSplits(
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> groupedDataFiles) {
        return generateSplits(isIncremental, splitGenerator(pathFactory), groupedDataFiles);
    }

    @VisibleForTesting
    public static List<DataSplit> generateSplits(
            boolean isIncremental,
            SplitGenerator splitGenerator,
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> groupedDataFiles) {
        List<DataSplit> splits = new ArrayList<>();
        for (Map.Entry<BinaryRowData, Map<Integer, List<DataFileMeta>>> entry :
                groupedDataFiles.entrySet()) {
            BinaryRowData partition = entry.getKey();
            Map<Integer, List<DataFileMeta>> buckets = entry.getValue();
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry : buckets.entrySet()) {
                int bucket = bucketEntry.getKey();
                if (isIncremental) {
                    // Don't split when incremental
                    splits.add(new DataSplit(partition, bucket, bucketEntry.getValue(), true));
                } else {
                    splitGenerator.split(bucketEntry.getValue()).stream()
                            .map(files -> new DataSplit(partition, bucket, files, false))
                            .forEach(splits::add);
                }
            }
        }
        return splits;
    }

    protected abstract SplitGenerator splitGenerator(FileStorePathFactory pathFactory);

    protected abstract void withNonPartitionFilter(Predicate predicate);

    /** Scanning plan containing snapshot ID and input splits. */
    public static class DataFilePlan implements Plan {

        @Nullable public final Long snapshotId;
        public final List<DataSplit> splits;

        @VisibleForTesting
        public DataFilePlan(@Nullable Long snapshotId, List<DataSplit> splits) {
            this.snapshotId = snapshotId;
            this.splits = splits;
        }

        @Override
        public List<Split> splits() {
            return (List) splits;
        }
    }
}
