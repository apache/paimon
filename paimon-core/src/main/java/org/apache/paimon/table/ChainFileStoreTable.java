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

package org.apache.paimon.table;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link FileStoreTable} which implements chain table functionality. It reads from snapshot
 * branch and delta branch based on the chain table read strategy.
 */
public class ChainFileStoreTable extends FallbackReadFileStoreTable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ChainFileStoreTable.class);

    private final FileStoreTable snapshotStoreTable;
    private final FileStoreTable deltaStoreTable;

    public ChainFileStoreTable(FileStoreTable snapshotStoreTable, FileStoreTable deltaStoreTable) {
        super(snapshotStoreTable, deltaStoreTable);
        this.snapshotStoreTable = snapshotStoreTable;
        this.deltaStoreTable = deltaStoreTable;
    }

    public FileStoreTable snapshotStoreTable() {
        return snapshotStoreTable;
    }

    public FileStoreTable deltaStoreTable() {
        return deltaStoreTable;
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new ChainFileStoreTable(
                snapshotStoreTable.copy(dynamicOptions), deltaStoreTable.copy(dynamicOptions));
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new ChainFileStoreTable(
                snapshotStoreTable.copy(newTableSchema), deltaStoreTable.copy(newTableSchema));
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new ChainFileStoreTable(
                snapshotStoreTable.copyWithoutTimeTravel(dynamicOptions),
                deltaStoreTable.copyWithoutTimeTravel(dynamicOptions));
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return new ChainFileStoreTable(
                snapshotStoreTable.copyWithLatestSchema(), deltaStoreTable.copyWithLatestSchema());
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        return new ChainFileStoreTable(
                snapshotStoreTable.switchToBranch(branchName),
                deltaStoreTable.switchToBranch(branchName));
    }

    @Override
    public DataTableScan newScan() {
        return new ChainTableBatchScan(snapshotStoreTable.newScan(), deltaStoreTable.newScan());
    }

    @Override
    public InnerTableRead newRead() {
        return new ChainTableRead(snapshotStoreTable.newRead(), deltaStoreTable.newRead());
    }

    /** DataSplit for chain table. */
    public static class ChainDataSplit extends DataSplit {

        private static final long serialVersionUID = 1L;

        private final boolean isSnapshot;

        private ChainDataSplit(DataSplit dataSplit, boolean isSnapshot) {
            assign(dataSplit);
            this.isSnapshot = isSnapshot;
        }

        public boolean isSnapshot() {
            return isSnapshot;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            ChainDataSplit that = (ChainDataSplit) o;
            return isSnapshot == that.isSnapshot;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), isSnapshot);
        }

        @Override
        public String toString() {
            return "ChainDataSplit{" + "isSnapshot=" + isSnapshot + ", " + super.toString() + '}';
        }
    }

    /** Scan implementation for {@link ChainFileStoreTable}. */
    public static class ChainTableBatchScan implements DataTableScan {

        private final DataTableScan snapshotScan;
        private final DataTableScan deltaScan;

        // Cache for partition existence check to avoid repeated scans
        private Set<BinaryRow> snapshotPartitionsCache;
        private boolean partitionsCacheInitialized = false;

        public ChainTableBatchScan(DataTableScan snapshotScan, DataTableScan deltaScan) {
            this.snapshotScan = snapshotScan;
            this.deltaScan = deltaScan;
        }

        @Override
        public ChainTableBatchScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
            snapshotScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            deltaScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
            return this;
        }

        @Override
        public ChainTableBatchScan withFilter(Predicate predicate) {
            snapshotScan.withFilter(predicate);
            deltaScan.withFilter(predicate);
            return this;
        }

        @Override
        public ChainTableBatchScan withLimit(int limit) {
            snapshotScan.withLimit(limit);
            deltaScan.withLimit(limit);
            return this;
        }

        @Override
        public ChainTableBatchScan withPartitionFilter(Map<String, String> partitionSpec) {
            snapshotScan.withPartitionFilter(partitionSpec);
            deltaScan.withPartitionFilter(partitionSpec);
            return this;
        }

        @Override
        public InnerTableScan withPartitionFilter(List partitions) {
            snapshotScan.withPartitionFilter(partitions);
            deltaScan.withPartitionFilter(partitions);
            return this;
        }

        @Override
        public InnerTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
            snapshotScan.withPartitionsFilter(partitions);
            deltaScan.withPartitionsFilter(partitions);
            return this;
        }

        @Override
        public InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
            snapshotScan.withPartitionFilter(partitionPredicate);
            deltaScan.withPartitionFilter(partitionPredicate);
            return this;
        }

        @Override
        public ChainTableBatchScan withBucketFilter(Filter<Integer> bucketFilter) {
            snapshotScan.withBucketFilter(bucketFilter);
            deltaScan.withBucketFilter(bucketFilter);
            return this;
        }

        @Override
        public ChainTableBatchScan withLevelFilter(Filter<Integer> levelFilter) {
            snapshotScan.withLevelFilter(levelFilter);
            deltaScan.withLevelFilter(levelFilter);
            return this;
        }

        @Override
        public ChainTableBatchScan withMetricRegistry(MetricRegistry metricRegistry) {
            snapshotScan.withMetricRegistry(metricRegistry);
            deltaScan.withMetricRegistry(metricRegistry);
            return this;
        }

        @Override
        public InnerTableScan withTopN(TopN topN) {
            snapshotScan.withTopN(topN);
            deltaScan.withTopN(topN);
            return this;
        }

        @Override
        public InnerTableScan dropStats() {
            snapshotScan.dropStats();
            deltaScan.dropStats();
            return this;
        }

        @Override
        public InnerTableScan withBucket(int bucket) {
            snapshotScan.withBucket(bucket);
            deltaScan.withBucket(bucket);
            return this;
        }

        @Override
        public TableScan.Plan plan() {
            long startTime = System.nanoTime();

            // Optimized: Use cached partitions if available
            Set<BinaryRow> completePartitions = getSnapshotPartitions();

            // First, get all splits from snapshot branch
            List<DataSplit> snapshotSplits = new ArrayList<>();
            List<Split> snapshotPlanSplits = snapshotScan.plan().splits();

            // Pre-allocate with estimated size for better performance
            snapshotSplits.ensureCapacity(snapshotPlanSplits.size());

            for (Split split : snapshotPlanSplits) {
                DataSplit dataSplit = (DataSplit) split;
                snapshotSplits.add(new ChainDataSplit(dataSplit, true));
            }

            // Optimized: Get remaining partitions from delta branch with caching
            List<BinaryRow> remainingPartitions =
                    deltaScan.listPartitions().stream()
                            .filter(p -> !completePartitions.contains(p))
                            .collect(Collectors.toList());

            List<DataSplit> deltaSplits = new ArrayList<>();
            if (!remainingPartitions.isEmpty()) {
                // Optimized: Batch process delta partitions
                deltaScan.withPartitionFilter(remainingPartitions);
                List<Split> deltaPlanSplits = deltaScan.plan().splits();
                deltaSplits.ensureCapacity(deltaPlanSplits.size());

                for (Split split : deltaPlanSplits) {
                    deltaSplits.add(new ChainDataSplit((DataSplit) split, false));
                }
            }

            // Optimized: Combine all splits with pre-allocated capacity
            List<DataSplit> allSplits = new ArrayList<>(snapshotSplits.size() + deltaSplits.size());
            allSplits.addAll(snapshotSplits);
            allSplits.addAll(deltaSplits);

            // Performance monitoring: log scan duration
            long duration = System.nanoTime() - startTime;
            if (duration > 1_000_000_000L) { // Log if scan takes more than 1 second
                LOG.warn(
                        "Chain table scan took {} ms, snapshot splits: {}, delta splits: {}",
                        duration / 1_000_000,
                        snapshotSplits.size(),
                        deltaSplits.size());
            }

            return new DataFilePlan(allSplits);
        }

        /**
         * Get snapshot partitions with caching for performance optimization. This method caches the
         * snapshot partitions to avoid repeated expensive operations.
         */
        private Set<BinaryRow> getSnapshotPartitions() {
            if (!partitionsCacheInitialized) {
                snapshotPartitionsCache = new HashSet<>();
                for (Split split : snapshotScan.plan().splits()) {
                    DataSplit dataSplit = (DataSplit) split;
                    snapshotPartitionsCache.add(dataSplit.partition());
                }
                partitionsCacheInitialized = true;
            }
            return snapshotPartitionsCache;
        }

        @Override
        public List<PartitionEntry> listPartitionEntries() {
            List<PartitionEntry> partitionEntries =
                    new ArrayList<>(snapshotScan.listPartitionEntries());

            Set<BinaryRow> partitions =
                    partitionEntries.stream()
                            .map((PartitionEntry entry) -> entry.partition())
                            .collect(Collectors.toSet());

            List<PartitionEntry> deltaPartitionEntries = deltaScan.listPartitionEntries();
            deltaPartitionEntries.stream()
                    .filter(e -> !partitions.contains(e.partition()))
                    .forEach(partitionEntries::add);

            return partitionEntries;
        }
    }

    /** Read implementation for {@link ChainFileStoreTable}. */
    private static class ChainTableRead implements InnerTableRead {

        private final InnerTableRead snapshotRead;
        private final InnerTableRead deltaRead;

        // Performance metrics
        private long totalReadTime = 0;
        private long snapshotReadCount = 0;
        private long deltaReadCount = 0;

        private ChainTableRead(InnerTableRead snapshotRead, InnerTableRead deltaRead) {
            this.snapshotRead = snapshotRead;
            this.deltaRead = deltaRead;
        }

        /** Get performance metrics for monitoring. */
        public long getTotalReadTime() {
            return totalReadTime;
        }

        public long getSnapshotReadCount() {
            return snapshotReadCount;
        }

        public long getDeltaReadCount() {
            return deltaReadCount;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            snapshotRead.withFilter(predicate);
            deltaRead.withFilter(predicate);
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            snapshotRead.withReadType(readType);
            deltaRead.withReadType(readType);
            return this;
        }

        @Override
        public InnerTableRead forceKeepDelete() {
            snapshotRead.forceKeepDelete();
            deltaRead.forceKeepDelete();
            return this;
        }

        @Override
        public TableRead executeFilter() {
            snapshotRead.executeFilter();
            deltaRead.executeFilter();
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            snapshotRead.withIOManager(ioManager);
            deltaRead.withIOManager(ioManager);
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            long startTime = System.nanoTime();

            RecordReader<InternalRow> reader;
            if (split instanceof ChainDataSplit) {
                ChainDataSplit chainDataSplit = (ChainDataSplit) split;
                if (chainDataSplit.isSnapshot()) {
                    snapshotReadCount++;
                    reader = snapshotRead.createReader(chainDataSplit);
                } else {
                    deltaReadCount++;
                    reader = deltaRead.createReader(chainDataSplit);
                }
            } else {
                // Fallback to snapshot read
                DataSplit dataSplit = (DataSplit) split;
                snapshotReadCount++;
                reader = snapshotRead.createReader(dataSplit);
            }

            // Track read time
            long duration = System.nanoTime() - startTime;
            totalReadTime += duration;
            
            // Log performance warning if reader creation is slow
            if (duration > 100_000_000L) { // Log if creation takes more than 100ms
                LOG.warn(
                        "Chain table reader creation took {} ms for split type: {}",
                        duration / 1_000_000,
                        split.getClass().getSimpleName());
            }

            return reader;
        }

        @Override
        public InnerTableRead withTopN(TopN topN) {
            snapshotRead.withTopN(topN);
            deltaRead.withTopN(topN);
            return this;
        }

        @Override
        public InnerTableRead withLimit(int limit) {
            snapshotRead.withLimit(limit);
            deltaRead.withLimit(limit);
            return this;
        }

        @Override
        public InnerTableRead withMetricRegistry(MetricRegistry registry) {
            snapshotRead.withMetricRegistry(registry);
            deltaRead.withMetricRegistry(registry);
            return this;
        }
    }
}
