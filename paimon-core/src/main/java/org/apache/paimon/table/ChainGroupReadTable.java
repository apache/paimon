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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Chain table which mainly read from the snapshot branch. However, if the snapshot branch does not
 * have a partition, it will fall back to chain read.
 */
public class ChainGroupReadTable extends FallbackReadFileStoreTable {

    public ChainGroupReadTable(FileStoreTable snapshotStoreTable, FileStoreTable deltaStoreTable) {
        super(snapshotStoreTable, deltaStoreTable);
        checkArgument(snapshotStoreTable instanceof PrimaryKeyFileStoreTable);
        checkArgument(deltaStoreTable instanceof PrimaryKeyFileStoreTable);
    }

    @Override
    public DataTableScan newScan() {
        super.validateSchema();
        return new ChainTableBatchScan(
                wrapped.newScan(),
                fallback().newScan(),
                ((AbstractFileStoreTable) wrapped).tableSchema,
                this);
    }

    private DataTableScan newSnapshotScan() {
        return wrapped.newScan();
    }

    private DataTableScan newDeltaScan() {
        return fallback().newScan();
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new ChainGroupReadTable(
                wrapped.copy(dynamicOptions),
                fallback().copy(rewriteFallbackOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new ChainGroupReadTable(
                wrapped.copy(newTableSchema),
                fallback()
                        .copy(
                                newTableSchema.copy(
                                        rewriteFallbackOptions(newTableSchema.options()))));
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new ChainGroupReadTable(
                wrapped.copyWithoutTimeTravel(dynamicOptions),
                fallback().copyWithoutTimeTravel(rewriteFallbackOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return new ChainGroupReadTable(
                wrapped.copyWithLatestSchema(), fallback().copyWithLatestSchema());
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        return new ChainGroupReadTable(switchWrappedToBranch(branchName), fallback());
    }

    /** Scan implementation for {@link ChainGroupReadTable}. */
    public static class ChainTableBatchScan extends FallbackReadScan {

        private final RowDataToObjectArrayConverter partitionConverter;
        private final InternalRowPartitionComputer partitionComputer;
        private final TableSchema tableSchema;
        private final CoreOptions options;
        private final RecordComparator partitionComparator;
        private final ChainGroupReadTable chainGroupReadTable;
        private PartitionPredicate partitionPredicate;
        private Predicate dataPredicate;
        private Filter<Integer> bucketFilter;

        public ChainTableBatchScan(
                DataTableScan mainScan,
                DataTableScan fallbackScan,
                TableSchema tableSchema,
                ChainGroupReadTable chainGroupReadTable) {
            super(mainScan, fallbackScan);
            this.tableSchema = tableSchema;
            this.options = CoreOptions.fromMap(tableSchema.options());
            this.chainGroupReadTable = chainGroupReadTable;
            this.partitionConverter =
                    new RowDataToObjectArrayConverter(tableSchema.logicalPartitionType());
            this.partitionComputer =
                    new InternalRowPartitionComputer(
                            options.partitionDefaultName(),
                            tableSchema.logicalPartitionType(),
                            tableSchema.partitionKeys().toArray(new String[0]),
                            options.legacyPartitionName());
            this.partitionComparator =
                    CodeGenUtils.newRecordComparator(
                            tableSchema.logicalPartitionType().getFieldTypes());
        }

        @Override
        public ChainTableBatchScan withFilter(Predicate predicate) {
            super.withFilter(predicate);
            if (predicate != null) {
                Pair<Optional<PartitionPredicate>, List<Predicate>> pair =
                        PartitionPredicate.splitPartitionPredicatesAndDataPredicates(
                                predicate,
                                tableSchema.logicalRowType(),
                                tableSchema.partitionKeys());
                setPartitionPredicate(pair.getLeft().orElse(null));
                dataPredicate =
                        pair.getRight().isEmpty() ? null : PredicateBuilder.and(pair.getRight());
            }
            return this;
        }

        @Override
        public ChainTableBatchScan withPartitionFilter(Map<String, String> partitionSpec) {
            super.withPartitionFilter(partitionSpec);
            if (partitionSpec != null) {
                setPartitionPredicate(
                        PartitionPredicate.fromMap(
                                tableSchema.logicalPartitionType(),
                                partitionSpec,
                                options.partitionDefaultName()));
            }
            return this;
        }

        @Override
        public ChainTableBatchScan withPartitionFilter(List<BinaryRow> partitions) {
            super.withPartitionFilter(partitions);
            if (partitions != null) {
                setPartitionPredicate(
                        PartitionPredicate.fromMultiple(
                                tableSchema.logicalPartitionType(), partitions));
            }
            return this;
        }

        @Override
        public ChainTableBatchScan withPartitionsFilter(List<Map<String, String>> partitions) {
            super.withPartitionsFilter(partitions);
            if (partitions != null) {
                setPartitionPredicate(
                        PartitionPredicate.fromMaps(
                                tableSchema.logicalPartitionType(),
                                partitions,
                                options.partitionDefaultName()));
            }
            return this;
        }

        @Override
        public ChainTableBatchScan withPartitionFilter(PartitionPredicate partitionPredicate) {
            super.withPartitionFilter(partitionPredicate);
            if (partitionPredicate != null) {
                setPartitionPredicate(partitionPredicate);
            }
            return this;
        }

        @Override
        public ChainTableBatchScan withPartitionFilter(Predicate partitionPredicate) {
            super.withPartitionFilter(partitionPredicate);
            if (partitionPredicate != null) {
                setPartitionPredicate(
                        PartitionPredicate.fromPredicate(
                                tableSchema.logicalPartitionType(), partitionPredicate));
            }
            return this;
        }

        @Override
        public ChainTableBatchScan withBucketFilter(Filter<Integer> bucketFilter) {
            this.bucketFilter = bucketFilter;
            super.withBucketFilter(bucketFilter);
            return this;
        }

        /**
         * Builds a plan for chain tables.
         *
         * <p>Partitions that exist in the snapshot branch (based on partition predicates only) are
         * treated as complete and are read directly from snapshot, subject to row-level predicates.
         * Partitions that exist only in the delta branch are planned as chain splits by pairing
         * each delta partition with the latest snapshot partition at or before it (if any), so the
         * reader sees a full partition view.
         */
        @Override
        public Plan plan() {
            List<Split> splits = new ArrayList<>();
            PredicateBuilder builder = new PredicateBuilder(tableSchema.logicalPartitionType());
            for (Split split : mainScan.plan().splits()) {
                DataSplit dataSplit = (DataSplit) split;
                HashMap<String, String> fileBucketPathMapping = new HashMap<>();
                HashMap<String, String> fileBranchMapping = new HashMap<>();
                for (DataFileMeta file : dataSplit.dataFiles()) {
                    fileBucketPathMapping.put(file.fileName(), ((DataSplit) split).bucketPath());
                    fileBranchMapping.put(file.fileName(), options.scanFallbackSnapshotBranch());
                }
                splits.add(
                        new ChainSplit(
                                dataSplit.partition(),
                                dataSplit.dataFiles(),
                                fileBranchMapping,
                                fileBucketPathMapping));
            }

            Set<BinaryRow> snapshotPartitions =
                    new HashSet<>(
                            newPartitionListingScan(true, partitionPredicate).listPartitions());

            DataTableScan deltaPartitionScan = newPartitionListingScan(false, partitionPredicate);
            List<BinaryRow> deltaPartitions =
                    deltaPartitionScan.listPartitions().stream()
                            .filter(p -> !snapshotPartitions.contains(p))
                            .sorted(partitionComparator)
                            .collect(Collectors.toList());

            if (!deltaPartitions.isEmpty()) {
                BinaryRow maxPartition = deltaPartitions.get(deltaPartitions.size() - 1);
                Predicate snapshotPredicate =
                        ChainTableUtils.createTriangularPredicate(
                                maxPartition,
                                partitionConverter,
                                builder::equal,
                                builder::lessThan);
                PartitionPredicate snapshotPartitionPredicate =
                        PartitionPredicate.fromPredicate(
                                tableSchema.logicalPartitionType(), snapshotPredicate);
                DataTableScan snapshotPartitionsScan =
                        newPartitionListingScan(true, snapshotPartitionPredicate);
                List<BinaryRow> candidateSnapshotPartitions =
                        snapshotPartitionsScan.listPartitions();
                candidateSnapshotPartitions =
                        candidateSnapshotPartitions.stream()
                                .sorted(partitionComparator)
                                .collect(Collectors.toList());
                Map<BinaryRow, BinaryRow> partitionMapping =
                        ChainTableUtils.findFirstLatestPartitions(
                                deltaPartitions, candidateSnapshotPartitions, partitionComparator);
                for (Map.Entry<BinaryRow, BinaryRow> partitionParis : partitionMapping.entrySet()) {
                    DataTableScan snapshotScan = newFilteredScan(true);
                    DataTableScan deltaScan = newFilteredScan(false);
                    if (partitionParis.getValue() == null) {
                        List<Predicate> predicates = new ArrayList<>();
                        predicates.add(
                                ChainTableUtils.createTriangularPredicate(
                                        partitionParis.getKey(),
                                        partitionConverter,
                                        builder::equal,
                                        builder::lessThan));
                        predicates.add(
                                ChainTableUtils.createLinearPredicate(
                                        partitionParis.getKey(),
                                        partitionConverter,
                                        builder::equal));
                        deltaScan.withPartitionFilter(PredicateBuilder.or(predicates));
                    } else {
                        List<BinaryRow> selectedDeltaPartitions =
                                ChainTableUtils.getDeltaPartitions(
                                        partitionParis.getValue(),
                                        partitionParis.getKey(),
                                        tableSchema.partitionKeys(),
                                        tableSchema.logicalPartitionType(),
                                        options,
                                        partitionComparator,
                                        partitionComputer);
                        deltaScan.withPartitionFilter(selectedDeltaPartitions);
                    }
                    List<Split> subSplits = deltaScan.plan().splits();
                    Set<String> snapshotFileNames = new HashSet<>();
                    if (partitionParis.getValue() != null) {
                        snapshotScan.withPartitionFilter(
                                Collections.singletonList(partitionParis.getValue()));
                        List<Split> mainSubSplits = snapshotScan.plan().splits();
                        snapshotFileNames =
                                mainSubSplits.stream()
                                        .flatMap(
                                                s ->
                                                        ((DataSplit) s)
                                                                .dataFiles().stream()
                                                                        .map(
                                                                                DataFileMeta
                                                                                        ::fileName))
                                        .collect(Collectors.toSet());
                        subSplits.addAll(mainSubSplits);
                    }
                    Map<Integer, List<DataSplit>> bucketSplits = new LinkedHashMap<>();
                    for (Split split : subSplits) {
                        DataSplit dataSplit = (DataSplit) split;
                        Integer totalBuckets = dataSplit.totalBuckets();
                        checkNotNull(totalBuckets);
                        checkArgument(
                                totalBuckets == options.bucket(),
                                "Inconsistent bucket num " + dataSplit.bucket());
                        bucketSplits
                                .computeIfAbsent(dataSplit.bucket(), k -> new ArrayList<>())
                                .add(dataSplit);
                    }
                    for (Map.Entry<Integer, List<DataSplit>> entry : bucketSplits.entrySet()) {
                        HashMap<String, String> fileBucketPathMapping = new HashMap<>();
                        HashMap<String, String> fileBranchMapping = new HashMap<>();
                        List<DataSplit> splitList = entry.getValue();
                        for (DataSplit dataSplit : splitList) {
                            for (DataFileMeta file : dataSplit.dataFiles()) {
                                fileBucketPathMapping.put(file.fileName(), dataSplit.bucketPath());
                                String branch =
                                        snapshotFileNames.contains(file.fileName())
                                                ? options.scanFallbackSnapshotBranch()
                                                : options.scanFallbackDeltaBranch();
                                fileBranchMapping.put(file.fileName(), branch);
                            }
                        }
                        ChainSplit split =
                                new ChainSplit(
                                        partitionParis.getKey(),
                                        entry.getValue().stream()
                                                .flatMap(
                                                        datsSplit -> datsSplit.dataFiles().stream())
                                                .collect(Collectors.toList()),
                                        fileBranchMapping,
                                        fileBucketPathMapping);
                        splits.add(split);
                    }
                }
            }
            return new DataFilePlan<>(splits);
        }

        @Override
        public List<PartitionEntry> listPartitionEntries() {
            DataTableScan snapshotScan = newPartitionListingScan(true, partitionPredicate);
            DataTableScan deltaScan = newPartitionListingScan(false, partitionPredicate);
            List<PartitionEntry> partitionEntries =
                    new ArrayList<>(snapshotScan.listPartitionEntries());
            Set<BinaryRow> partitions =
                    partitionEntries.stream()
                            .map(PartitionEntry::partition)
                            .collect(Collectors.toSet());
            List<PartitionEntry> fallBackPartitionEntries = deltaScan.listPartitionEntries();
            fallBackPartitionEntries.stream()
                    .filter(e -> !partitions.contains(e.partition()))
                    .forEach(partitionEntries::add);
            return partitionEntries;
        }

        private void setPartitionPredicate(PartitionPredicate predicate) {
            this.partitionPredicate = predicate;
        }

        private DataTableScan newPartitionListingScan(
                boolean snapshot, PartitionPredicate scanPartitionPredicate) {
            DataTableScan scan =
                    snapshot
                            ? chainGroupReadTable.newSnapshotScan()
                            : chainGroupReadTable.newDeltaScan();
            if (scanPartitionPredicate != null) {
                scan.withPartitionFilter(scanPartitionPredicate);
            }
            return scan;
        }

        private DataTableScan newFilteredScan(boolean snapshot) {
            DataTableScan scan =
                    snapshot
                            ? chainGroupReadTable.newSnapshotScan()
                            : chainGroupReadTable.newDeltaScan();
            if (dataPredicate != null) {
                scan.withFilter(dataPredicate);
            }
            if (bucketFilter != null) {
                scan.withBucketFilter(bucketFilter);
            }
            return scan;
        }
    }

    @Override
    public InnerTableRead newRead() {
        return new Read();
    }

    private class Read implements InnerTableRead {

        private final InnerTableRead mainRead;
        private final InnerTableRead fallbackRead;

        private Read() {
            this.mainRead = wrapped.newRead();
            this.fallbackRead = fallback().newRead();
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            mainRead.withFilter(predicate);
            fallbackRead.withFilter(predicate);
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            mainRead.withReadType(readType);
            fallbackRead.withReadType(readType);
            return this;
        }

        @Override
        public InnerTableRead forceKeepDelete() {
            mainRead.forceKeepDelete();
            fallbackRead.forceKeepDelete();
            return this;
        }

        @Override
        public TableRead executeFilter() {
            mainRead.executeFilter();
            fallbackRead.executeFilter();
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            mainRead.withIOManager(ioManager);
            fallbackRead.withIOManager(ioManager);
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            checkArgument(split instanceof ChainSplit);
            return fallbackRead.createReader(split);
        }
    }
}
