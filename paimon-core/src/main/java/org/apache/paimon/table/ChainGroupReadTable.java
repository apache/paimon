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
import org.apache.paimon.utils.ChainPartitionProjector;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.Filter;
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
        super(snapshotStoreTable, deltaStoreTable, true);
        checkArgument(snapshotStoreTable instanceof PrimaryKeyFileStoreTable);
        checkArgument(deltaStoreTable instanceof PrimaryKeyFileStoreTable);
    }

    @Override
    public DataTableScan newScan() {
        super.validateSchema();
        return new ChainTableBatchScan(
                wrapped.newScan(),
                other().newScan(),
                ((AbstractFileStoreTable) wrapped).tableSchema,
                this);
    }

    private DataTableScan newSnapshotScan() {
        return wrapped.newScan();
    }

    private DataTableScan newDeltaScan() {
        return other().newScan();
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new ChainGroupReadTable(
                wrapped.copy(dynamicOptions), other().copy(rewriteOtherOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new ChainGroupReadTable(
                wrapped.copy(newTableSchema),
                other().copy(newTableSchema.copy(rewriteOtherOptions(newTableSchema.options()))));
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new ChainGroupReadTable(
                wrapped.copyWithoutTimeTravel(dynamicOptions),
                other().copyWithoutTimeTravel(rewriteOtherOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return new ChainGroupReadTable(
                wrapped.copyWithLatestSchema(), other().copyWithLatestSchema());
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        return new ChainGroupReadTable(switchWrappedToBranch(branchName), other());
    }

    /** Scan implementation for {@link ChainGroupReadTable}. */
    public static class ChainTableBatchScan extends FallbackReadScan {

        private final RowDataToObjectArrayConverter partitionConverter;
        private final CoreOptions options;
        private final RecordComparator partitionComparator;
        private final ChainGroupReadTable chainGroupReadTable;
        private final RecordComparator chainPartitionComparator;
        private final ChainPartitionProjector partitionProjector;
        private Predicate dataPredicate;
        private Filter<Integer> bucketFilter;

        public ChainTableBatchScan(
                DataTableScan mainScan,
                DataTableScan fallbackScan,
                TableSchema tableSchema,
                ChainGroupReadTable chainGroupReadTable) {
            super(
                    mainScan,
                    fallbackScan,
                    chainGroupReadTable.wrapped,
                    chainGroupReadTable.other(),
                    tableSchema);
            this.options = CoreOptions.fromMap(tableSchema.options());
            this.chainGroupReadTable = chainGroupReadTable;
            this.partitionConverter =
                    new RowDataToObjectArrayConverter(tableSchema.logicalPartitionType());
            this.partitionComparator =
                    CodeGenUtils.newRecordComparator(
                            tableSchema.logicalPartitionType().getFieldTypes());
            List<String> chainKeys =
                    ChainTableUtils.chainPartitionKeys(options, tableSchema.partitionKeys());
            int chainFieldCount = chainKeys.size();
            // full partition comparator
            this.partitionProjector =
                    new ChainPartitionProjector(
                            tableSchema.logicalPartitionType(), chainFieldCount);
            // chain dimension comparator (compares only the last chainFieldCount fields)
            this.chainPartitionComparator =
                    CodeGenUtils.newRecordComparator(
                            partitionProjector.chainPartitionType().getFieldTypes());
        }

        @Override
        public ChainTableBatchScan withFilter(Predicate predicate) {
            super.withFilter(predicate);
            if (predicate == null) {
                dataPredicate = null;
            } else {
                Pair<Optional<PartitionPredicate>, List<Predicate>> pair =
                        PartitionPredicate.splitPartitionPredicatesAndDataPredicates(
                                predicate,
                                tableSchema.logicalRowType(),
                                tableSchema.partitionKeys());
                dataPredicate =
                        pair.getRight().isEmpty() ? null : PredicateBuilder.and(pair.getRight());
            }
            return this;
        }

        @Override
        public ChainTableBatchScan withPartitionFilter(Map<String, String> partitionSpec) {
            super.withPartitionFilter(partitionSpec);
            return this;
        }

        @Override
        public ChainTableBatchScan withPartitionFilter(List<BinaryRow> partitions) {
            super.withPartitionFilter(partitions);
            return this;
        }

        @Override
        public ChainTableBatchScan withPartitionsFilter(List<Map<String, String>> partitions) {
            super.withPartitionsFilter(partitions);
            return this;
        }

        @Override
        public ChainTableBatchScan withPartitionFilter(PartitionPredicate partitionPredicate) {
            super.withPartitionFilter(partitionPredicate);
            return this;
        }

        @Override
        public ChainTableBatchScan withPartitionFilter(Predicate partitionPredicate) {
            super.withPartitionFilter(partitionPredicate);
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
         * <p>Partitions that exist in the snapshot branch (matched via the user's partition
         * predicate) are treated as complete and are read directly from snapshot, subject to
         * row-level predicates.
         *
         * <p>Partitions that exist only in the delta branch are planned group-by-group:
         *
         * <ol>
         *   <li>Delta partitions are grouped by their <em>group</em> key prefix (the non-chain
         *       partition fields). When {@code chain-table.chain-partition-keys} is not configured,
         *       all partitions fall into a single implicit group.
         *   <li>For each group the nearest earlier snapshot partition is used as an <em>anchor</em>
         *       for the chain merge. The anchor search uses a targeted predicate (group fields
         *       exact-match AND chain &lt; maxChainInGroup) that intentionally bypasses the user's
         *       partition predicate, so the correct anchor is found even when the user queries a
         *       future or non-existent partition.
         *   <li>A {@link ChainSplit} is built that merges the anchor snapshot data with the delta
         *       data in the range {@code (anchorChain, queryChain]}, giving the reader a complete
         *       view of the queried partition.
         * </ol>
         */
        @Override
        public Plan plan() {
            List<Split> splits = new ArrayList<>();
            PartitionPredicate partitionPredicate = getPartitionPredicate();
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
                            newChainPartitionListingScan(true, partitionPredicate)
                                    .listPartitions());

            DataTableScan deltaPartitionScan =
                    newChainPartitionListingScan(false, partitionPredicate);
            List<BinaryRow> deltaPartitions =
                    deltaPartitionScan.listPartitions().stream()
                            .filter(p -> !snapshotPartitions.contains(p))
                            .sorted(partitionComparator)
                            .collect(Collectors.toList());

            if (!deltaPartitions.isEmpty()) {
                // Group delta partitions by group key.
                // When groupFieldCount == 0, extractGroupPartition returns a zero-field BinaryRow,
                // so all partitions are placed in a single group.
                Map<BinaryRow, List<BinaryRow>> groupedDeltaPartitions = new LinkedHashMap<>();
                for (BinaryRow partition : deltaPartitions) {
                    BinaryRow groupKey = partitionProjector.extractGroupPartition(partition);
                    groupedDeltaPartitions
                            .computeIfAbsent(groupKey, k -> new ArrayList<>())
                            .add(partition);
                }

                for (List<BinaryRow> deltaPartitionsInGroup : groupedDeltaPartitions.values()) {

                    // Sort delta by chain dimension ascending.
                    // chainPartitionForCompare avoids copying BinaryRow in the comparator hot path.
                    deltaPartitionsInGroup.sort(
                            (a, b) ->
                                    chainPartitionComparator.compare(
                                            partitionProjector.chainPartitionForCompare(a),
                                            partitionProjector.chainPartitionForCompare(b)));

                    // Build a targeted snapshot-anchor predicate:
                    //   group fields exact-match  AND  chain < maxChainInGroup
                    // When groupFieldCount == 0 this degenerates to the plain triangular predicate
                    // (equivalent to the former planWithoutGroupPartition snapshot listing).
                    BinaryRow maxDeltaInGroup =
                            deltaPartitionsInGroup.get(deltaPartitionsInGroup.size() - 1);
                    Predicate snapshotSearchPred =
                            ChainTableUtils.createGroupChainPredicate(
                                    maxDeltaInGroup,
                                    partitionConverter,
                                    partitionProjector.groupFieldCount(),
                                    builder::equal,
                                    builder::lessThan);
                    PartitionPredicate snapshotAnchorPredicate =
                            PartitionPredicate.fromPredicate(
                                    tableSchema.logicalPartitionType(), snapshotSearchPred);

                    // List snapshot partitions for this group, sorted by chain dimension.
                    List<BinaryRow> snapshotPartitionsInGroup =
                            newChainPartitionListingScan(true, snapshotAnchorPredicate)
                                    .listPartitions().stream()
                                    .sorted(
                                            (a, b) ->
                                                    chainPartitionComparator.compare(
                                                            partitionProjector
                                                                    .chainPartitionForCompare(a),
                                                            partitionProjector
                                                                    .chainPartitionForCompare(b)))
                                    .collect(Collectors.toList());

                    // Find delta → snapshot mapping (for each delta partition, find the nearest
                    // earlier
                    // snapshot)
                    // Note: uses chainPartitionComparator, comparing only the chain dimension
                    Map<BinaryRow, BinaryRow> partitionMapping =
                            ChainTableUtils.findFirstLatestPartitionsWithProjector(
                                    deltaPartitionsInGroup,
                                    snapshotPartitionsInGroup,
                                    chainPartitionComparator,
                                    partitionProjector);

                    for (Map.Entry<BinaryRow, BinaryRow> partitionPairs :
                            partitionMapping.entrySet()) {
                        BinaryRow deltaPartition = partitionPairs.getKey();
                        BinaryRow snapshotPartition = partitionPairs.getValue();

                        DataTableScan snapshotScan = newFilteredScan(true);
                        DataTableScan deltaScan = newFilteredScan(false);

                        if (snapshotPartition == null) {
                            List<Predicate> predicates = new ArrayList<>();
                            predicates.add(
                                    ChainTableUtils.createGroupChainPredicate(
                                            deltaPartition,
                                            partitionConverter,
                                            partitionProjector.groupFieldCount(),
                                            builder::equal,
                                            builder::lessThan));
                            predicates.add(
                                    ChainTableUtils.createLinearPredicate(
                                            deltaPartition, partitionConverter, builder::equal));
                            deltaScan.withPartitionFilter(PredicateBuilder.or(predicates));
                        } else {
                            List<BinaryRow> selectedDeltaPartitions =
                                    ChainTableUtils.getDeltaPartitionsWithProjector(
                                            snapshotPartition,
                                            deltaPartition,
                                            options,
                                            chainPartitionComparator,
                                            partitionProjector);
                            deltaScan.withPartitionFilter(selectedDeltaPartitions);
                        }

                        List<Split> subSplits = deltaScan.plan().splits();
                        Set<String> snapshotFileNames = new HashSet<>();
                        if (partitionPairs.getValue() != null) {
                            snapshotScan.withPartitionFilter(
                                    Collections.singletonList(partitionPairs.getValue()));
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
                        Integer bucketInAll = null;
                        for (Split split : subSplits) {
                            DataSplit dataSplit = (DataSplit) split;
                            Integer totalBuckets = dataSplit.totalBuckets();
                            checkNotNull(totalBuckets);
                            if (bucketInAll == null) {
                                bucketInAll = totalBuckets;
                            } else {
                                checkArgument(
                                        totalBuckets == bucketInAll,
                                        "Inconsistent bucket num " + dataSplit.bucket());
                            }

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
                                    fileBucketPathMapping.put(
                                            file.fileName(), dataSplit.bucketPath());
                                    String branch =
                                            snapshotFileNames.contains(file.fileName())
                                                    ? options.scanFallbackSnapshotBranch()
                                                    : options.scanFallbackDeltaBranch();
                                    fileBranchMapping.put(file.fileName(), branch);
                                }
                            }
                            ChainSplit split =
                                    new ChainSplit(
                                            partitionPairs.getKey(),
                                            entry.getValue().stream()
                                                    .flatMap(
                                                            dataSplit ->
                                                                    dataSplit.dataFiles().stream())
                                                    .collect(Collectors.toList()),
                                            fileBranchMapping,
                                            fileBucketPathMapping);
                            splits.add(split);
                        }
                    }
                }
            }
            return new DataFilePlan<>(splits);
        }

        @Override
        public List<PartitionEntry> listPartitionEntries() {
            PartitionPredicate partitionPredicate = getPartitionPredicate();
            DataTableScan snapshotScan = newChainPartitionListingScan(true, partitionPredicate);
            DataTableScan deltaScan = newChainPartitionListingScan(false, partitionPredicate);
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

        private DataTableScan newChainPartitionListingScan(
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
            this.fallbackRead = other().newRead();
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
