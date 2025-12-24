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
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
        public Plan plan() {
            List<Split> splits = new ArrayList<>();
            Set<BinaryRow> completePartitions = new HashSet<>();
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
                                fileBucketPathMapping,
                                fileBranchMapping));
                completePartitions.add(dataSplit.partition());
            }
            List<BinaryRow> remainingPartitions =
                    fallbackScan.listPartitions().stream()
                            .filter(p -> !completePartitions.contains(p))
                            .collect(Collectors.toList());
            if (!remainingPartitions.isEmpty()) {
                fallbackScan.withPartitionFilter(remainingPartitions);
                List<BinaryRow> deltaPartitions = fallbackScan.listPartitions();
                deltaPartitions =
                        deltaPartitions.stream()
                                .sorted(partitionComparator)
                                .collect(Collectors.toList());
                BinaryRow maxPartition = deltaPartitions.get(deltaPartitions.size() - 1);
                Predicate snapshotPredicate =
                        ChainTableUtils.createTriangularPredicate(
                                maxPartition,
                                partitionConverter,
                                builder::equal,
                                builder::lessThan);
                mainScan.withPartitionFilter(snapshotPredicate);
                List<BinaryRow> candidateSnapshotPartitions = mainScan.listPartitions();
                candidateSnapshotPartitions =
                        candidateSnapshotPartitions.stream()
                                .sorted(partitionComparator)
                                .collect(Collectors.toList());
                Map<BinaryRow, BinaryRow> partitionMapping =
                        ChainTableUtils.findFirstLatestPartitions(
                                deltaPartitions, candidateSnapshotPartitions, partitionComparator);
                for (Map.Entry<BinaryRow, BinaryRow> partitionParis : partitionMapping.entrySet()) {
                    DataTableScan snapshotScan = chainGroupReadTable.newSnapshotScan();
                    DataTableScan deltaScan = chainGroupReadTable.newDeltaScan();
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
                                        fileBucketPathMapping,
                                        fileBranchMapping);
                        splits.add(split);
                    }
                }
            }
            return new DataFilePlan<>(splits);
        }

        @Override
        public List<PartitionEntry> listPartitionEntries() {
            return super.listPartitionEntries();
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
