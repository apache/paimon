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
import org.apache.paimon.partition.InternalRowPartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.ChainDataSplit;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** A specific implementation about {@link FallbackReadFileStoreTable} for chain table. */
public class ChainFileStoreTable extends FallbackReadFileStoreTable {

    private static final Logger LOG = LoggerFactory.getLogger(ChainFileStoreTable.class);

    public ChainFileStoreTable(
            AbstractFileStoreTable snapshotStoreTable, AbstractFileStoreTable deltaStoreTable) {
        super(snapshotStoreTable, deltaStoreTable);
    }

    @Override
    public DataTableScan newScan() {
        super.validateSchema();
        return new ChainTableBatchScan(
                wrapped.newScan(),
                fallback.newScan(),
                ((AbstractFileStoreTable) wrapped).tableSchema);
    }

    public FileStoreTable fallback() {
        return fallback;
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new ChainFileStoreTable(
                (AbstractFileStoreTable) wrapped.copy(dynamicOptions),
                (AbstractFileStoreTable) fallback.copy(rewriteFallbackOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new ChainFileStoreTable(
                (AbstractFileStoreTable) wrapped.copy(newTableSchema),
                (AbstractFileStoreTable)
                        fallback.copy(
                                newTableSchema.copy(
                                        rewriteFallbackOptions(newTableSchema.options()))));
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new ChainFileStoreTable(
                (AbstractFileStoreTable) wrapped.copyWithoutTimeTravel(dynamicOptions),
                (AbstractFileStoreTable)
                        fallback.copyWithoutTimeTravel(rewriteFallbackOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return new ChainFileStoreTable(
                (AbstractFileStoreTable) wrapped.copyWithLatestSchema(),
                (AbstractFileStoreTable) fallback.copyWithLatestSchema());
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        return new ChainFileStoreTable(
                (AbstractFileStoreTable) switchWrappedToBranch(branchName),
                (AbstractFileStoreTable) fallback);
    }

    /** Scan implementation for {@link ChainFileStoreTable}. */
    public static class ChainTableBatchScan extends FallbackReadScan {

        private final RowDataToObjectArrayConverter partitionConverter;
        private final InternalRowPartitionComputer partitionComputer;
        private final TableSchema tableSchema;
        private final CoreOptions options;
        private final RecordComparator partitionComparator;

        public ChainTableBatchScan(
                DataTableScan mainScan, DataTableScan fallbackScan, TableSchema tableSchema) {
            super(mainScan, fallbackScan);
            this.tableSchema = tableSchema;
            this.options = CoreOptions.fromMap(tableSchema.options());
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
            List<DataSplit> splits = new ArrayList<>();
            Set<BinaryRow> completePartitions = new HashSet<>();
            for (Split split : mainScan.plan().splits()) {
                DataSplit dataSplit = (DataSplit) split;
                HashMap<String, String> fileBucketPathMapping = new HashMap<>();
                HashMap<String, String> fileBranchMapping = new HashMap<>();
                for (DataFileMeta file : dataSplit.dataFiles()) {
                    fileBucketPathMapping.put(file.fileName(), ((DataSplit) split).bucketPath());
                    fileBranchMapping.put(file.fileName(), options.scanFallbackSnapshotBranch());
                }
                splits.add(
                        new ChainDataSplit(
                                (DataSplit) split,
                                ((DataSplit) split).partition(),
                                fileBucketPathMapping,
                                fileBranchMapping,
                                true));
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
                                .sorted((o1, o2) -> partitionComparator.compare(o1, o2))
                                .collect(Collectors.toList());
                BinaryRow maxPartition = deltaPartitions.get(deltaPartitions.size() - 1);
                Predicate snapshotPredicate =
                        partitionConverter.createLessThanPredicate(maxPartition, false);
                mainScan.withPartitionFilter(snapshotPredicate);
                List<BinaryRow> candidateSnapshotPartitions = mainScan.listPartitions();
                candidateSnapshotPartitions =
                        candidateSnapshotPartitions.stream()
                                .sorted((o1, o2) -> partitionComparator.compare(o1, o2))
                                .collect(Collectors.toList());
                Map<BinaryRow, BinaryRow> partitionMapping =
                        InternalRowPartitionUtils.findFirstLatestPartitions(
                                deltaPartitions, candidateSnapshotPartitions, partitionComparator);
                for (Map.Entry<BinaryRow, BinaryRow> partitionParis : partitionMapping.entrySet()) {
                    if (partitionParis.getValue() == null) {
                        Predicate historyPredicate =
                                partitionConverter.createLessThanPredicate(
                                        partitionParis.getKey(), true);
                        fallbackScan.withPartitionFilter(historyPredicate);
                    } else {
                        List<BinaryRow> selectedDeltaPartitions =
                                InternalRowPartitionUtils.getDeltaPartitions(
                                        partitionParis.getValue(),
                                        partitionParis.getKey(),
                                        tableSchema.partitionKeys(),
                                        tableSchema.logicalPartitionType(),
                                        options,
                                        partitionComparator,
                                        partitionComputer);
                        fallbackScan.withPartitionFilter(selectedDeltaPartitions);
                    }
                    List<Split> subSplits = fallbackScan.plan().splits();
                    boolean allSnapshotSplit = subSplits.isEmpty();
                    Set<String> snapshotFileNames = new HashSet<>();
                    if (partitionParis.getValue() != null) {
                        mainScan.withPartitionFilter(Arrays.asList(partitionParis.getValue()));
                        List<Split> mainSubSplits = mainScan.plan().splits();
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

                        ChainDataSplit chainDataSplit =
                                new ChainDataSplit(
                                        partitionParis.getKey(),
                                        entry.getKey(),
                                        entry.getValue(),
                                        fileBucketPathMapping,
                                        fileBranchMapping,
                                        allSnapshotSplit);
                        splits.add(chainDataSplit);
                    }
                }
            }
            return new DataFilePlan(splits);
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
            this.fallbackRead = fallback.newRead();
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
            Preconditions.checkArgument(split instanceof ChainDataSplit);
            if (((ChainDataSplit) split).isAllSnapshotSplit()) {
                return mainRead.createReader(split);
            } else {
                return fallbackRead.createReader(split);
            }
        }
    }
}
