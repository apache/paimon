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
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.partition.PartitionPredicate.createBinaryPartitions;

/**
 * Validator for chain table partition drop.
 *
 * <p>Checks if a partition can be dropped based on the chain table's snapshot partition
 * dependencies.
 */
public class ChainPartitionDropValidator implements PartitionValidator {

    private static final Logger LOG = LoggerFactory.getLogger(ChainPartitionDropValidator.class);

    private final FileStoreTable table;
    private final CoreOptions coreOptions;

    public ChainPartitionDropValidator(FileStoreTable table) {
        this.table = table;
        this.coreOptions = table.coreOptions();
    }

    @Override
    public void validatePartitionDrop(List<Map<String, String>> partitionSpecs) {
        if (!ChainTableUtils.isScanFallbackSnapshotBranch(coreOptions)) {
            return;
        }
        FileStoreTable candidateTable = table;
        if (table instanceof FallbackReadFileStoreTable) {
            candidateTable =
                    ((ChainGroupReadTable) ((FallbackReadFileStoreTable) table).fallback())
                            .wrapped();
        }
        FileStoreTable deltaTable =
                candidateTable.switchToBranch(coreOptions.scanFallbackDeltaBranch());
        List<BinaryRow> partitions =
                createBinaryPartitions(
                        partitionSpecs,
                        table.schema().logicalPartitionType(),
                        coreOptions.partitionDefaultName());
        RowDataToObjectArrayConverter partitionConverter =
                new RowDataToObjectArrayConverter(table.schema().logicalPartitionType());
        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        coreOptions.partitionDefaultName(),
                        table.schema().logicalPartitionType(),
                        table.schema().partitionKeys().toArray(new String[0]),
                        coreOptions.legacyPartitionName());
        RecordComparator partitionComparator =
                CodeGenUtils.newRecordComparator(
                        table.schema().logicalPartitionType().getFieldTypes());
        List<BinaryRow> snapshotPartitions =
                table.newSnapshotReader().partitionEntries().stream()
                        .map(PartitionEntry::partition)
                        .sorted(partitionComparator)
                        .collect(Collectors.toList());
        SnapshotReader deltaSnapshotReader = deltaTable.newSnapshotReader();
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalPartitionType());
        for (BinaryRow partition : partitions) {
            Optional<BinaryRow> preSnapshotPartition =
                    findPreSnapshotPartition(snapshotPartitions, partition, partitionComparator);
            Optional<BinaryRow> nextSnapshotPartition =
                    findNextSnapshotPartition(snapshotPartitions, partition, partitionComparator);
            Predicate deltaFollowingPredicate =
                    ChainTableUtils.createTriangularPredicate(
                            partition, partitionConverter, builder::equal, builder::greaterThan);
            List<BinaryRow> deltaFollowingPartitions =
                    deltaSnapshotReader.withPartitionFilter(deltaFollowingPredicate)
                            .partitionEntries().stream()
                            .map(PartitionEntry::partition)
                            .filter(
                                    deltaPartition ->
                                            isNextIntervalPartition(
                                                    deltaPartition,
                                                    nextSnapshotPartition,
                                                    partitionComparator))
                            .collect(Collectors.toList());
            boolean canDrop =
                    deltaFollowingPartitions.isEmpty() || preSnapshotPartition.isPresent();
            LOG.info(
                    "Drop partition, partition={}, canDrop={}, preSnapshotPartition={}, nextSnapshotPartition={}",
                    partitionComputer.generatePartValues(partition),
                    canDrop,
                    generatePartitionValues(preSnapshotPartition, partitionComputer),
                    generatePartitionValues(nextSnapshotPartition, partitionComputer));
            if (!canDrop) {
                throw new RuntimeException("Snapshot partition cannot be dropped.");
            }
        }
    }

    private Optional<BinaryRow> findPreSnapshotPartition(
            List<BinaryRow> snapshotPartitions,
            BinaryRow partition,
            RecordComparator partitionComparator) {
        BinaryRow pre = null;
        for (BinaryRow snapshotPartition : snapshotPartitions) {
            if (partitionComparator.compare(snapshotPartition, partition) < 0) {
                pre = snapshotPartition;
            } else {
                break;
            }
        }
        return Optional.ofNullable(pre);
    }

    private Optional<BinaryRow> findNextSnapshotPartition(
            List<BinaryRow> snapshotPartitions,
            BinaryRow partition,
            RecordComparator partitionComparator) {
        for (BinaryRow snapshotPartition : snapshotPartitions) {
            if (partitionComparator.compare(snapshotPartition, partition) > 0) {
                return Optional.of(snapshotPartition);
            }
        }
        return Optional.empty();
    }

    private boolean isNextIntervalPartition(
            BinaryRow partition,
            Optional<BinaryRow> nextSnapshotPartition,
            RecordComparator partitionComparator) {
        return !nextSnapshotPartition.isPresent()
                || partitionComparator.compare(partition, nextSnapshotPartition.get()) < 0;
    }

    private String generatePartitionValues(
            Optional<BinaryRow> partition, InternalRowPartitionComputer partitionComputer) {
        if (!partition.isPresent()) {
            return "<none>";
        }
        return partitionComputer.generatePartValues(partition.get()).toString();
    }

    @Override
    public void close() throws Exception {}
}
