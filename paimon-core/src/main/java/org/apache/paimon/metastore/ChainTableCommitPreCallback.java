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

package org.apache.paimon.metastore;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.commit.ManifestEntryChanges;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitPreCallback;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ChainPartitionProjector;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link CommitPreCallback} implementation for chain tables.
 *
 * <p>This callback performs a pre-check before dropping partitions on the snapshot branch of a
 * chain table. It verifies that a snapshot partition being dropped is either followed by no delta
 * partitions in the chain interval or has a previous snapshot partition that can serve as its
 * predecessor.
 *
 * <p>The callback is only executed when all of following conditions are met:
 *
 * <ul>
 *   <li>The table is configured as a chain table and the current branch is the snapshot branch (see
 *       {@link ChainTableUtils#isScanFallbackSnapshotBranch(CoreOptions)}).
 *   <li>The committed snapshot kind is {@link CommitKind#OVERWRITE}.
 *   <li>All table and index manifest entries in the commit are {@link FileKind#DELETE deletes}.
 * </ul>
 *
 * <p>If the validation fails for any of the affected partitions, a {@link RuntimeException} is
 * thrown and the commit is aborted.
 *
 * <p>This implementation keeps only references to the table and its options and does not maintain
 * mutable state between invocations.
 */
public class ChainTableCommitPreCallback implements CommitPreCallback {

    private static final Logger LOG = LoggerFactory.getLogger(ChainTableCommitPreCallback.class);

    private transient FileStoreTable table;
    private transient CoreOptions coreOptions;

    public ChainTableCommitPreCallback(FileStoreTable table) {
        this.table = table;
        this.coreOptions = table.coreOptions();
    }

    @Override
    public void call(
            List<SimpleFileEntry> baseFiles,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot) {
        if (!ChainTableUtils.isScanFallbackSnapshotBranch(coreOptions)) {
            return;
        }
        if (snapshot.commitKind() != CommitKind.OVERWRITE) {
            return;
        }
        if (!isPureDeleteCommit(deltaFiles, indexFiles)) {
            return;
        }
        List<BinaryRow> changedPartitions =
                ManifestEntryChanges.changedPartitions(deltaFiles, indexFiles);
        FileStoreTable candidateTable = ChainTableUtils.resolveChainPrimaryTable(table);
        FileStoreTable deltaTable =
                candidateTable.switchToBranch(coreOptions.scanFallbackDeltaBranch());
        RowType partitionType = table.schema().logicalPartitionType();
        RowDataToObjectArrayConverter partitionConverter =
                new RowDataToObjectArrayConverter(partitionType);
        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        coreOptions.partitionDefaultName(),
                        partitionType,
                        table.schema().partitionKeys().toArray(new String[0]),
                        coreOptions.legacyPartitionName());

        List<String> chainKeys =
                ChainTableUtils.chainPartitionKeys(coreOptions, table.schema().partitionKeys());
        int chainFieldCount = chainKeys.size();
        ChainPartitionProjector projector =
                new ChainPartitionProjector(partitionType, chainFieldCount);
        int groupFieldCount = projector.groupFieldCount();
        RecordComparator chainComparator =
                CodeGenUtils.newRecordComparator(projector.chainPartitionType().getFieldTypes());

        List<BinaryRow> snapshotPartitions =
                table.newSnapshotReader().partitionEntries().stream()
                        .map(PartitionEntry::partition)
                        .collect(Collectors.toList());
        SnapshotReader deltaSnapshotReader = deltaTable.newSnapshotReader();
        PredicateBuilder builder = new PredicateBuilder(partitionType);
        for (BinaryRow partition : changedPartitions) {
            BinaryRow partitionGroup = projector.extractGroupPartition(partition);
            BinaryRow partitionChain = projector.extractChainPartition(partition);

            List<BinaryRow> sameGroupSnapshots =
                    filterSameGroup(snapshotPartitions, partitionGroup, projector);
            sameGroupSnapshots.sort(
                    (a, b) ->
                            chainComparator.compare(
                                    projector.extractChainPartition(a),
                                    projector.extractChainPartition(b)));

            Optional<BinaryRow> preSnapshotPartition =
                    findPreSnapshotInGroup(
                            sameGroupSnapshots, partitionChain, chainComparator, projector);
            Optional<BinaryRow> nextSnapshotPartition =
                    findNextSnapshotInGroup(
                            sameGroupSnapshots, partitionChain, chainComparator, projector);

            Predicate deltaFollowingPredicate =
                    ChainTableUtils.createGroupChainPredicate(
                            partition,
                            partitionConverter,
                            groupFieldCount,
                            builder::equal,
                            builder::greaterThan);
            List<BinaryRow> deltaFollowingPartitions =
                    deltaSnapshotReader.withPartitionFilter(deltaFollowingPredicate)
                            .partitionEntries().stream()
                            .map(PartitionEntry::partition)
                            .filter(
                                    deltaPartition ->
                                            isBeforeNextSnapshotInGroup(
                                                    deltaPartition,
                                                    nextSnapshotPartition,
                                                    chainComparator,
                                                    projector))
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

    private boolean isPureDeleteCommit(
            List<ManifestEntry> deltaFiles, List<IndexManifestEntry> indexFiles) {
        return deltaFiles.stream().allMatch(f -> f.kind() == FileKind.DELETE)
                && indexFiles.stream().allMatch(f -> f.kind() == FileKind.DELETE);
    }

    private List<BinaryRow> filterSameGroup(
            List<BinaryRow> partitions, BinaryRow groupKey, ChainPartitionProjector projector) {
        List<BinaryRow> result = new ArrayList<>();
        for (BinaryRow partition : partitions) {
            if (projector.extractGroupPartition(partition).equals(groupKey)) {
                result.add(partition);
            }
        }
        return result;
    }

    private Optional<BinaryRow> findPreSnapshotInGroup(
            List<BinaryRow> sortedSameGroupPartitions,
            BinaryRow targetChain,
            RecordComparator chainComparator,
            ChainPartitionProjector projector) {
        BinaryRow pre = null;
        for (BinaryRow snapshotPartition : sortedSameGroupPartitions) {
            BinaryRow chain = projector.extractChainPartition(snapshotPartition);
            if (chainComparator.compare(chain, targetChain) < 0) {
                pre = snapshotPartition;
            } else {
                break;
            }
        }
        return Optional.ofNullable(pre);
    }

    private Optional<BinaryRow> findNextSnapshotInGroup(
            List<BinaryRow> sortedSameGroupPartitions,
            BinaryRow targetChain,
            RecordComparator chainComparator,
            ChainPartitionProjector projector) {
        for (BinaryRow snapshotPartition : sortedSameGroupPartitions) {
            BinaryRow chain = projector.extractChainPartition(snapshotPartition);
            if (chainComparator.compare(chain, targetChain) > 0) {
                return Optional.of(snapshotPartition);
            }
        }
        return Optional.empty();
    }

    private boolean isBeforeNextSnapshotInGroup(
            BinaryRow partition,
            Optional<BinaryRow> nextSnapshotPartition,
            RecordComparator chainComparator,
            ChainPartitionProjector projector) {
        if (!nextSnapshotPartition.isPresent()) {
            return true;
        }
        BinaryRow partitionChain = projector.extractChainPartition(partition);
        BinaryRow nextChain = projector.extractChainPartition(nextSnapshotPartition.get());
        return chainComparator.compare(partitionChain, nextChain) < 0;
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
