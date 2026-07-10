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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Reassigns row IDs for data evolution tables by rewriting metadata only. */
public class DataEvolutionRowIdReassigner {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionRowIdReassigner.class);
    private static final String COMMIT_USER_PREFIX = "reassign-row-id";
    private static final int MAX_COMMIT_ATTEMPTS = 3;

    private final FileStoreTable table;
    private final @Nullable PartitionPredicate partitionPredicate;
    private final Runnable beforeCommit;

    public DataEvolutionRowIdReassigner(FileStoreTable table) {
        this(table, null);
    }

    public DataEvolutionRowIdReassigner(
            FileStoreTable table, @Nullable PartitionPredicate partitionPredicate) {
        this(table, partitionPredicate, () -> {});
    }

    @VisibleForTesting
    DataEvolutionRowIdReassigner(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            Runnable beforeCommit) {
        this.table = table;
        this.partitionPredicate = partitionPredicate;
        this.beforeCommit = beforeCommit;
    }

    public Result reassign() {
        Map<String, String> dynamicOptions = new HashMap<>(table.coreOptions().toMap());
        dynamicOptions.put(CoreOptions.COMMIT_USER_PREFIX.key(), COMMIT_USER_PREFIX);
        return reassign(CoreOptions.createCommitUser(new Options(dynamicOptions)));
    }

    public Result reassign(String commitUser) {
        checkArgument(
                table.coreOptions().rowTrackingEnabled(),
                "Table '%s' must enable 'row-tracking.enabled=true' before reassigning row IDs.",
                table.name());
        checkArgument(
                table.coreOptions().dataEvolutionEnabled(),
                "Table '%s' must enable 'data-evolution.enabled=true' before reassigning row IDs.",
                table.name());

        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        ManifestList manifestList = table.store().manifestListFactory().create();
        ReassignContext context = new ReassignContext();
        Snapshot latest = table.snapshotManager().latestSnapshot();
        checkArgument(
                latest != null, "Cannot reassign row IDs for empty table '%s'.", table.name());
        long nextRowId = requireNextRowId(latest);
        if (table.schema().logicalPartitionType().getFieldCount() == 0) {
            LOG.info(
                    "Skip reassigning row IDs for table {} because it is not partitioned.",
                    table.name());
            return Result.skipped(latest.id(), nextRowId, "table is not partitioned");
        }

        AssignmentPlan assignment =
                planAssignment(
                        latest, manifestList.readDataManifests(latest), manifestFile, context);
        if (!assignment.hasCurrentFiles) {
            return Result.skipped(
                    latest.id(),
                    nextRowId,
                    partitionFilterEnabled()
                            ? "partition filter matches no current files"
                            : "table has no current files");
        }
        if (assignment.plannedFiles.isEmpty()) {
            LOG.info(
                    "Skip reassigning row IDs for table {} because partition row IDs are already contiguous.",
                    table.name());
            return Result.skipped(
                    latest.id(), nextRowId, "partition row IDs are already contiguous");
        }

        for (int attempt = 1; attempt <= MAX_COMMIT_ATTEMPTS; attempt++) {
            CommitAssignmentResult commitResult =
                    commitAssignment(assignment, manifestFile, manifestList, context, commitUser);
            if (commitResult.success) {
                LOG.info(
                        "Reassigned row IDs for table {} from {} to {}, partitions={}, files={}, rows={}.",
                        table.name(),
                        assignment.firstAssignedRowId,
                        assignment.nextRowId,
                        assignment.rowIdMappings.size(),
                        assignment.plannedFiles.size(),
                        assignment.logicalRowCount);
                return new Result(
                        assignment.snapshot.id(),
                        assignment.snapshot.id() + 1,
                        assignment.plannedFiles.size(),
                        assignment.logicalRowCount,
                        commitResult.indexFileCount,
                        assignment.firstAssignedRowId,
                        assignment.nextRowId);
            }

            if (attempt == MAX_COMMIT_ATTEMPTS) {
                throw new RuntimeException(
                        "Failed to reassign row IDs because a newer snapshot has been committed.");
            }

            Snapshot newLatest = table.snapshotManager().latestSnapshot();
            checkState(newLatest != null, "Latest snapshot disappeared while reassigning row IDs.");
            advanceAssignment(assignment, newLatest, manifestFile, manifestList, context);
            LOG.info(
                    "Failed to commit row-id reassignment for table {} based on snapshot {} because snapshot {} has been committed. Retrying {}/{} with the updated assignment plan.",
                    table.name(),
                    latest.id(),
                    newLatest.id(),
                    attempt + 1,
                    MAX_COMMIT_ATTEMPTS);
            latest = newLatest;
        }

        throw new IllegalStateException("Unreachable retry state while reassigning row IDs.");
    }

    private long requireNextRowId(Snapshot snapshot) {
        Long nextRowId = snapshot.nextRowId();
        checkState(
                nextRowId != null,
                "Next row id cannot be null for row-tracking table '%s'.",
                table.name());
        return nextRowId;
    }

    private AssignmentPlan planAssignment(
            Snapshot snapshot,
            List<ManifestFileMeta> manifestMetas,
            ManifestFile manifestFile,
            ReassignContext context) {
        List<List<ManifestFileMeta>> manifestGroups = manifestGroupsByPartition(manifestMetas);
        Map<BinaryRow, List<SourcedManifestEntry>> entriesToReassignByPartition =
                new LinkedHashMap<>();
        boolean hasCurrentFiles = false;

        for (List<ManifestFileMeta> manifestGroup : manifestGroups) {
            if (skipManifestGroupByPartitionFilter(manifestGroup)) {
                continue;
            }

            CurrentManifest currentManifest = currentManifest(manifestGroup, manifestFile, context);
            if (currentManifest.currentEntries.isEmpty()) {
                continue;
            }
            hasCurrentFiles = true;

            Map<BinaryRow, List<ManifestEntry>> entriesByPartition =
                    entriesByPartition(currentManifest.currentEntries);
            Set<BinaryRow> partitionsToReassign = partitionsToReassign(entriesByPartition);
            if (partitionsToReassign.isEmpty()) {
                continue;
            }

            for (SourcedManifestEntry entry : currentManifest.currentEntries) {
                if (partitionsToReassign.contains(entry.entry.partition())) {
                    entriesToReassignByPartition
                            .computeIfAbsent(entry.entry.partition(), ignored -> new ArrayList<>())
                            .add(entry);
                }
            }
        }

        AssignmentPlan assignment =
                new AssignmentPlan(
                        snapshot, manifestMetas, entriesToReassignByPartition, hasCurrentFiles);
        recalculateAssignment(assignment, requireNextRowId(snapshot));
        return assignment;
    }

    private List<List<ManifestFileMeta>> manifestGroupsByPartition(
            List<ManifestFileMeta> manifestMetas) {
        List<ManifestFileMeta> nonEmptyManifestMetas = new ArrayList<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            if (manifestMeta.numAddedFiles() + manifestMeta.numDeletedFiles() > 0) {
                nonEmptyManifestMetas.add(manifestMeta);
            }
        }
        if (nonEmptyManifestMetas.size() <= 1) {
            return nonEmptyManifestMetas.isEmpty()
                    ? Collections.emptyList()
                    : Collections.singletonList(nonEmptyManifestMetas);
        }

        int partitionFieldCount = table.schema().logicalPartitionType().getFieldCount();
        for (ManifestFileMeta manifestMeta : nonEmptyManifestMetas) {
            if (!containsPartitionStats(manifestMeta, partitionFieldCount)) {
                return Collections.singletonList(nonEmptyManifestMetas);
            }
        }

        RecordComparator partitionComparator = partitionComparator();
        List<PartitionManifestRange> manifestRanges = new ArrayList<>(nonEmptyManifestMetas.size());
        for (int i = 0; i < nonEmptyManifestMetas.size(); i++) {
            ManifestFileMeta manifestMeta = nonEmptyManifestMetas.get(i);
            manifestRanges.add(
                    new PartitionManifestRange(
                            manifestMeta,
                            manifestMeta.partitionStats().minValues(),
                            manifestMeta.partitionStats().maxValues(),
                            containsNullPartition(manifestMeta, partitionFieldCount),
                            i));
        }
        Collections.sort(
                manifestRanges,
                (left, right) -> {
                    int result = partitionComparator.compare(left.minPartition, right.minPartition);
                    if (result != 0) {
                        return result;
                    }
                    return partitionComparator.compare(left.maxPartition, right.maxPartition);
                });

        List<List<PartitionManifestRange>> groupedManifestRanges = new ArrayList<>();
        List<PartitionManifestRange> currentGroup = new ArrayList<>();
        currentGroup.add(manifestRanges.get(0));
        BinaryRow currentMaxPartition = manifestRanges.get(0).maxPartition;
        for (int i = 1; i < manifestRanges.size(); i++) {
            PartitionManifestRange current = manifestRanges.get(i);
            if (partitionComparator.compare(current.minPartition, currentMaxPartition) <= 0) {
                currentGroup.add(current);
                if (partitionComparator.compare(current.maxPartition, currentMaxPartition) > 0) {
                    currentMaxPartition = current.maxPartition;
                }
            } else {
                groupedManifestRanges.add(currentGroup);
                currentGroup = new ArrayList<>();
                currentGroup.add(current);
                currentMaxPartition = current.maxPartition;
            }
        }
        groupedManifestRanges.add(currentGroup);

        // Partition min/max excludes nulls, so null-bearing ranges need an extra shared group.
        List<PartitionManifestRange> nullPartitionGroup = new ArrayList<>();
        int nullPartitionGroupIndex = -1;
        for (int i = 0; i < groupedManifestRanges.size(); ) {
            List<PartitionManifestRange> group = groupedManifestRanges.get(i);
            boolean containsNullPartition = false;
            for (PartitionManifestRange range : group) {
                if (range.containsNullPartition) {
                    containsNullPartition = true;
                    break;
                }
            }
            if (containsNullPartition) {
                if (nullPartitionGroupIndex < 0) {
                    nullPartitionGroupIndex = i;
                }
                nullPartitionGroup.addAll(group);
                groupedManifestRanges.remove(i);
            } else {
                i++;
            }
        }
        if (!nullPartitionGroup.isEmpty()) {
            groupedManifestRanges.add(nullPartitionGroupIndex, nullPartitionGroup);
        }

        List<List<ManifestFileMeta>> groups = new ArrayList<>();
        for (List<PartitionManifestRange> group : groupedManifestRanges) {
            Collections.sort(group, Comparator.comparingInt(left -> left.originalIndex));
            List<ManifestFileMeta> manifestGroup = new ArrayList<>(group.size());
            for (PartitionManifestRange range : group) {
                manifestGroup.add(range.manifest);
            }
            groups.add(manifestGroup);
        }
        return groups;
    }

    private boolean skipManifestGroupByPartitionFilter(List<ManifestFileMeta> manifestGroup) {
        if (!partitionFilterEnabled()) {
            return false;
        }

        int partitionFieldCount = table.schema().logicalPartitionType().getFieldCount();
        for (ManifestFileMeta manifestMeta : manifestGroup) {
            if (!containsPartitionStats(manifestMeta, partitionFieldCount)) {
                return false;
            }

            SimpleStats partitionStats = manifestMeta.partitionStats();
            if (partitionPredicate.test(
                    manifestMeta.numAddedFiles() + manifestMeta.numDeletedFiles(),
                    partitionStats.minValues(),
                    partitionStats.maxValues(),
                    partitionStats.nullCounts())) {
                return false;
            }
        }
        return true;
    }

    private boolean containsPartitionStats(ManifestFileMeta manifestMeta, int partitionFieldCount) {
        SimpleStats partitionStats = manifestMeta.partitionStats();
        return partitionStats != null
                && partitionStats.minValues().getFieldCount() == partitionFieldCount
                && partitionStats.maxValues().getFieldCount() == partitionFieldCount
                && partitionStats.nullCounts().size() == partitionFieldCount;
    }

    private boolean containsNullPartition(ManifestFileMeta manifestMeta, int partitionFieldCount) {
        for (int i = 0; i < partitionFieldCount; i++) {
            if (manifestMeta.partitionStats().nullCounts().getLong(i) != 0) {
                return true;
            }
        }
        return false;
    }

    private CurrentManifest currentManifest(
            List<ManifestFileMeta> manifestMetas,
            ManifestFile manifestFile,
            ReassignContext context) {
        Set<FileEntry.Identifier> deletedIdentifiers =
                deletedIdentifiers(manifestFile, manifestMetas, context);

        List<SourcedManifestEntry> currentEntries = new ArrayList<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            if (manifestMeta.numAddedFiles() <= 0) {
                continue;
            }
            List<ManifestEntry> entries =
                    readPlanningManifestEntries(manifestFile, manifestMeta, context);
            for (ManifestEntry entry : entries) {
                if (entry.kind() == FileKind.ADD
                        && partitionIncluded(entry.partition())
                        && !deletedIdentifiers.contains(entry.identifier())) {
                    currentEntries.add(new SourcedManifestEntry(manifestMeta, entry));
                }
            }
        }
        return new CurrentManifest(currentEntries);
    }

    private CurrentManifest currentManifest(
            List<ManifestFileMeta> manifestMetas,
            ManifestFile manifestFile,
            PartitionPredicate effectivePartitionPredicate) {
        Map<ManifestFileMeta, List<ManifestEntry>> entriesByManifest = new HashMap<>();
        Set<FileEntry.Identifier> deletedIdentifiers = new HashSet<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            if (manifestMeta.numDeletedFiles() <= 0) {
                continue;
            }
            for (ManifestEntry entry :
                    entriesByManifest.computeIfAbsent(
                            manifestMeta,
                            ignored ->
                                    readPlanningManifestEntries(
                                            manifestFile,
                                            manifestMeta,
                                            effectivePartitionPredicate))) {
                if (entry.kind() == FileKind.DELETE) {
                    deletedIdentifiers.add(entry.identifier());
                }
            }
        }

        List<SourcedManifestEntry> currentEntries = new ArrayList<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            if (manifestMeta.numAddedFiles() <= 0) {
                continue;
            }
            for (ManifestEntry entry :
                    entriesByManifest.computeIfAbsent(
                            manifestMeta,
                            ignored ->
                                    readPlanningManifestEntries(
                                            manifestFile,
                                            manifestMeta,
                                            effectivePartitionPredicate))) {
                if (entry.kind() == FileKind.ADD
                        && !deletedIdentifiers.contains(entry.identifier())) {
                    currentEntries.add(new SourcedManifestEntry(manifestMeta, entry));
                }
            }
        }
        return new CurrentManifest(currentEntries);
    }

    private Set<FileEntry.Identifier> deletedIdentifiers(
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifestMetas,
            ReassignContext context) {
        Set<FileEntry.Identifier> deletedIdentifiers = new HashSet<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            if (manifestMeta.numDeletedFiles() <= 0) {
                continue;
            }
            List<ManifestEntry> entries =
                    readPlanningManifestEntries(manifestFile, manifestMeta, context);
            for (ManifestEntry entry : entries) {
                if (entry.kind() == FileKind.DELETE && partitionIncluded(entry.partition())) {
                    deletedIdentifiers.add(entry.identifier());
                }
            }
        }
        return deletedIdentifiers;
    }

    private boolean partitionIncluded(BinaryRow partition) {
        return !partitionFilterEnabled() || partitionPredicate.test(partition);
    }

    private boolean partitionFilterEnabled() {
        return partitionPredicate != null;
    }

    private CommitAssignmentResult commitAssignment(
            AssignmentPlan assignment,
            ManifestFile manifestFile,
            ManifestList manifestList,
            ReassignContext context,
            String commitUser) {
        Map<String, List<ManifestFileMeta>> rewrittenManifestMetas =
                writeManifestReplacements(assignment, manifestFile);
        Pair<String, Long> baseManifestList =
                writeBaseManifestList(
                        assignment.manifestMetas, rewrittenManifestMetas, manifestList);
        Pair<String, Long> deltaManifestList = manifestList.write(Collections.emptyList());
        RewrittenIndexManifest rewrittenIndexManifest =
                rewriteIndexManifest(assignment.snapshot, assignment, context);

        boolean success;
        try (FileStoreCommitImpl commit =
                (FileStoreCommitImpl) table.store().newCommit(commitUser, table)) {
            beforeCommit.run();
            success =
                    commit.replaceManifestList(
                            assignment.snapshot,
                            assignment.snapshot.totalRecordCount(),
                            baseManifestList,
                            deltaManifestList,
                            rewrittenIndexManifest.indexManifest,
                            assignment.nextRowId);
        }
        return new CommitAssignmentResult(success, rewrittenIndexManifest.indexFileCount);
    }

    private void advanceAssignment(
            AssignmentPlan assignment,
            Snapshot latest,
            ManifestFile manifestFile,
            ManifestList manifestList,
            ReassignContext context) {
        checkState(
                latest.id() > assignment.snapshot.id(),
                "Cannot advance row-id assignment from snapshot %s to %s.",
                assignment.snapshot.id(),
                latest.id());

        Set<BinaryRow> touchedUnplannedPartitions = new HashSet<>();
        for (long id = assignment.snapshot.id() + 1; id <= latest.id(); id++) {
            Snapshot snapshot;
            try {
                snapshot = table.snapshotManager().tryGetSnapshot(id);
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format(
                                "Abort row-id reassignment because snapshot %s cannot be read.",
                                id),
                        e);
            }

            if (snapshot.commitKind() == Snapshot.CommitKind.COMPACT
                    || snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE) {
                throw new RuntimeException(
                        String.format(
                                "Abort row-id reassignment because %s snapshot %s was committed after snapshot %s.",
                                snapshot.commitKind(), snapshot.id(), assignment.snapshot.id()));
            }
            if (snapshot.commitKind() == Snapshot.CommitKind.ANALYZE) {
                continue;
            }
            checkState(
                    snapshot.commitKind() == Snapshot.CommitKind.APPEND,
                    "Unsupported snapshot kind %s while advancing row-id assignment.",
                    snapshot.commitKind());

            for (ManifestFileMeta manifestMeta : manifestList.readDeltaManifests(snapshot)) {
                for (ManifestEntry entry :
                        readPlanningManifestEntries(manifestFile, manifestMeta, context)) {
                    checkState(
                            entry.kind() == FileKind.ADD,
                            "APPEND snapshot %s contains non-ADD manifest entry %s.",
                            snapshot.id(),
                            entry);
                    SourcedManifestEntry sourced = new SourcedManifestEntry(manifestMeta, entry);
                    if (assignment.containsPartition(entry.partition())) {
                        assignment.add(sourced);
                    } else {
                        touchedUnplannedPartitions.add(entry.partition());
                    }
                }
            }
        }

        List<ManifestFileMeta> latestManifestMetas = manifestList.readDataManifests(latest);
        if (!touchedUnplannedPartitions.isEmpty()) {
            Map<BinaryRow, List<SourcedManifestEntry>> touchedEntries =
                    currentEntriesByPartition(
                            latestManifestMetas, touchedUnplannedPartitions, manifestFile);
            for (Map.Entry<BinaryRow, List<SourcedManifestEntry>> partition :
                    touchedEntries.entrySet()) {
                List<ManifestEntry> entries = new ArrayList<>(partition.getValue().size());
                for (SourcedManifestEntry sourced : partition.getValue()) {
                    entries.add(sourced.entry);
                }
                if (!partitionRowIdsAreContiguous(entries)) {
                    assignment.addAll(partition.getValue());
                }
            }
        }

        relocatePlannedFiles(assignment, latestManifestMetas, manifestFile, context);
        context.retainLatest(latestManifestMetas, latest.indexManifest());
        assignment.snapshot = latest;
        assignment.manifestMetas = latestManifestMetas;
        recalculateAssignment(assignment, requireNextRowId(latest));
    }

    private Map<BinaryRow, List<SourcedManifestEntry>> currentEntriesByPartition(
            List<ManifestFileMeta> manifestMetas,
            Set<BinaryRow> partitions,
            ManifestFile manifestFile) {
        Map<BinaryRow, List<SourcedManifestEntry>> result = new LinkedHashMap<>();
        PartitionPredicate touchedPartitionPredicate =
                PartitionPredicate.fromMultiple(table.schema().logicalPartitionType(), partitions);
        checkState(touchedPartitionPredicate != null, "Touched partition predicate is null.");
        List<PartitionPredicate> predicates = new ArrayList<>();
        if (partitionPredicate != null) {
            predicates.add(partitionPredicate);
        }
        predicates.add(touchedPartitionPredicate);
        PartitionPredicate effectivePartitionPredicate = PartitionPredicate.and(predicates);
        checkState(effectivePartitionPredicate != null, "Effective partition predicate is null.");
        for (List<ManifestFileMeta> manifestGroup : manifestGroupsByPartition(manifestMetas)) {
            if (!manifestGroupMayContainPartitions(manifestGroup, effectivePartitionPredicate)) {
                continue;
            }
            CurrentManifest currentManifest =
                    currentManifest(manifestGroup, manifestFile, effectivePartitionPredicate);
            for (SourcedManifestEntry entry : currentManifest.currentEntries) {
                if (partitions.contains(entry.entry.partition())) {
                    result.computeIfAbsent(entry.entry.partition(), ignored -> new ArrayList<>())
                            .add(entry);
                }
            }
        }
        return result;
    }

    private boolean manifestGroupMayContainPartitions(
            List<ManifestFileMeta> manifestGroup, PartitionPredicate effectivePartitionPredicate) {
        int partitionFieldCount = table.schema().logicalPartitionType().getFieldCount();
        for (ManifestFileMeta manifestMeta : manifestGroup) {
            if (!containsPartitionStats(manifestMeta, partitionFieldCount)) {
                return true;
            }

            SimpleStats partitionStats = manifestMeta.partitionStats();
            if (effectivePartitionPredicate.test(
                    manifestMeta.numAddedFiles() + manifestMeta.numDeletedFiles(),
                    partitionStats.minValues(),
                    partitionStats.maxValues(),
                    partitionStats.nullCounts())) {
                return true;
            }
        }
        return false;
    }

    private void relocatePlannedFiles(
            AssignmentPlan assignment,
            List<ManifestFileMeta> latestManifestMetas,
            ManifestFile manifestFile,
            ReassignContext context) {
        Set<String> previousManifestFiles = manifestFileNames(assignment.manifestMetas);
        Set<String> latestManifestFiles = manifestFileNames(latestManifestMetas);
        Set<FileEntry.Identifier> unresolved = new HashSet<>();
        for (Map.Entry<FileEntry.Identifier, SourcedManifestEntry> planned :
                assignment.plannedFiles.entrySet()) {
            ManifestFileMeta source = planned.getValue().manifest;
            if (source == null || !latestManifestFiles.contains(source.fileName())) {
                planned.getValue().manifest = null;
                unresolved.add(planned.getKey());
            }
        }

        if (unresolved.isEmpty()) {
            return;
        }

        for (ManifestFileMeta manifestMeta : latestManifestMetas) {
            if (unresolved.isEmpty()) {
                break;
            }
            if (previousManifestFiles.contains(manifestMeta.fileName())) {
                continue;
            }
            for (ManifestEntry entry :
                    readPlanningManifestEntries(manifestFile, manifestMeta, context)) {
                if (entry.kind() != FileKind.ADD) {
                    continue;
                }
                FileEntry.Identifier identifier = entry.identifier();
                if (unresolved.remove(identifier)) {
                    assignment.plannedFiles.get(identifier).manifest = manifestMeta;
                }
            }
        }
        checkState(
                unresolved.isEmpty(),
                "Cannot locate planned files %s in the latest APPEND manifests.",
                unresolved);
    }

    private Set<String> manifestFileNames(List<ManifestFileMeta> manifestMetas) {
        Set<String> result = new HashSet<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            result.add(manifestMeta.fileName());
        }
        return result;
    }

    private Pair<String, Long> writeBaseManifestList(
            List<ManifestFileMeta> manifestMetas,
            Map<String, List<ManifestFileMeta>> rewrittenManifestMetas,
            ManifestList manifestList) {
        List<ManifestFileMeta> baseManifestMetas = new ArrayList<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            List<ManifestFileMeta> replacement =
                    rewrittenManifestMetas.get(manifestMeta.fileName());
            if (replacement == null) {
                baseManifestMetas.add(manifestMeta);
            } else {
                baseManifestMetas.addAll(replacement);
            }
        }
        return manifestList.write(baseManifestMetas);
    }

    private Map<String, List<ManifestFileMeta>> writeManifestReplacements(
            AssignmentPlan assignment, ManifestFile manifestFile) {
        Map<String, Integer> assignmentsByManifest = new HashMap<>();
        for (Map.Entry<FileEntry.Identifier, SourcedManifestEntry> planned :
                assignment.plannedFiles.entrySet()) {
            ManifestFileMeta manifest = planned.getValue().manifest;
            Long reassignedFirstRowId = planned.getValue().reassignedFirstRowId;
            checkState(
                    manifest != null,
                    "Cannot locate manifest for planned file %s.",
                    planned.getKey());
            checkState(
                    reassignedFirstRowId != null,
                    "Cannot find reassigned first row ID for planned file %s.",
                    planned.getKey());
            assignmentsByManifest.merge(manifest.fileName(), 1, Integer::sum);
        }

        Map<String, List<ManifestFileMeta>> rewrittenManifestMetas = new HashMap<>();
        for (ManifestFileMeta manifestMeta : assignment.manifestMetas) {
            Integer expectedAssignments = assignmentsByManifest.remove(manifestMeta.fileName());
            if (expectedAssignments == null) {
                continue;
            }

            List<ManifestEntry> entries =
                    manifestFile.read(manifestMeta.fileName(), manifestMeta.fileSize());
            Set<FileEntry.Identifier> rewrittenIdentifiers = new HashSet<>();
            for (int i = 0; i < entries.size(); i++) {
                ManifestEntry entry = entries.get(i);
                if (entry.kind() == FileKind.ADD) {
                    SourcedManifestEntry planned = assignment.plannedFiles.get(entry.identifier());
                    if (planned != null
                            && planned.manifest != null
                            && planned.manifest.fileName().equals(manifestMeta.fileName())) {
                        checkState(
                                rewrittenIdentifiers.add(entry.identifier()),
                                "Duplicate planned file %s in manifest %s.",
                                entry.identifier(),
                                manifestMeta.fileName());
                        entries.set(i, entry.assignFirstRowId(planned.reassignedFirstRowId));
                    }
                }
            }
            checkState(
                    rewrittenIdentifiers.size() == expectedAssignments,
                    "Expected %s planned files in manifest %s, but found %s.",
                    expectedAssignments,
                    manifestMeta.fileName(),
                    rewrittenIdentifiers.size());
            rewrittenManifestMetas.put(manifestMeta.fileName(), manifestFile.write(entries));
        }
        checkState(
                assignmentsByManifest.isEmpty(),
                "Cannot find planned manifests %s in the latest snapshot.",
                assignmentsByManifest.keySet());
        return rewrittenManifestMetas;
    }

    private Map<BinaryRow, List<ManifestEntry>> entriesByPartition(
            List<SourcedManifestEntry> entries) {
        Comparator<ManifestEntry> comparator = entryComparator();
        Collections.sort(entries, (left, right) -> comparator.compare(left.entry, right.entry));

        Map<BinaryRow, List<ManifestEntry>> entriesByPartition = new LinkedHashMap<>();
        for (SourcedManifestEntry sourcedEntry : entries) {
            ManifestEntry entry = sourcedEntry.entry;
            validatePlanningEntry(entry);
            entriesByPartition
                    .computeIfAbsent(entry.partition(), k -> new ArrayList<>())
                    .add(entry);
        }
        return entriesByPartition;
    }

    private void validatePlanningEntry(ManifestEntry entry) {
        List<String> writeCols = entry.file().writeCols();
        checkState(
                writeCols == null || !writeCols.contains(SpecialFields.ROW_ID.name()),
                "Cannot reassign row IDs for file '%s' because it physically stores the row-id field.",
                entry.file().fileName());
        checkState(
                entry.file().firstRowId() != null,
                "File '%s' in table '%s' does not have first row id.",
                entry.file().fileName(),
                table.name());
    }

    private Set<BinaryRow> partitionsToReassign(
            Map<BinaryRow, List<ManifestEntry>> entriesByPartition) {
        Set<BinaryRow> partitionsToReassign = new HashSet<>();
        for (Map.Entry<BinaryRow, List<ManifestEntry>> entry : entriesByPartition.entrySet()) {
            if (!partitionRowIdsAreContiguous(entry.getValue())) {
                partitionsToReassign.add(entry.getKey());
            }
        }
        return partitionsToReassign;
    }

    private boolean partitionRowIdsAreContiguous(List<ManifestEntry> entries) {
        List<Range> logicalRanges = logicalRanges(entries);
        if (logicalRanges.size() <= 1) {
            return true;
        }

        Collections.sort(
                logicalRanges,
                (left, right) -> {
                    int result = Long.compare(left.from, right.from);
                    return result == 0 ? Long.compare(left.to, right.to) : result;
                });
        long previousEnd = logicalRanges.get(0).to;
        for (int i = 1; i < logicalRanges.size(); i++) {
            Range current = logicalRanges.get(i);
            if (current.from != previousEnd + 1) {
                return false;
            }
            previousEnd = current.to;
        }
        return true;
    }

    private List<Range> logicalRanges(List<ManifestEntry> entries) {
        RangeHelper<ManifestEntry> rangeHelper =
                new RangeHelper<>(entry -> entry.file().nonNullRowIdRange());
        List<List<ManifestEntry>> groups = rangeHelper.mergeOverlappingRanges(entries);
        List<Range> logicalRanges = new ArrayList<>(groups.size());
        for (List<ManifestEntry> group : groups) {
            logicalRanges.add(oldLogicalRange(group));
        }
        return logicalRanges;
    }

    private void recalculateAssignment(AssignmentPlan assignment, long firstRowId) {
        assignment.rowIdMappings.clear();
        for (SourcedManifestEntry plannedFile : assignment.plannedFiles.values()) {
            plannedFile.reassignedFirstRowId = null;
        }
        List<BinaryRow> partitions =
                new ArrayList<>(assignment.entriesToReassignByPartition.keySet());
        RecordComparator partitionComparator = partitionComparator();
        Collections.sort(partitions, partitionComparator::compare);

        long nextRowId = firstRowId;
        long logicalRowCount = 0;
        for (BinaryRow partition : partitions) {
            List<SourcedManifestEntry> sourcedEntries =
                    assignment.entriesToReassignByPartition.get(partition);
            List<ManifestEntry> entries = new ArrayList<>(sourcedEntries.size());
            for (SourcedManifestEntry sourcedEntry : sourcedEntries) {
                validatePlanningEntry(sourcedEntry.entry);
                entries.add(sourcedEntry.entry);
            }

            long partitionFirstRowId = nextRowId;
            PartitionAssignment partitionAssignment =
                    assignPartition(entries, assignment.plannedFiles, nextRowId);
            assignment.rowIdMappings.put(partition, partitionAssignment.rowIdMappings);
            nextRowId = partitionAssignment.nextRowId;
            logicalRowCount += nextRowId - partitionFirstRowId;
        }

        for (Map.Entry<FileEntry.Identifier, SourcedManifestEntry> planned :
                assignment.plannedFiles.entrySet()) {
            checkState(
                    planned.getValue().reassignedFirstRowId != null,
                    "Cannot find assignment for planned file %s.",
                    planned.getKey());
        }
        assignment.firstAssignedRowId = firstRowId;
        assignment.nextRowId = nextRowId;
        assignment.logicalRowCount = logicalRowCount;
    }

    private PartitionAssignment assignPartition(
            List<ManifestEntry> entries,
            Map<FileEntry.Identifier, SourcedManifestEntry> plannedFiles,
            long firstRowId) {
        RangeHelper<ManifestEntry> rangeHelper =
                new RangeHelper<>(entry -> entry.file().nonNullRowIdRange());
        List<List<ManifestEntry>> groups = rangeHelper.mergeOverlappingRanges(entries);

        List<RowRangeMappingIndex.Mapping> mappings = new ArrayList<>();
        long nextRowId = firstRowId;

        for (List<ManifestEntry> group : groups) {
            Collections.sort(group, entryComparatorWithoutPartition());
            Range oldLogicalRange = oldLogicalRange(group);
            mappings.add(
                    RowRangeMappingIndex.mapping(
                            oldLogicalRange.from, oldLogicalRange.to, nextRowId));

            for (ManifestEntry entry : group) {
                long oldFirstRowId = entry.file().nonNullFirstRowId();
                long newFirstRowId = nextRowId + oldFirstRowId - oldLogicalRange.from;
                SourcedManifestEntry plannedFile = plannedFiles.get(entry.identifier());
                checkState(
                        plannedFile != null,
                        "Cannot find planned manifest entry for file %s.",
                        entry.identifier());
                checkState(
                        plannedFile.reassignedFirstRowId == null,
                        "Duplicate current manifest entry for file %s.",
                        entry.identifier());
                plannedFile.reassignedFirstRowId = newFirstRowId;
            }

            nextRowId += oldLogicalRange.count();
        }

        return new PartitionAssignment(RowRangeMappingIndex.create(mappings), nextRowId);
    }

    private Range oldLogicalRange(List<ManifestEntry> group) {
        List<ManifestEntry> dataFiles = new ArrayList<>();
        for (ManifestEntry entry : group) {
            if (!isSpecialFile(entry)) {
                dataFiles.add(entry);
            }
        }

        Range logicalRange;
        if (dataFiles.isEmpty()) {
            logicalRange = spanningRange(group);
        } else {
            logicalRange = dataFiles.get(0).file().nonNullRowIdRange();
            for (ManifestEntry dataFile : dataFiles) {
                Range current = dataFile.file().nonNullRowIdRange();
                checkState(
                        logicalRange.from == current.from && logicalRange.to == current.to,
                        "Data files in one overlapping row-id group must have the same row-id range, but found %s and %s.",
                        logicalRange,
                        current);
            }
        }

        for (ManifestEntry entry : group) {
            Range range = entry.file().nonNullRowIdRange();
            checkState(
                    range.from >= logicalRange.from && range.to <= logicalRange.to,
                    "File '%s' row-id range %s is outside logical row-id range %s.",
                    entry.file().fileName(),
                    range,
                    logicalRange);
        }
        return logicalRange;
    }

    private Range spanningRange(List<ManifestEntry> group) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (ManifestEntry entry : group) {
            Range range = entry.file().nonNullRowIdRange();
            min = Math.min(min, range.from);
            max = Math.max(max, range.to);
        }
        return new Range(min, max);
    }

    private RewrittenIndexManifest rewriteIndexManifest(
            Snapshot latest, AssignmentPlan assignment, ReassignContext context) {
        if (latest.indexManifest() == null) {
            return new RewrittenIndexManifest(null, 0);
        }

        IndexManifestFile indexManifestFile = table.store().indexManifestFileFactory().create();
        List<IndexManifestEntry> indexEntries =
                readIndexManifestEntries(indexManifestFile, latest.indexManifest(), context);
        if (indexEntries.isEmpty()) {
            return new RewrittenIndexManifest(null, 0);
        }

        List<IndexManifestEntry> rewritten = new ArrayList<>(indexEntries.size());
        long globalIndexFileCount = 0;
        for (IndexManifestEntry entry : indexEntries) {
            checkState(
                    entry.kind() == FileKind.ADD,
                    "Index manifest '%s' contains non-current entry %s.",
                    latest.indexManifest(),
                    entry);

            IndexFileMeta indexFile = entry.indexFile();
            GlobalIndexMeta globalIndex = indexFile.globalIndexMeta();
            RowRangeMappingIndex mappingIndex = assignment.rowIdMappings.get(entry.partition());
            if (globalIndex == null || mappingIndex == null) {
                rewritten.add(entry);
                continue;
            }

            Optional<Range> newRange = mappingIndex.map(globalIndex.rowRange());
            if (!newRange.isPresent()) {
                LOG.warn(
                        "Drop global index file '{}' from table {} during row-id reassignment because its row range {} cannot be rewritten safely.",
                        indexFile.fileName(),
                        table.name(),
                        globalIndex.rowRange());
                continue;
            }
            Range rewrittenRange = newRange.get();
            globalIndexFileCount++;
            GlobalIndexMeta newGlobalIndex =
                    new GlobalIndexMeta(
                            rewrittenRange.from,
                            rewrittenRange.to,
                            globalIndex.indexFieldId(),
                            globalIndex.extraFieldIds(),
                            globalIndex.indexMeta());
            IndexFileMeta newIndexFile =
                    new IndexFileMeta(
                            indexFile.indexType(),
                            indexFile.fileName(),
                            indexFile.fileSize(),
                            indexFile.rowCount(),
                            indexFile.dvRanges(),
                            indexFile.externalPath(),
                            newGlobalIndex);
            rewritten.add(
                    new IndexManifestEntry(
                            entry.kind(), entry.partition(), entry.bucket(), newIndexFile));
        }

        return new RewrittenIndexManifest(
                indexManifestFile.writeWithoutRolling(rewritten), globalIndexFileCount);
    }

    private List<ManifestEntry> readPlanningManifestEntries(
            ManifestFile manifestFile, ManifestFileMeta manifestMeta, ReassignContext context) {
        // Rewrites reread full entries so this retry cache can safely omit value stats.
        return context.planningManifestEntries.computeIfAbsent(
                manifestMeta,
                ignored ->
                        Collections.unmodifiableList(
                                readPlanningManifestEntries(
                                        manifestFile, manifestMeta, partitionPredicate)));
    }

    private List<ManifestEntry> readPlanningManifestEntries(
            ManifestFile manifestFile,
            ManifestFileMeta manifestMeta,
            @Nullable PartitionPredicate effectivePartitionPredicate) {
        return manifestFile.read(
                manifestMeta.fileName(),
                manifestMeta.fileSize(),
                effectivePartitionPredicate,
                null,
                Filter.alwaysTrue(),
                entry ->
                        effectivePartitionPredicate == null
                                || effectivePartitionPredicate.test(entry.partition()),
                ManifestEntry::copyWithoutStats);
    }

    private List<IndexManifestEntry> readIndexManifestEntries(
            IndexManifestFile indexManifestFile, String indexManifest, ReassignContext context) {
        return context.indexManifestEntries.computeIfAbsent(
                indexManifest,
                ignored -> Collections.unmodifiableList(indexManifestFile.read(indexManifest)));
    }

    private Comparator<ManifestEntry> entryComparator() {
        RecordComparator partitionComparator = partitionComparator();
        Comparator<ManifestEntry> withoutPartition = entryComparatorWithoutPartition();
        return (left, right) -> {
            int partitionCompare = partitionComparator.compare(left.partition(), right.partition());
            if (partitionCompare != 0) {
                return partitionCompare;
            }
            return withoutPartition.compare(left, right);
        };
    }

    private RecordComparator partitionComparator() {
        return CodeGenUtils.newRecordComparator(
                table.schema().logicalPartitionType().getFieldTypes());
    }

    private Comparator<ManifestEntry> entryComparatorWithoutPartition() {
        return (left, right) -> {
            int result =
                    Long.compare(left.file().nonNullFirstRowId(), right.file().nonNullFirstRowId());
            if (result != 0) {
                return result;
            }
            result = Integer.compare(fileOrder(left), fileOrder(right));
            if (result != 0) {
                return result;
            }
            result =
                    Long.compare(right.file().maxSequenceNumber(), left.file().maxSequenceNumber());
            if (result != 0) {
                return result;
            }
            return left.file().fileName().compareTo(right.file().fileName());
        };
    }

    private int fileOrder(ManifestEntry entry) {
        if (isBlobFile(entry.file().fileName())) {
            return 1;
        }
        if (isVectorStoreFile(entry.file().fileName())) {
            return 2;
        }
        return 0;
    }

    private boolean isSpecialFile(ManifestEntry entry) {
        return isBlobFile(entry.file().fileName()) || isVectorStoreFile(entry.file().fileName());
    }

    /** Result of row-id reassignment. */
    public static class Result {
        public final long previousSnapshotId;
        public final long newSnapshotId;
        public final long fileCount;
        public final long rowCount;
        public final long indexFileCount;
        public final long firstAssignedRowId;
        public final long nextRowId;
        public final boolean reassigned;
        @Nullable public final String skipReason;

        public Result(
                long previousSnapshotId,
                long newSnapshotId,
                long fileCount,
                long rowCount,
                long indexFileCount,
                long firstAssignedRowId,
                long nextRowId) {
            this(
                    previousSnapshotId,
                    newSnapshotId,
                    fileCount,
                    rowCount,
                    indexFileCount,
                    firstAssignedRowId,
                    nextRowId,
                    true,
                    null);
        }

        public Result(
                long previousSnapshotId,
                long newSnapshotId,
                long fileCount,
                long rowCount,
                long indexFileCount,
                long firstAssignedRowId,
                long nextRowId,
                boolean reassigned) {
            this(
                    previousSnapshotId,
                    newSnapshotId,
                    fileCount,
                    rowCount,
                    indexFileCount,
                    firstAssignedRowId,
                    nextRowId,
                    reassigned,
                    null);
        }

        public Result(
                long previousSnapshotId,
                long newSnapshotId,
                long fileCount,
                long rowCount,
                long indexFileCount,
                long firstAssignedRowId,
                long nextRowId,
                boolean reassigned,
                @Nullable String skipReason) {
            this.previousSnapshotId = previousSnapshotId;
            this.newSnapshotId = newSnapshotId;
            this.fileCount = fileCount;
            this.rowCount = rowCount;
            this.indexFileCount = indexFileCount;
            this.firstAssignedRowId = firstAssignedRowId;
            this.nextRowId = nextRowId;
            this.reassigned = reassigned;
            this.skipReason = skipReason;
        }

        private static Result skipped(long snapshotId, long nextRowId, String reason) {
            return new Result(snapshotId, snapshotId, 0, 0, 0, nextRowId, nextRowId, false, reason);
        }
    }

    private static class RewrittenIndexManifest {
        @Nullable private final String indexManifest;
        private final long indexFileCount;

        private RewrittenIndexManifest(@Nullable String indexManifest, long indexFileCount) {
            this.indexManifest = indexManifest;
            this.indexFileCount = indexFileCount;
        }
    }

    private static class ReassignContext {
        private final Map<ManifestFileMeta, List<ManifestEntry>> planningManifestEntries =
                new HashMap<>();
        private final Map<String, List<IndexManifestEntry>> indexManifestEntries = new HashMap<>();

        private void retainLatest(
                List<ManifestFileMeta> manifestMetas, @Nullable String indexManifest) {
            planningManifestEntries.keySet().retainAll(new HashSet<>(manifestMetas));
            if (indexManifest == null) {
                indexManifestEntries.clear();
            } else {
                indexManifestEntries.keySet().retainAll(Collections.singleton(indexManifest));
            }
        }
    }

    private static class AssignmentPlan {
        private Snapshot snapshot;
        private List<ManifestFileMeta> manifestMetas;
        private final Map<BinaryRow, List<SourcedManifestEntry>> entriesToReassignByPartition;
        private final Map<FileEntry.Identifier, SourcedManifestEntry> plannedFiles;
        private final Map<BinaryRow, RowRangeMappingIndex> rowIdMappings;
        private long firstAssignedRowId;
        private long nextRowId;
        private long logicalRowCount;
        private final boolean hasCurrentFiles;

        private AssignmentPlan(
                Snapshot snapshot,
                List<ManifestFileMeta> manifestMetas,
                Map<BinaryRow, List<SourcedManifestEntry>> entriesToReassignByPartition,
                boolean hasCurrentFiles) {
            this.snapshot = snapshot;
            this.manifestMetas = manifestMetas;
            this.entriesToReassignByPartition = new LinkedHashMap<>();
            this.plannedFiles = new LinkedHashMap<>();
            this.rowIdMappings = new LinkedHashMap<>();
            this.hasCurrentFiles = hasCurrentFiles;
            for (List<SourcedManifestEntry> entries : entriesToReassignByPartition.values()) {
                addAll(entries);
            }
        }

        private boolean containsPartition(BinaryRow partition) {
            return entriesToReassignByPartition.containsKey(partition);
        }

        private void addAll(List<SourcedManifestEntry> entries) {
            for (SourcedManifestEntry entry : entries) {
                add(entry);
            }
        }

        private void add(SourcedManifestEntry entry) {
            FileEntry.Identifier identifier = entry.entry.identifier();
            checkState(
                    !plannedFiles.containsKey(identifier),
                    "Duplicate planned manifest entry for file %s.",
                    entry.entry);
            plannedFiles.put(identifier, entry);
            entriesToReassignByPartition
                    .computeIfAbsent(entry.entry.partition(), ignored -> new ArrayList<>())
                    .add(entry);
        }
    }

    private static class CommitAssignmentResult {
        private final boolean success;
        private final long indexFileCount;

        private CommitAssignmentResult(boolean success, long indexFileCount) {
            this.success = success;
            this.indexFileCount = indexFileCount;
        }
    }

    private static class CurrentManifest {
        private final List<SourcedManifestEntry> currentEntries;

        private CurrentManifest(List<SourcedManifestEntry> currentEntries) {
            this.currentEntries = currentEntries;
        }
    }

    private static class SourcedManifestEntry {
        @Nullable private ManifestFileMeta manifest;
        private final ManifestEntry entry;
        @Nullable private Long reassignedFirstRowId;

        private SourcedManifestEntry(ManifestFileMeta manifest, ManifestEntry entry) {
            this.manifest = manifest;
            this.entry = entry;
        }
    }

    private static class PartitionManifestRange {
        private final ManifestFileMeta manifest;
        private final BinaryRow minPartition;
        private final BinaryRow maxPartition;
        private final boolean containsNullPartition;
        private final int originalIndex;

        private PartitionManifestRange(
                ManifestFileMeta manifest,
                BinaryRow minPartition,
                BinaryRow maxPartition,
                boolean containsNullPartition,
                int originalIndex) {
            this.manifest = manifest;
            this.minPartition = minPartition;
            this.maxPartition = maxPartition;
            this.containsNullPartition = containsNullPartition;
            this.originalIndex = originalIndex;
        }
    }

    private static class PartitionAssignment {
        private final RowRangeMappingIndex rowIdMappings;
        private final long nextRowId;

        private PartitionAssignment(RowRangeMappingIndex rowIdMappings, long nextRowId) {
            this.rowIdMappings = rowIdMappings;
            this.nextRowId = nextRowId;
        }
    }
}
