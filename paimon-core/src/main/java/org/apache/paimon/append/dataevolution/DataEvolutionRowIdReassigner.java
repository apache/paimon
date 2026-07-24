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

        Snapshot latest = table.snapshotManager().latestSnapshot();
        checkArgument(
                latest != null, "Cannot reassign row IDs for empty table '%s'.", table.name());
        Long nextRowId = latest.nextRowId();
        checkState(
                nextRowId != null,
                "Next row id cannot be null for row-tracking table '%s'.",
                table.name());
        if (table.schema().logicalPartitionType().getFieldCount() == 0) {
            LOG.info(
                    "Skip reassigning row IDs for table {} because it is not partitioned.",
                    table.name());
            return Result.skipped(latest.id(), nextRowId, "table is not partitioned");
        }

        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        ManifestList manifestList = table.store().manifestListFactory().create();
        Optional<AssignmentPlan> optionalPlan =
                planAssignment(manifestList.readDataManifests(latest), manifestFile);
        if (!optionalPlan.isPresent()) {
            LOG.info(
                    "Skip reassigning row IDs for table {} because no partition requires reassignment.",
                    table.name());
            return Result.skipped(
                    latest.id(), nextRowId, "no partition requires row-id reassignment");
        }
        AssignmentPlan assignmentPlan = optionalPlan.get();

        for (int attempt = 1; attempt <= MAX_COMMIT_ATTEMPTS; attempt++) {
            Assignment assignment = assignmentPlan.createAssignment(latest);
            CommitAssignmentResult commitResult =
                    commitAssignment(assignment, manifestFile, manifestList, commitUser);
            if (commitResult.success) {
                LOG.info(
                        "Reassigned row IDs for table {} from {} to {}, partitions={}, files={}, rows={}.",
                        table.name(),
                        assignment.firstAssignedRowId,
                        assignment.nextRowId,
                        assignment.rowIdMappings.size(),
                        commitResult.fileCount,
                        assignment.logicalRowCount());
                return new Result(
                        assignment.snapshot.id(),
                        assignment.snapshot.id() + 1,
                        commitResult.fileCount,
                        assignment.logicalRowCount(),
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
            assignmentPlan =
                    advanceAssignmentPlan(
                            assignmentPlan, latest, newLatest, manifestFile, manifestList);
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

    private Optional<AssignmentPlan> planAssignment(
            List<ManifestFileMeta> manifestMetas, ManifestFile manifestFile) {
        List<List<ManifestFileMeta>> manifestGroups = manifestGroupsByPartition(manifestMetas);
        Map<BinaryRow, List<ManifestEntry>> entriesToReassignByPartition = new LinkedHashMap<>();

        for (List<ManifestFileMeta> manifestGroup : manifestGroups) {
            if (skipManifestGroupByPartitionFilter(manifestGroup)) {
                continue;
            }

            List<ManifestEntry> currentEntries = currentEntries(manifestGroup, manifestFile);
            if (currentEntries.isEmpty()) {
                continue;
            }

            Map<BinaryRow, List<ManifestEntry>> entriesByPartition =
                    entriesByPartition(currentEntries);
            Set<BinaryRow> partitionsToReassign = partitionsToReassign(entriesByPartition);
            if (partitionsToReassign.isEmpty()) {
                continue;
            }

            for (ManifestEntry entry : currentEntries) {
                if (partitionsToReassign.contains(entry.partition())) {
                    entriesToReassignByPartition
                            .computeIfAbsent(entry.partition(), ignored -> new ArrayList<>())
                            .add(entry);
                }
            }
        }

        if (entriesToReassignByPartition.isEmpty()) {
            return Optional.empty();
        }

        RelativeRowIdMappings relativeRowIdMappings =
                createRelativeRowIdMappings(entriesToReassignByPartition);
        return Optional.of(
                new AssignmentPlan(
                        findManifestMetasToRewrite(
                                manifestMetas, relativeRowIdMappings, manifestFile),
                        relativeRowIdMappings));
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

    private List<ManifestEntry> currentEntries(
            List<ManifestFileMeta> manifestMetas, ManifestFile manifestFile) {
        Set<FileEntry.Identifier> deletedIdentifiers =
                deletedIdentifiers(manifestFile, manifestMetas);

        List<ManifestEntry> currentEntries = new ArrayList<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            if (manifestMeta.numAddedFiles() <= 0) {
                continue;
            }
            List<ManifestEntry> entries = readPlanningManifestEntries(manifestFile, manifestMeta);
            for (ManifestEntry entry : entries) {
                if (entry.kind() == FileKind.ADD
                        && partitionIncluded(entry.partition())
                        && !deletedIdentifiers.contains(entry.identifier())) {
                    currentEntries.add(entry);
                }
            }
        }
        return currentEntries;
    }

    private Set<FileEntry.Identifier> deletedIdentifiers(
            ManifestFile manifestFile, List<ManifestFileMeta> manifestMetas) {
        Set<FileEntry.Identifier> deletedIdentifiers = new HashSet<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            if (manifestMeta.numDeletedFiles() <= 0) {
                continue;
            }
            List<ManifestEntry> entries = readPlanningManifestEntries(manifestFile, manifestMeta);
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
            Assignment assignment,
            ManifestFile manifestFile,
            ManifestList manifestList,
            String commitUser) {
        RewrittenDataManifests rewrittenDataManifests =
                writeManifestReplacements(assignment, manifestFile);
        Pair<String, Long> baseManifestList =
                writeBaseManifestList(
                        manifestList.readDataManifests(assignment.snapshot),
                        rewrittenDataManifests.manifestMetas,
                        manifestList);
        Pair<String, Long> deltaManifestList = manifestList.write(Collections.emptyList());
        RewrittenIndexManifest rewrittenIndexManifest = rewriteIndexManifest(assignment);

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
        return new CommitAssignmentResult(
                success, rewrittenDataManifests.fileCount, rewrittenIndexManifest.indexFileCount);
    }

    private AssignmentPlan advanceAssignmentPlan(
            AssignmentPlan assignmentPlan,
            Snapshot previous,
            Snapshot latest,
            ManifestFile manifestFile,
            ManifestList manifestList) {
        checkState(
                latest.id() > previous.id(),
                "Cannot advance row-id assignment from snapshot %s to %s.",
                previous.id(),
                latest.id());

        Map<String, ManifestFileMeta> manifestMetasToRewrite = new LinkedHashMap<>();
        for (ManifestFileMeta manifestMeta : assignmentPlan.manifestMetasToRewrite) {
            manifestMetasToRewrite.put(manifestMeta.fileName(), manifestMeta);
        }
        for (long id = previous.id() + 1; id <= latest.id(); id++) {
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
                                snapshot.commitKind(), snapshot.id(), previous.id()));
            }
            if (snapshot.commitKind() == Snapshot.CommitKind.ANALYZE) {
                continue;
            }
            checkState(
                    snapshot.commitKind() == Snapshot.CommitKind.APPEND,
                    "Unsupported snapshot kind %s while advancing row-id assignment.",
                    snapshot.commitKind());

            for (ManifestFileMeta manifestMeta : manifestList.readDeltaManifests(snapshot)) {
                boolean needsReassign = false;
                for (ManifestEntry entry :
                        readPlanningManifestEntries(manifestFile, manifestMeta)) {
                    checkState(
                            entry.kind() == FileKind.ADD,
                            "APPEND snapshot %s contains non-ADD manifest entry %s.",
                            snapshot.id(),
                            entry);
                    if (appendedEntryNeedsReassign(assignmentPlan, entry)) {
                        needsReassign = true;
                    }
                }
                if (needsReassign) {
                    manifestMetasToRewrite.put(manifestMeta.fileName(), manifestMeta);
                }
            }
        }

        List<ManifestFileMeta> latestManifestMetas = manifestList.readDataManifests(latest);
        Set<String> latestManifestFiles = new HashSet<>();
        for (ManifestFileMeta manifestMeta : latestManifestMetas) {
            latestManifestFiles.add(manifestMeta.fileName());
        }
        for (String plannedManifestFile : manifestMetasToRewrite.keySet()) {
            checkState(
                    latestManifestFiles.contains(plannedManifestFile),
                    "Cannot advance row-id assignment because planned manifest %s no longer exists after APPEND manifest merge.",
                    plannedManifestFile);
        }
        return new AssignmentPlan(
                new ArrayList<>(manifestMetasToRewrite.values()),
                assignmentPlan.relativeRowIdMappings);
    }

    private boolean appendedEntryNeedsReassign(
            AssignmentPlan assignmentPlan, ManifestEntry appendedEntry) {
        RowRangeMappingIndex mapping =
                assignmentPlan.relativeRowIdMappings.mappings.get(appendedEntry.partition());
        if (mapping == null) {
            return false;
        }

        Range appendedRange = appendedEntry.file().nonNullRowIdRange();
        if (mapping.map(appendedRange).isPresent()) {
            return true;
        }

        checkState(
                !mapping.overlaps(appendedRange),
                "Cannot advance row-id assignment because appended row-id range %s partially overlaps planned ranges in partition %s.",
                appendedRange,
                appendedEntry.partition());
        return false;
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

    private List<ManifestFileMeta> findManifestMetasToRewrite(
            List<ManifestFileMeta> manifestMetas,
            RelativeRowIdMappings relativeRowIdMappings,
            ManifestFile manifestFile) {
        PartitionPredicate plannedPartitionPredicate =
                PartitionPredicate.fromMultiple(
                        table.schema().logicalPartitionType(),
                        relativeRowIdMappings.mappings.keySet());
        checkState(plannedPartitionPredicate != null, "Planned partition predicate is null.");
        List<ManifestFileMeta> result = new ArrayList<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            if (!manifestGroupMayContainPartitions(
                    Collections.singletonList(manifestMeta), plannedPartitionPredicate)) {
                continue;
            }
            for (ManifestEntry entry : readPlanningManifestEntries(manifestFile, manifestMeta)) {
                RowRangeMappingIndex mapping =
                        relativeRowIdMappings.mappings.get(entry.partition());
                if (mapping != null && mapping.map(entry.file().nonNullRowIdRange()).isPresent()) {
                    result.add(manifestMeta);
                    break;
                }
            }
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

    private RewrittenDataManifests writeManifestReplacements(
            Assignment assignment, ManifestFile manifestFile) {
        Map<String, List<ManifestFileMeta>> rewrittenManifestMetas = new HashMap<>();
        long fileCount = 0L;
        for (ManifestFileMeta manifestMeta : assignment.manifestMetasToRewrite) {
            List<ManifestEntry> entries =
                    manifestFile.read(manifestMeta.fileName(), manifestMeta.fileSize());
            long reassignedAddFileCount = 0L;
            boolean hasRewrittenEntry = false;
            for (int i = 0; i < entries.size(); i++) {
                ManifestEntry entry = entries.get(i);
                RowRangeMappingIndex mapping = assignment.rowIdMappings.get(entry.partition());
                if (mapping == null) {
                    continue;
                }
                Optional<Range> reassignedRange = mapping.map(entry.file().nonNullRowIdRange());
                if (reassignedRange.isPresent()) {
                    validatePlanningEntry(entry);
                    entries.set(i, entry.assignFirstRowId(reassignedRange.get().from));
                    hasRewrittenEntry = true;
                    if (entry.kind() == FileKind.ADD) {
                        reassignedAddFileCount++;
                    }
                }
            }
            checkState(
                    hasRewrittenEntry,
                    "Cannot find entries to reassign in planned manifest %s.",
                    manifestMeta.fileName());
            rewrittenManifestMetas.put(manifestMeta.fileName(), manifestFile.write(entries));
            fileCount += reassignedAddFileCount;
        }
        return new RewrittenDataManifests(rewrittenManifestMetas, fileCount);
    }

    private Map<BinaryRow, List<ManifestEntry>> entriesByPartition(List<ManifestEntry> entries) {
        Comparator<ManifestEntry> comparator = entryComparator();
        Collections.sort(entries, comparator);

        Map<BinaryRow, List<ManifestEntry>> entriesByPartition = new LinkedHashMap<>();
        for (ManifestEntry entry : entries) {
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
        for (ManifestEntry entry : entries) {
            validatePlanningEntry(entry);
        }
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

    private RelativeRowIdMappings createRelativeRowIdMappings(
            Map<BinaryRow, List<ManifestEntry>> entriesByPartition) {
        Map<BinaryRow, List<Range>> logicalRangesByPartition = new LinkedHashMap<>();
        for (Map.Entry<BinaryRow, List<ManifestEntry>> partition : entriesByPartition.entrySet()) {
            for (ManifestEntry entry : partition.getValue()) {
                validatePlanningEntry(entry);
            }
            logicalRangesByPartition.put(partition.getKey(), logicalRanges(partition.getValue()));
        }
        return createRelativeRowIdMappingsFromRanges(logicalRangesByPartition);
    }

    private RelativeRowIdMappings createRelativeRowIdMappingsFromRanges(
            Map<BinaryRow, List<Range>> logicalRangesByPartition) {
        List<BinaryRow> partitions = new ArrayList<>(logicalRangesByPartition.keySet());
        RecordComparator partitionComparator = partitionComparator();
        Collections.sort(partitions, partitionComparator);

        Map<BinaryRow, RowRangeMappingIndex> result = new LinkedHashMap<>();
        long nextOffset = 0L;
        for (BinaryRow partition : partitions) {
            List<Range> ranges = new ArrayList<>(logicalRangesByPartition.get(partition));
            Collections.sort(
                    ranges,
                    (left, right) -> {
                        int compare = Long.compare(left.from, right.from);
                        return compare == 0 ? Long.compare(left.to, right.to) : compare;
                    });
            List<RowRangeMappingIndex.Mapping> mappings = new ArrayList<>(ranges.size());
            for (Range range : ranges) {
                mappings.add(RowRangeMappingIndex.mapping(range.from, range.to, nextOffset));
                nextOffset = Math.addExact(nextOffset, range.count());
            }
            result.put(partition, RowRangeMappingIndex.create(mappings));
        }
        return new RelativeRowIdMappings(result, nextOffset);
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

    private RewrittenIndexManifest rewriteIndexManifest(Assignment assignment) {
        if (assignment.snapshot.indexManifest() == null) {
            return new RewrittenIndexManifest(null, 0);
        }

        IndexManifestFile indexManifestFile = table.store().indexManifestFileFactory().create();
        List<IndexManifestEntry> indexEntries =
                indexManifestFile.read(assignment.snapshot.indexManifest());
        if (indexEntries.isEmpty()) {
            return new RewrittenIndexManifest(null, 0);
        }

        List<IndexManifestEntry> rewritten = new ArrayList<>(indexEntries.size());
        long globalIndexFileCount = 0;
        for (IndexManifestEntry entry : indexEntries) {
            checkState(
                    entry.kind() == FileKind.ADD,
                    "Index manifest '%s' contains non-current entry %s.",
                    assignment.snapshot.indexManifest(),
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
                if (!mappingIndex.overlaps(globalIndex.rowRange())) {
                    rewritten.add(entry);
                    continue;
                }
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
                            globalIndex.indexMeta(),
                            globalIndex.sourceMeta());
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
            ManifestFile manifestFile, ManifestFileMeta manifestMeta) {
        return manifestFile.read(
                manifestMeta.fileName(),
                manifestMeta.fileSize(),
                partitionPredicate,
                null,
                Filter.alwaysTrue(),
                entry -> partitionPredicate == null || partitionPredicate.test(entry.partition()),
                ManifestEntry::copyWithoutStats);
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

    private static class RelativeRowIdMappings {
        private final Map<BinaryRow, RowRangeMappingIndex> mappings;
        private final long totalOffset;

        private RelativeRowIdMappings(
                Map<BinaryRow, RowRangeMappingIndex> mappings, long totalOffset) {
            this.mappings = Collections.unmodifiableMap(new LinkedHashMap<>(mappings));
            this.totalOffset = totalOffset;
        }
    }

    private static class AssignmentPlan {
        private final List<ManifestFileMeta> manifestMetasToRewrite;
        private final RelativeRowIdMappings relativeRowIdMappings;

        private AssignmentPlan(
                List<ManifestFileMeta> manifestMetasToRewrite,
                RelativeRowIdMappings relativeRowIdMappings) {
            this.manifestMetasToRewrite = new ArrayList<>(manifestMetasToRewrite);
            this.relativeRowIdMappings = relativeRowIdMappings;
        }

        private Assignment createAssignment(Snapshot snapshot) {
            Long firstAssignedRowId = snapshot.nextRowId();
            checkState(
                    firstAssignedRowId != null,
                    "Next row id cannot be null for snapshot %s.",
                    snapshot.id());
            Map<BinaryRow, RowRangeMappingIndex> absoluteRowIdMappings = new LinkedHashMap<>();
            for (Map.Entry<BinaryRow, RowRangeMappingIndex> mapping :
                    relativeRowIdMappings.mappings.entrySet()) {
                absoluteRowIdMappings.put(
                        mapping.getKey(), mapping.getValue().shiftNewStarts(firstAssignedRowId));
            }
            return new Assignment(
                    snapshot,
                    manifestMetasToRewrite,
                    absoluteRowIdMappings,
                    firstAssignedRowId,
                    Math.addExact(firstAssignedRowId, relativeRowIdMappings.totalOffset));
        }
    }

    private static class Assignment {
        private final Snapshot snapshot;
        private final List<ManifestFileMeta> manifestMetasToRewrite;
        private final Map<BinaryRow, RowRangeMappingIndex> rowIdMappings;
        private final long firstAssignedRowId;
        private final long nextRowId;

        private Assignment(
                Snapshot snapshot,
                List<ManifestFileMeta> manifestMetasToRewrite,
                Map<BinaryRow, RowRangeMappingIndex> rowIdMappings,
                long firstAssignedRowId,
                long nextRowId) {
            this.snapshot = snapshot;
            this.manifestMetasToRewrite =
                    Collections.unmodifiableList(new ArrayList<>(manifestMetasToRewrite));
            this.rowIdMappings = Collections.unmodifiableMap(new LinkedHashMap<>(rowIdMappings));
            this.firstAssignedRowId = firstAssignedRowId;
            this.nextRowId = nextRowId;
        }

        private long logicalRowCount() {
            return nextRowId - firstAssignedRowId;
        }
    }

    private static class RewrittenDataManifests {
        private final Map<String, List<ManifestFileMeta>> manifestMetas;
        private final long fileCount;

        private RewrittenDataManifests(
                Map<String, List<ManifestFileMeta>> manifestMetas, long fileCount) {
            this.manifestMetas = manifestMetas;
            this.fileCount = fileCount;
        }
    }

    private static class CommitAssignmentResult {
        private final boolean success;
        private final long fileCount;
        private final long indexFileCount;

        private CommitAssignmentResult(boolean success, long fileCount, long indexFileCount) {
            this.success = success;
            this.fileCount = fileCount;
            this.indexFileCount = indexFileCount;
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
}
