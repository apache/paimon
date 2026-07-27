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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.VersionedObjectSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
                planAssignment(manifestList.readDataManifests(latest));
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

    private Optional<AssignmentPlan> planAssignment(List<ManifestFileMeta> manifestMetas) {
        List<List<ManifestFileMeta>> manifestGroups = manifestGroupsByPartition(manifestMetas);
        List<List<ManifestFileMeta>> includedGroups = new ArrayList<>();
        for (List<ManifestFileMeta> manifestGroup : manifestGroups) {
            if (!skipManifestGroupByPartitionFilter(manifestGroup)) {
                includedGroups.add(manifestGroup);
            }
        }

        PlanningState planningState =
                new PlanningState(table, partitionPredicate, new ArrayList<>(manifestMetas));
        planningState.validateGroups(includedGroups);
        for (List<ManifestFileMeta> manifestGroup : includedGroups) {
            planningState.planGroup(manifestGroup);
        }
        PlanningState.Result compactPlan = planningState.buildResult();
        if (compactPlan.isEmpty()) {
            return Optional.empty();
        }

        List<ManifestFileMeta> manifestMetasToRewrite =
                new ArrayList<>(compactPlan.manifestOrdinals.length);
        for (int ordinal : compactPlan.manifestOrdinals) {
            manifestMetasToRewrite.add(manifestMetas.get(ordinal));
        }

        Map<BinaryRow, RowRangeMappingIndex> mappings = new LinkedHashMap<>();
        for (PlanningState.PartitionMapping mapping : compactPlan.partitionMappings) {
            mappings.put(
                    mapping.partition,
                    RowRangeMappingIndex.createFromOwnedArrays(
                            mapping.oldStarts, mapping.oldEnds, mapping.newRelativeStarts));
        }
        return Optional.of(
                new AssignmentPlan(
                        manifestMetasToRewrite,
                        new RelativeRowIdMappings(mappings, compactPlan.totalOffset)));
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

    private RecordComparator partitionComparator() {
        return CodeGenUtils.newRecordComparator(
                table.schema().logicalPartitionType().getFieldTypes());
    }

    /**
     * Per-invocation state for {@link #planAssignment(List)}. Each pass projects only the manifest
     * fields it needs, and no state escapes the plan invocation.
     */
    static final class PlanningState {

        private static final int MANIFEST_ENTRY_VERSION = 2;
        private static final int EXCLUDED_PARTITION_CACHE_SIZE = 1024;
        private static final int CURRENT_ENTRY_WORDS = 3;
        private static final int MAX_INITIAL_CURRENT_ENTRIES = 1 << 24;
        private static final long CURRENT_SPECIAL = 1L << 32;
        private static final BinaryString ROW_ID_FIELD =
                BinaryString.fromString(SpecialFields.ROW_ID.name());
        private static final BinaryString BLOB_FILE_SUFFIX = BinaryString.fromString(".blob");
        private static final BinaryString VECTOR_FILE_MARKER = BinaryString.fromString(".vector.");

        private final FileStoreTable table;
        private final @Nullable PartitionPredicate partitionPredicate;
        private final List<ManifestFileMeta> manifestMetas;
        private final Map<String, Integer> manifestOrdinals;
        private final boolean[] rewrittenManifests;
        private final Projection deleteProjection;
        private final Projection addIdentifierProjection;
        private final Projection compactAddProjection;
        private final Projection rewriteProjection;
        private final Map<ByteArrayKey, SelectedPartition> selectedPartitions;
        private long nextManifestGroupOrdinal;
        private long nextRetainedAddScanOrdinal;

        PlanningState(
                FileStoreTable table,
                @Nullable PartitionPredicate partitionPredicate,
                List<ManifestFileMeta> manifestMetas) {
            this.table = table;
            this.partitionPredicate = partitionPredicate;
            this.manifestMetas = manifestMetas;
            this.manifestOrdinals = manifestOrdinals(manifestMetas);
            this.rewrittenManifests = new boolean[manifestMetas.size()];
            this.deleteProjection =
                    Projection.create(
                            table,
                            true,
                            "_FILE_NAME",
                            "_LEVEL",
                            "_EXTRA_FILES",
                            "_EMBEDDED_FILE_INDEX",
                            "_EXTERNAL_PATH");
            this.addIdentifierProjection =
                    Projection.create(
                            table,
                            true,
                            "_FILE_NAME",
                            "_ROW_COUNT",
                            "_LEVEL",
                            "_EXTRA_FILES",
                            "_EMBEDDED_FILE_INDEX",
                            "_EXTERNAL_PATH",
                            "_FIRST_ROW_ID",
                            "_WRITE_COLS",
                            "_MAX_SEQUENCE_NUMBER");
            this.compactAddProjection =
                    Projection.create(
                            table,
                            false,
                            "_FILE_NAME",
                            "_ROW_COUNT",
                            "_FIRST_ROW_ID",
                            "_WRITE_COLS",
                            "_MAX_SEQUENCE_NUMBER");
            this.rewriteProjection =
                    Projection.create(table, false, "_ROW_COUNT", "_FIRST_ROW_ID", "_WRITE_COLS");
            this.selectedPartitions = new LinkedHashMap<>();
        }

        void planGroup(List<ManifestFileMeta> manifestGroup) {
            long manifestGroupOrdinal = nextManifestGroupOrdinal;
            nextManifestGroupOrdinal = Math.addExact(nextManifestGroupOrdinal, 1L);
            GroupState group =
                    new GroupState(
                            table.schema().logicalPartitionType().getFieldCount(),
                            partitionPredicate,
                            EXCLUDED_PARTITION_CACHE_SIZE,
                            partitionPredicate == null
                                    ? initialCurrentEntryCapacity(manifestGroup)
                                    : 0);
            IdentifierScratch identifier = new IdentifierScratch();
            long[] rowRangeScratch = new long[2];

            for (ManifestFileMeta manifestMeta : manifestGroup) {
                if (manifestMeta.numDeletedFiles() <= 0) {
                    continue;
                }
                scan(
                        manifestMeta,
                        deleteProjection,
                        row -> {
                            if (kind(row) != FileKind.DELETE.toByteValue()) {
                                return true;
                            }
                            PartitionState partition = group.internPartition(row.getBinary(2));
                            if (partition == null) {
                                return true;
                            }
                            InternalRow file = file(row, deleteProjection);
                            identifier.encode(row, file, deleteProjection);
                            group.deletedIdentifiers.add(
                                    partition.id, identifier.bytes(), identifier.length());
                            return true;
                        });
            }

            Projection addProjection =
                    group.deletedIdentifiers.isEmpty()
                            ? compactAddProjection
                            : addIdentifierProjection;
            for (ManifestFileMeta manifestMeta : manifestGroup) {
                if (manifestMeta.numAddedFiles() <= 0) {
                    continue;
                }
                int manifestOrdinal = ordinal(manifestMeta);
                scan(
                        manifestMeta,
                        addProjection,
                        row -> {
                            if (kind(row) != FileKind.ADD.toByteValue()) {
                                return true;
                            }
                            PartitionState partition = group.internPartition(row.getBinary(2));
                            if (partition == null) {
                                return true;
                            }
                            InternalRow file = file(row, addProjection);
                            if (!group.deletedIdentifiers.isEmpty()) {
                                identifier.encode(row, file, addProjection);
                                if (group.deletedIdentifiers.contains(
                                        partition.id, identifier.bytes(), identifier.length())) {
                                    return true;
                                }
                            }

                            BinaryString fileName = requiredFileName(file, addProjection);
                            readRowRange(
                                    file,
                                    addProjection,
                                    manifestOrdinal,
                                    fileName,
                                    rowRangeScratch);
                            checkState(
                                    !writesRowId(file, addProjection),
                                    "Cannot reassign row IDs for file '%s' because it physically stores the row-id field.",
                                    fileName);
                            long maxSequenceNumber = requiredMaxSequenceNumber(file, addProjection);
                            long retainedAddScanOrdinal = nextRetainedAddScanOrdinal;
                            nextRetainedAddScanOrdinal =
                                    Math.addExact(nextRetainedAddScanOrdinal, 1L);
                            partition.considerLegacyOrderKey(
                                    manifestGroupOrdinal,
                                    rowRangeScratch[0],
                                    fileOrder(fileName),
                                    maxSequenceNumber,
                                    fileName,
                                    retainedAddScanOrdinal);
                            group.currentEntries.add(
                                    partition.id,
                                    isSpecialFile(fileName),
                                    rowRangeScratch[0],
                                    inclusiveRangeCount(rowRangeScratch[0], rowRangeScratch[1]));
                            return true;
                        });
            }
            identifier.release();
            group.releaseDeletedIdentifiers();

            GroupSelection[] groupSelections = group.finishAddPass();
            boolean selected = false;
            for (GroupSelection selection : groupSelections) {
                if (selection == null) {
                    continue;
                }
                selected = true;
                mergeSelectedPartition(selection);
            }
            if (!selected) {
                return;
            }

            for (ManifestFileMeta manifestMeta : manifestGroup) {
                int manifestOrdinal = ordinal(manifestMeta);
                scan(
                        manifestMeta,
                        rewriteProjection,
                        row -> {
                            PartitionState partition = group.internPartition(row.getBinary(2));
                            if (partition == null) {
                                return true;
                            }
                            if (partition.id >= groupSelections.length) {
                                return true;
                            }
                            GroupSelection selection = groupSelections[partition.id];
                            if (selection == null) {
                                return true;
                            }
                            InternalRow file = file(row, rewriteProjection);
                            readRowRange(
                                    file,
                                    rewriteProjection,
                                    manifestOrdinal,
                                    null,
                                    rowRangeScratch);
                            if (!rangesFullyCover(
                                    rowRangeScratch[0],
                                    rowRangeScratch[1],
                                    selection.logicalRanges)) {
                                return true;
                            }
                            checkState(
                                    !writesRowId(file, rewriteProjection),
                                    "Cannot reassign an entry in manifest '%s' because it physically stores the row-id field.",
                                    manifestMeta.fileName());
                            rewrittenManifests[manifestOrdinal] = true;
                            return false;
                        });
            }
        }

        Result buildResult() {
            if (selectedPartitions.isEmpty()) {
                return new Result(new int[0], Collections.emptyList(), 0L);
            }

            List<SelectedPartition> partitions = new ArrayList<>(selectedPartitions.values());
            RecordComparator typedComparator =
                    CodeGenUtils.newRecordComparator(
                            table.schema().logicalPartitionType().getFieldTypes());
            partitions.sort(
                    (left, right) -> {
                        int comparison = typedComparator.compare(left.partition, right.partition);
                        return comparison != 0
                                ? comparison
                                : left.legacyOrderKey.compareTo(right.legacyOrderKey);
                    });

            List<PartitionMapping> mappings = new ArrayList<>(partitions.size());
            long nextOffset = 0L;
            for (SelectedPartition partition : partitions) {
                partition.logicalRanges.normalizeOverlapping();
                int rangeCount = partition.logicalRanges.size();
                checkState(rangeCount > 0, "Selected partition has no logical row-id ranges.");
                OwnedPrimitiveRanges ownedRanges = partition.logicalRanges.takeOwned();
                long[] oldStarts = ownedRanges.starts;
                long[] oldEnds = ownedRanges.ends;
                long[] newStarts = new long[rangeCount];
                for (int i = 0; i < rangeCount; i++) {
                    newStarts[i] = nextOffset;
                    nextOffset =
                            Math.addExact(
                                    nextOffset, inclusiveRangeCount(oldStarts[i], oldEnds[i]));
                }
                mappings.add(
                        new PartitionMapping(partition.partition, oldStarts, oldEnds, newStarts));
            }

            int rewrittenCount = 0;
            for (boolean rewritten : rewrittenManifests) {
                if (rewritten) {
                    rewrittenCount++;
                }
            }
            int[] ordinals = new int[rewrittenCount];
            int position = 0;
            for (int i = 0; i < rewrittenManifests.length; i++) {
                if (rewrittenManifests[i]) {
                    ordinals[position++] = i;
                }
            }
            checkState(
                    ordinals.length > 0,
                    "Selected row-id mappings do not reference any manifest file.");
            return new Result(ordinals, mappings, nextOffset);
        }

        private void mergeSelectedPartition(GroupSelection selection) {
            ByteArrayLookupKey lookup = new ByteArrayLookupKey(selection.partition.serialized);
            SelectedPartition selected = selectedPartitions.get(lookup);
            if (selected == null) {
                LegacyPartitionOrderKey legacyOrderKey =
                        selection.partition.requiredLegacyOrderKey();
                selected =
                        new SelectedPartition(
                                selection.partition.serialized,
                                selection.partition.partition,
                                legacyOrderKey,
                                selection.logicalRanges);
                selectedPartitions.put(new ByteArrayKey(selection.partition.serialized), selected);
                return;
            }
            LegacyPartitionOrderKey incomingOrderKey = selection.partition.requiredLegacyOrderKey();
            if (incomingOrderKey.compareTo(selected.legacyOrderKey) < 0) {
                selected.legacyOrderKey = incomingOrderKey;
            }
            selected.logicalRanges.append(selection.logicalRanges);
            selected.logicalRanges.normalizeOverlapping();
        }

        private void scan(
                ManifestFileMeta manifestMeta, Projection projection, ProjectedRowVisitor visitor) {
            try (RecordReader<InternalRow> reader =
                    FileUtils.createFormatReader(
                            table.fileIO(),
                            projection.readerFactory,
                            table.store().pathFactory().toManifestFilePath(manifestMeta.fileName()),
                            manifestMeta.fileSize())) {
                boolean keepReading = true;
                while (keepReading) {
                    RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
                    if (batch == null) {
                        break;
                    }
                    try {
                        InternalRow row;
                        while ((row = batch.next()) != null) {
                            validateManifestRow(row);
                            if (!visitor.visit(row)) {
                                keepReading = false;
                                break;
                            }
                        }
                    } finally {
                        batch.releaseBatch();
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to read manifest file " + manifestMeta.fileName(), e);
            }
        }

        private static void validateManifestRow(InternalRow row) {
            checkState(
                    row.getInt(0) == MANIFEST_ENTRY_VERSION,
                    "Unsupported manifest entry version %s.",
                    row.getInt(0));
            byte kind = row.getByte(1);
            checkState(
                    kind == FileKind.ADD.toByteValue() || kind == FileKind.DELETE.toByteValue(),
                    "Unsupported manifest file kind %s.",
                    kind);
        }

        private static byte kind(InternalRow row) {
            return row.getByte(1);
        }

        private static InternalRow file(InternalRow row, Projection projection) {
            InternalRow file = row.getRow(projection.outerFilePosition, projection.fileFieldCount);
            checkState(file != null, "Manifest data file metadata cannot be null.");
            return file;
        }

        private static BinaryString requiredFileName(InternalRow file, Projection projection) {
            checkState(
                    projection.fileNamePosition >= 0,
                    "The selected projection does not contain file name.");
            BinaryString fileName = file.getString(projection.fileNamePosition);
            checkState(fileName != null, "Manifest file name cannot be null.");
            return fileName;
        }

        private static void readRowRange(
                InternalRow file,
                Projection projection,
                int manifestOrdinal,
                @Nullable BinaryString fileName,
                long[] result) {
            checkState(
                    projection.rowCountPosition >= 0 && projection.firstRowIdPosition >= 0,
                    "The selected projection does not contain a row-id range.");
            checkState(
                    !file.isNullAt(projection.firstRowIdPosition),
                    fileName == null
                            ? "Manifest %s contains a file without first row id."
                            : "File '%s' does not have first row id.",
                    fileName == null ? manifestOrdinal : fileName);
            long firstRowId = file.getLong(projection.firstRowIdPosition);
            long rowCount = file.getLong(projection.rowCountPosition);
            checkState(
                    rowCount > 0,
                    "Manifest %s contains a file with non-positive row count %s.",
                    manifestOrdinal,
                    rowCount);
            result[0] = firstRowId;
            result[1] = Math.addExact(firstRowId, rowCount - 1L);
        }

        private static boolean writesRowId(InternalRow file, Projection projection) {
            checkState(
                    projection.writeColsPosition >= 0,
                    "The selected projection does not contain write columns.");
            if (file.isNullAt(projection.writeColsPosition)) {
                return false;
            }
            InternalArray writeCols = file.getArray(projection.writeColsPosition);
            checkState(writeCols != null, "Manifest write columns cannot be null.");
            for (int i = 0; i < writeCols.size(); i++) {
                checkState(!writeCols.isNullAt(i), "Manifest write column cannot be null.");
                if (ROW_ID_FIELD.equals(writeCols.getString(i))) {
                    return true;
                }
            }
            return false;
        }

        private static long requiredMaxSequenceNumber(InternalRow file, Projection projection) {
            checkState(
                    projection.maxSequenceNumberPosition >= 0,
                    "The selected projection does not contain max sequence number.");
            checkState(
                    !file.isNullAt(projection.maxSequenceNumberPosition),
                    "Manifest max sequence number cannot be null.");
            return file.getLong(projection.maxSequenceNumberPosition);
        }

        private static int fileOrder(BinaryString fileName) {
            if (fileName.endsWith(BLOB_FILE_SUFFIX)) {
                return 1;
            }
            if (fileName.contains(VECTOR_FILE_MARKER)) {
                return 2;
            }
            return 0;
        }

        private static boolean isSpecialFile(BinaryString fileName) {
            return fileOrder(fileName) != 0;
        }

        private int ordinal(ManifestFileMeta manifestMeta) {
            Integer ordinal = manifestOrdinals.get(manifestMeta.fileName());
            checkArgument(
                    ordinal != null,
                    "Planning group references unknown manifest '%s'.",
                    manifestMeta.fileName());
            return ordinal;
        }

        private static int initialCurrentEntryCapacity(List<ManifestFileMeta> manifestGroup) {
            long addedCount = 0L;
            long deletedCount = 0L;
            for (ManifestFileMeta manifestMeta : manifestGroup) {
                checkState(
                        manifestMeta.numAddedFiles() >= 0 && manifestMeta.numDeletedFiles() >= 0,
                        "Manifest file counts cannot be negative.");
                addedCount = Math.addExact(addedCount, manifestMeta.numAddedFiles());
                deletedCount = Math.addExact(deletedCount, manifestMeta.numDeletedFiles());
            }
            return initialCurrentEntryCapacity(addedCount, deletedCount);
        }

        static int initialCurrentEntryCapacity(long addedCount, long deletedCount) {
            checkArgument(addedCount >= 0, "Added entry count cannot be negative.");
            checkArgument(deletedCount >= 0, "Deleted entry count cannot be negative.");
            // Counts are only a sizing hint: DELETE entries may be duplicated or may not match an
            // ADD
            // in this group. Estimate the live set, cap the eager allocation, and let
            // CurrentEntries
            // grow if the actual number of retained ADD entries is larger.
            long estimatedLiveCount = addedCount > deletedCount ? addedCount - deletedCount : 0L;
            return (int) Math.min(estimatedLiveCount, MAX_INITIAL_CURRENT_ENTRIES);
        }

        void validateGroups(List<List<ManifestFileMeta>> groups) {
            boolean[] seen = new boolean[manifestMetas.size()];
            for (List<ManifestFileMeta> group : groups) {
                checkArgument(group != null && !group.isEmpty(), "Manifest group cannot be empty.");
                for (ManifestFileMeta manifestMeta : group) {
                    checkArgument(manifestMeta != null, "Manifest meta cannot be null.");
                    int ordinal = ordinal(manifestMeta);
                    checkArgument(
                            !seen[ordinal],
                            "Manifest '%s' occurs in more than one planning group.",
                            manifestMeta.fileName());
                    seen[ordinal] = true;
                }
            }
        }

        private static Map<String, Integer> manifestOrdinals(List<ManifestFileMeta> manifestMetas) {
            Map<String, Integer> result = new HashMap<>();
            for (int i = 0; i < manifestMetas.size(); i++) {
                ManifestFileMeta manifestMeta = manifestMetas.get(i);
                checkArgument(manifestMeta != null, "Manifest meta cannot be null.");
                checkArgument(
                        result.put(manifestMeta.fileName(), i) == null,
                        "Duplicate manifest file '%s'.",
                        manifestMeta.fileName());
            }
            return result;
        }

        private static boolean rangesFullyCover(
                long rangeStart, long rangeEnd, PrimitiveRangeBuffer mappings) {
            long cursor = rangeStart;
            for (int i = 0; i < mappings.size(); i++) {
                long mappingStart = mappings.start(i);
                long mappingEnd = mappings.end(i);
                if (mappingEnd < cursor) {
                    continue;
                }
                if (mappingStart > cursor) {
                    return false;
                }
                long segmentEnd = Math.min(mappingEnd, rangeEnd);
                if (segmentEnd == rangeEnd) {
                    return true;
                }
                if (segmentEnd == Long.MAX_VALUE) {
                    return false;
                }
                cursor = segmentEnd + 1L;
            }
            return false;
        }

        private static long inclusiveRangeCount(long start, long end) {
            return Math.addExact(Math.subtractExact(end, start), 1L);
        }

        /**
         * Compact planner result. All new row starts are relative to the snapshot's next row ID.
         */
        static final class Result {

            final int[] manifestOrdinals;
            final List<PartitionMapping> partitionMappings;
            final long totalOffset;

            private Result(
                    int[] manifestOrdinals,
                    List<PartitionMapping> partitionMappings,
                    long totalOffset) {
                this.manifestOrdinals = manifestOrdinals;
                this.partitionMappings =
                        Collections.unmodifiableList(new ArrayList<>(partitionMappings));
                this.totalOffset = totalOffset;
            }

            boolean isEmpty() {
                return partitionMappings.isEmpty();
            }
        }

        /** Mapping arrays for one partition. Elements at the same index form one mapping. */
        static final class PartitionMapping {

            final BinaryRow partition;
            final long[] oldStarts;
            final long[] oldEnds;
            final long[] newRelativeStarts;

            private PartitionMapping(
                    BinaryRow partition,
                    long[] oldStarts,
                    long[] oldEnds,
                    long[] newRelativeStarts) {
                this.partition = partition;
                this.oldStarts = oldStarts;
                this.oldEnds = oldEnds;
                this.newRelativeStarts = newRelativeStarts;
            }
        }

        private interface ProjectedRowVisitor {

            boolean visit(InternalRow row);
        }

        private static final class Projection {

            private final FormatReaderFactory readerFactory;
            private final int outerFilePosition;
            private final int bucketPosition;
            private final int fileFieldCount;
            private final int fileNamePosition;
            private final int rowCountPosition;
            private final int levelPosition;
            private final int extraFilesPosition;
            private final int embeddedFileIndexPosition;
            private final int externalPathPosition;
            private final int firstRowIdPosition;
            private final int writeColsPosition;
            private final int maxSequenceNumberPosition;

            private Projection(
                    FormatReaderFactory readerFactory,
                    int outerFilePosition,
                    int bucketPosition,
                    int fileFieldCount,
                    int fileNamePosition,
                    int rowCountPosition,
                    int levelPosition,
                    int extraFilesPosition,
                    int embeddedFileIndexPosition,
                    int externalPathPosition,
                    int firstRowIdPosition,
                    int writeColsPosition,
                    int maxSequenceNumberPosition) {
                this.readerFactory = readerFactory;
                this.outerFilePosition = outerFilePosition;
                this.bucketPosition = bucketPosition;
                this.fileFieldCount = fileFieldCount;
                this.fileNamePosition = fileNamePosition;
                this.rowCountPosition = rowCountPosition;
                this.levelPosition = levelPosition;
                this.extraFilesPosition = extraFilesPosition;
                this.embeddedFileIndexPosition = embeddedFileIndexPosition;
                this.externalPathPosition = externalPathPosition;
                this.firstRowIdPosition = firstRowIdPosition;
                this.writeColsPosition = writeColsPosition;
                this.maxSequenceNumberPosition = maxSequenceNumberPosition;
            }

            private static Projection create(
                    FileStoreTable table, boolean includeBucket, String... projectedFileFields) {
                RowType fullType = VersionedObjectSerializer.versionType(ManifestEntry.SCHEMA);
                RowType projectedFileType = DataFileMeta.SCHEMA.project(projectedFileFields);
                List<DataField> fields = new ArrayList<>();
                fields.add(fullType.getField("_VERSION"));
                fields.add(fullType.getField("_KIND"));
                fields.add(fullType.getField("_PARTITION"));
                int bucketPosition = -1;
                if (includeBucket) {
                    bucketPosition = fields.size();
                    fields.add(fullType.getField("_BUCKET"));
                }
                int outerFilePosition = fields.size();
                fields.add(fullType.getField("_FILE").newType(projectedFileType));
                RowType projectedType = new RowType(false, fields);
                FileFormat format = FileFormat.manifestFormat(table.coreOptions());
                FormatReaderFactory readerFactory =
                        format.createReaderFactory(
                                fullType, projectedType, Collections.emptyList());
                return new Projection(
                        readerFactory,
                        outerFilePosition,
                        bucketPosition,
                        projectedFileFields.length,
                        position(projectedFileFields, "_FILE_NAME"),
                        position(projectedFileFields, "_ROW_COUNT"),
                        position(projectedFileFields, "_LEVEL"),
                        position(projectedFileFields, "_EXTRA_FILES"),
                        position(projectedFileFields, "_EMBEDDED_FILE_INDEX"),
                        position(projectedFileFields, "_EXTERNAL_PATH"),
                        position(projectedFileFields, "_FIRST_ROW_ID"),
                        position(projectedFileFields, "_WRITE_COLS"),
                        position(projectedFileFields, "_MAX_SEQUENCE_NUMBER"));
            }

            private static int position(String[] fields, String field) {
                for (int i = 0; i < fields.length; i++) {
                    if (field.equals(fields[i])) {
                        return i;
                    }
                }
                return -1;
            }
        }

        private static final class GroupState {

            private final GroupPartitionDictionary partitions;
            private final DeletedIdentifierSet deletedIdentifiers = new DeletedIdentifierSet();
            private final CurrentEntries currentEntries;

            private GroupState(
                    int partitionArity,
                    @Nullable PartitionPredicate partitionPredicate,
                    int excludedPartitionCacheSize,
                    int expectedAddEntryCount) {
                this.partitions =
                        new GroupPartitionDictionary(
                                partitionArity, partitionPredicate, excludedPartitionCacheSize);
                this.currentEntries = new CurrentEntries(expectedAddEntryCount);
            }

            private @Nullable PartitionState internPartition(byte[] serialized) {
                return partitions.intern(serialized);
            }

            private void releaseDeletedIdentifiers() {
                deletedIdentifiers.release();
            }

            private GroupSelection[] finishAddPass() {
                currentEntries.sort();
                GroupSelection[] selections = new GroupSelection[partitions.partitionCount()];
                long[] rangeScratch = new long[2];
                int groupStart = 0;
                while (groupStart < currentEntries.size()) {
                    int partitionId = currentEntries.partitionId(groupStart);
                    int groupEnd = groupStart + 1;
                    while (groupEnd < currentEntries.size()
                            && currentEntries.partitionId(groupEnd) == partitionId) {
                        groupEnd++;
                    }
                    int rangeScan =
                            currentEntries.scanLogicalRanges(groupStart, groupEnd, rangeScratch);
                    if (rangeScan > 0) {
                        PrimitiveRangeBuffer logicalRanges =
                                currentEntries.materializeLogicalRanges(
                                        groupStart, groupEnd, rangeScan, rangeScratch);
                        selections[partitionId] =
                                new GroupSelection(
                                        partitions.partition(partitionId), logicalRanges);
                    }
                    groupStart = groupEnd;
                }
                currentEntries.release();
                return selections;
            }
        }

        private static final class GroupPartitionDictionary {

            private final int expectedArity;
            private final @Nullable PartitionPredicate partitionPredicate;
            private final int excludedCacheSize;
            private final Map<ByteArrayKey, PartitionState> included = new HashMap<>();
            private final LinkedHashMap<ByteArrayKey, Boolean> excluded;
            private final ByteArrayLookupKey lookup = new ByteArrayLookupKey();
            private final List<PartitionState> partitions = new ArrayList<>();

            private GroupPartitionDictionary(
                    int expectedArity,
                    @Nullable PartitionPredicate partitionPredicate,
                    int excludedCacheSize) {
                checkArgument(excludedCacheSize >= 0, "Excluded cache size cannot be negative.");
                this.expectedArity = expectedArity;
                this.partitionPredicate = partitionPredicate;
                this.excludedCacheSize = excludedCacheSize;
                this.excluded =
                        new LinkedHashMap<ByteArrayKey, Boolean>(16, 0.75f, true) {
                            @Override
                            protected boolean removeEldestEntry(
                                    Map.Entry<ByteArrayKey, Boolean> eldest) {
                                return size() > GroupPartitionDictionary.this.excludedCacheSize;
                            }
                        };
            }

            private @Nullable PartitionState intern(byte[] serialized) {
                checkArgument(serialized != null, "Serialized partition cannot be null.");
                lookup.reset(serialized);
                try {
                    PartitionState existing = included.get(lookup);
                    if (existing != null) {
                        return existing;
                    }
                    if (partitionPredicate != null && excluded.get(lookup) != null) {
                        return null;
                    }

                    byte[] canonical = copyValidatedPartition(expectedArity, serialized);
                    BinaryRow partition = SerializationUtils.deserializeBinaryRow(canonical);
                    if (partitionPredicate != null && !partitionPredicate.test(partition)) {
                        if (excludedCacheSize > 0) {
                            excluded.put(new ByteArrayKey(canonical), Boolean.TRUE);
                        }
                        return null;
                    }

                    checkState(
                            partitions.size() < Integer.MAX_VALUE,
                            "Too many partitions in one manifest group.");
                    PartitionState created =
                            new PartitionState(partitions.size(), canonical, partition);
                    partitions.add(created);
                    included.put(new ByteArrayKey(canonical), created);
                    return created;
                } finally {
                    lookup.clear();
                }
            }

            private int partitionCount() {
                return partitions.size();
            }

            private PartitionState partition(int partitionId) {
                return partitions.get(partitionId);
            }
        }

        private static byte[] copyValidatedPartition(int expectedArity, byte[] serialized) {
            checkArgument(serialized.length >= 4, "Serialized partition is truncated.");
            int arity =
                    ((serialized[0] & 0xFF) << 24)
                            | ((serialized[1] & 0xFF) << 16)
                            | ((serialized[2] & 0xFF) << 8)
                            | (serialized[3] & 0xFF);
            checkArgument(
                    arity == expectedArity,
                    "Serialized partition has arity %s, expected %s.",
                    arity,
                    expectedArity);
            int fixedLength = BinaryRow.calculateFixPartSizeInBytes(arity);
            checkArgument(
                    serialized.length - 4 >= fixedLength,
                    "Serialized partition payload is truncated.");
            return Arrays.copyOf(serialized, serialized.length);
        }

        private static final class PartitionState {

            private final int id;
            private final byte[] serialized;
            private final BinaryRow partition;
            private @Nullable LegacyPartitionOrderKey legacyOrderKey;

            private PartitionState(int id, byte[] serialized, BinaryRow partition) {
                this.id = id;
                this.serialized = serialized;
                this.partition = partition;
            }

            private void considerLegacyOrderKey(
                    long manifestGroupOrdinal,
                    long firstRowId,
                    int fileOrder,
                    long maxSequenceNumber,
                    BinaryString fileName,
                    long retainedAddScanOrdinal) {
                if (legacyOrderKey == null) {
                    legacyOrderKey =
                            new LegacyPartitionOrderKey(
                                    manifestGroupOrdinal,
                                    firstRowId,
                                    fileOrder,
                                    maxSequenceNumber,
                                    fileName.toString(),
                                    retainedAddScanOrdinal);
                    return;
                }

                int comparison =
                        Long.compare(manifestGroupOrdinal, legacyOrderKey.manifestGroupOrdinal);
                if (comparison == 0) {
                    comparison = Long.compare(firstRowId, legacyOrderKey.firstRowId);
                }
                if (comparison == 0) {
                    comparison = Integer.compare(fileOrder, legacyOrderKey.fileOrder);
                }
                if (comparison == 0) {
                    comparison = Long.compare(legacyOrderKey.maxSequenceNumber, maxSequenceNumber);
                }

                String stableFileName = null;
                if (comparison == 0) {
                    stableFileName = fileName.toString();
                    comparison = stableFileName.compareTo(legacyOrderKey.fileName);
                }
                if (comparison == 0) {
                    comparison =
                            Long.compare(
                                    retainedAddScanOrdinal, legacyOrderKey.retainedAddScanOrdinal);
                }
                if (comparison < 0) {
                    if (stableFileName == null) {
                        stableFileName = fileName.toString();
                    }
                    legacyOrderKey =
                            new LegacyPartitionOrderKey(
                                    manifestGroupOrdinal,
                                    firstRowId,
                                    fileOrder,
                                    maxSequenceNumber,
                                    stableFileName,
                                    retainedAddScanOrdinal);
                }
            }

            private LegacyPartitionOrderKey requiredLegacyOrderKey() {
                checkState(
                        legacyOrderKey != null,
                        "Selected partition does not have a retained ADD ordering key.");
                return legacyOrderKey;
            }
        }

        private static final class LegacyPartitionOrderKey
                implements Comparable<LegacyPartitionOrderKey> {

            private final long manifestGroupOrdinal;
            private final long firstRowId;
            private final int fileOrder;
            private final long maxSequenceNumber;
            private final String fileName;
            private final long retainedAddScanOrdinal;

            private LegacyPartitionOrderKey(
                    long manifestGroupOrdinal,
                    long firstRowId,
                    int fileOrder,
                    long maxSequenceNumber,
                    String fileName,
                    long retainedAddScanOrdinal) {
                this.manifestGroupOrdinal = manifestGroupOrdinal;
                this.firstRowId = firstRowId;
                this.fileOrder = fileOrder;
                this.maxSequenceNumber = maxSequenceNumber;
                this.fileName = fileName;
                this.retainedAddScanOrdinal = retainedAddScanOrdinal;
            }

            @Override
            public int compareTo(LegacyPartitionOrderKey other) {
                int comparison = Long.compare(manifestGroupOrdinal, other.manifestGroupOrdinal);
                if (comparison != 0) {
                    return comparison;
                }
                comparison = Long.compare(firstRowId, other.firstRowId);
                if (comparison != 0) {
                    return comparison;
                }
                comparison = Integer.compare(fileOrder, other.fileOrder);
                if (comparison != 0) {
                    return comparison;
                }
                comparison = Long.compare(other.maxSequenceNumber, maxSequenceNumber);
                if (comparison != 0) {
                    return comparison;
                }
                comparison = fileName.compareTo(other.fileName);
                return comparison != 0
                        ? comparison
                        : Long.compare(retainedAddScanOrdinal, other.retainedAddScanOrdinal);
            }
        }

        private static final class GroupSelection {

            private final PartitionState partition;
            private final PrimitiveRangeBuffer logicalRanges;

            private GroupSelection(PartitionState partition, PrimitiveRangeBuffer logicalRanges) {
                this.partition = partition;
                this.logicalRanges = logicalRanges;
            }
        }

        private static final class SelectedPartition {

            private final byte[] serializedPartition;
            private final BinaryRow partition;
            private LegacyPartitionOrderKey legacyOrderKey;
            private final PrimitiveRangeBuffer logicalRanges;

            private SelectedPartition(
                    byte[] serializedPartition,
                    BinaryRow partition,
                    LegacyPartitionOrderKey legacyOrderKey,
                    PrimitiveRangeBuffer logicalRanges) {
                this.serializedPartition = serializedPartition;
                this.partition = partition;
                this.legacyOrderKey = legacyOrderKey;
                this.logicalRanges = logicalRanges;
            }
        }

        /** Object-free storage for current entries. */
        static final class CurrentEntries {

            private long[] words;
            private int size;

            CurrentEntries() {
                this(0);
            }

            CurrentEntries(int expectedEntries) {
                checkArgument(
                        expectedEntries >= 0, "Expected current entry count cannot be negative.");
                this.words = new long[Math.multiplyExact(expectedEntries, CURRENT_ENTRY_WORDS)];
            }

            void add(int partitionId, boolean special, long firstRowId, long rowCount) {
                checkArgument(partitionId >= 0, "Partition id cannot be negative.");
                checkArgument(rowCount > 0, "Row count must be positive.");
                Math.addExact(firstRowId, rowCount - 1L);
                ensureCapacity(Math.addExact(size, 1));
                int offset = size * CURRENT_ENTRY_WORDS;
                words[offset] =
                        Integer.toUnsignedLong(partitionId) | (special ? CURRENT_SPECIAL : 0L);
                words[offset + 1] = firstRowId;
                words[offset + 2] = rowCount;
                size++;
            }

            int size() {
                return size;
            }

            int retainedWordCount() {
                return words.length;
            }

            int usedWordCount() {
                return size * CURRENT_ENTRY_WORDS;
            }

            private void release() {
                words = new long[0];
                size = 0;
            }

            int partitionId(int index) {
                return (int) words[offset(index)];
            }

            private boolean special(int index) {
                return (words[offset(index)] & CURRENT_SPECIAL) != 0;
            }

            private long firstRowId(int index) {
                return words[offset(index) + 1];
            }

            private long rowCount(int index) {
                return words[offset(index) + 2];
            }

            private long lastRowId(int index) {
                return firstRowId(index) + rowCount(index) - 1L;
            }

            private int offset(int index) {
                checkArgument(index >= 0 && index < size, "Current entry index is out of bounds.");
                return index * CURRENT_ENTRY_WORDS;
            }

            private void ensureCapacity(int requiredEntries) {
                long requiredWords = (long) requiredEntries * CURRENT_ENTRY_WORDS;
                checkState(
                        requiredWords <= Integer.MAX_VALUE,
                        "Too many current entries in one manifest group.");
                if (requiredWords <= words.length) {
                    return;
                }
                int newLength = Math.max(48, words.length);
                while (newLength < requiredWords) {
                    int grown = newLength + (newLength >>> 1);
                    if (grown <= newLength || grown > Integer.MAX_VALUE) {
                        newLength = (int) requiredWords;
                        break;
                    }
                    newLength = grown;
                }
                words = Arrays.copyOf(words, newLength);
            }

            private void sort() {
                if (size > 1) {
                    sort(0, size - 1);
                }
            }

            private void sort(int left, int right) {
                while (left < right) {
                    int middle = left + ((right - left) >>> 1);
                    long pivotPartition = words[middle * CURRENT_ENTRY_WORDS];
                    long pivotFirst = words[middle * CURRENT_ENTRY_WORDS + 1];
                    long pivotCount = words[middle * CURRENT_ENTRY_WORDS + 2];
                    int lower = left;
                    int current = left;
                    int upper = right;
                    while (current <= upper) {
                        int comparison = compare(current, pivotPartition, pivotFirst, pivotCount);
                        if (comparison < 0) {
                            swap(lower++, current++);
                        } else if (comparison > 0) {
                            swap(current, upper--);
                        } else {
                            current++;
                        }
                    }

                    if (lower - left < right - upper) {
                        if (left < lower - 1) {
                            sort(left, lower - 1);
                        }
                        left = upper + 1;
                    } else {
                        if (upper + 1 < right) {
                            sort(upper + 1, right);
                        }
                        right = lower - 1;
                    }
                }
            }

            private int compare(int index, long pivotPartition, long pivotFirst, long pivotCount) {
                int offset = index * CURRENT_ENTRY_WORDS;
                int result =
                        Long.compare(words[offset] & 0xFFFF_FFFFL, pivotPartition & 0xFFFF_FFFFL);
                if (result != 0) {
                    return result;
                }
                result = Long.compare(words[offset + 1], pivotFirst);
                if (result != 0) {
                    return result;
                }
                long end = words[offset + 1] + words[offset + 2] - 1L;
                long pivotEnd = pivotFirst + pivotCount - 1L;
                return Long.compare(end, pivotEnd);
            }

            private void swap(int left, int right) {
                if (left == right) {
                    return;
                }
                int leftOffset = left * CURRENT_ENTRY_WORDS;
                int rightOffset = right * CURRENT_ENTRY_WORDS;
                for (int i = 0; i < CURRENT_ENTRY_WORDS; i++) {
                    long value = words[leftOffset + i];
                    words[leftOffset + i] = words[rightOffset + i];
                    words[rightOffset + i] = value;
                }
            }

            /**
             * Scans logical ranges without retaining one object (or even one primitive pair) per
             * range.
             *
             * <p>The absolute return value is the number of logical ranges. A negative result means
             * that all logical ranges are contiguous and therefore this partition does not need a
             * plan. A positive result means that the ranges are fragmented and need
             * materialization.
             */
            private int scanLogicalRanges(int from, int to, long[] rangeScratch) {
                checkArgument(from >= 0 && from < to && to <= size, "Invalid entry slice.");
                int overlapStart = from;
                long currentEnd = lastRowId(from);
                int rangeCount = 0;
                boolean contiguous = true;
                boolean hasPrevious = false;
                long previousEnd = 0L;
                for (int i = from + 1; i < to; i++) {
                    if (firstRowId(i) <= currentEnd) {
                        currentEnd = Math.max(currentEnd, lastRowId(i));
                    } else {
                        computeLogicalRange(overlapStart, i, rangeScratch);
                        rangeCount++;
                        if (hasPrevious
                                && (previousEnd == Long.MAX_VALUE
                                        || rangeScratch[0] != previousEnd + 1L)) {
                            contiguous = false;
                        }
                        previousEnd = rangeScratch[1];
                        hasPrevious = true;
                        overlapStart = i;
                        currentEnd = lastRowId(i);
                    }
                }
                computeLogicalRange(overlapStart, to, rangeScratch);
                rangeCount++;
                if (hasPrevious
                        && (previousEnd == Long.MAX_VALUE || rangeScratch[0] != previousEnd + 1L)) {
                    contiguous = false;
                }
                return contiguous ? -rangeCount : rangeCount;
            }

            private PrimitiveRangeBuffer materializeLogicalRanges(
                    int from, int to, int expectedRangeCount, long[] rangeScratch) {
                checkArgument(
                        from >= 0 && from < to && to <= size && expectedRangeCount > 0,
                        "Invalid fragmented entry slice.");
                PrimitiveRangeBuffer ranges = new PrimitiveRangeBuffer(expectedRangeCount);
                int overlapStart = from;
                long currentEnd = lastRowId(from);
                for (int i = from + 1; i < to; i++) {
                    if (firstRowId(i) <= currentEnd) {
                        currentEnd = Math.max(currentEnd, lastRowId(i));
                    } else {
                        computeLogicalRange(overlapStart, i, rangeScratch);
                        ranges.add(rangeScratch[0], rangeScratch[1]);
                        overlapStart = i;
                        currentEnd = lastRowId(i);
                    }
                }
                computeLogicalRange(overlapStart, to, rangeScratch);
                ranges.add(rangeScratch[0], rangeScratch[1]);
                checkState(
                        ranges.size() == expectedRangeCount,
                        "Logical range count changed between scan and materialization.");
                return ranges;
            }

            private void computeLogicalRange(int from, int to, long[] result) {
                boolean hasOrdinary = false;
                long ordinaryStart = 0L;
                long ordinaryEnd = 0L;
                long spanningStart = Long.MAX_VALUE;
                long spanningEnd = Long.MIN_VALUE;
                for (int i = from; i < to; i++) {
                    long start = firstRowId(i);
                    long end = lastRowId(i);
                    spanningStart = Math.min(spanningStart, start);
                    spanningEnd = Math.max(spanningEnd, end);
                    if (!special(i)) {
                        checkState(
                                !hasOrdinary || (ordinaryStart == start && ordinaryEnd == end),
                                "Data files in one overlapping row-id group must have the same row-id range.");
                        ordinaryStart = start;
                        ordinaryEnd = end;
                        hasOrdinary = true;
                    }
                }
                long logicalStart = hasOrdinary ? ordinaryStart : spanningStart;
                long logicalEnd = hasOrdinary ? ordinaryEnd : spanningEnd;
                for (int i = from; i < to; i++) {
                    checkState(
                            firstRowId(i) >= logicalStart && lastRowId(i) <= logicalEnd,
                            "File row-id range is outside its logical row-id range.");
                }
                result[0] = logicalStart;
                result[1] = logicalEnd;
            }

            @Nullable
            PrimitiveRangeBuffer selectedRangesForTesting() {
                checkState(size > 0, "Cannot inspect an empty current-entry buffer.");
                sort();
                int partitionId = partitionId(0);
                for (int i = 1; i < size; i++) {
                    checkState(
                            partitionId(i) == partitionId,
                            "The structural range test helper requires one partition.");
                }
                long[] rangeScratch = new long[2];
                int rangeScan = scanLogicalRanges(0, size, rangeScratch);
                return rangeScan < 0
                        ? null
                        : materializeLogicalRanges(0, size, rangeScan, rangeScratch);
            }
        }

        /** Compact, collision-safe set backed by primitive arrays and one identifier byte arena. */
        static final class DeletedIdentifierSet {

            private static final float LOAD_FACTOR = 0.75f;

            private int[] buckets = filledWithMinusOne(16);
            private long[] hashes = new long[16];
            private int[] partitionIds = new int[16];
            private int[] offsets = new int[16];
            private int[] lengths = new int[16];
            private int[] next = new int[16];
            private byte[] arena = new byte[256];
            private int arenaSize;
            private int size;

            boolean isEmpty() {
                return size == 0;
            }

            int size() {
                return size;
            }

            int retainedIdentifierBytes() {
                return arenaSize;
            }

            private void release() {
                buckets = filledWithMinusOne(16);
                hashes = new long[0];
                partitionIds = new int[0];
                offsets = new int[0];
                lengths = new int[0];
                next = new int[0];
                arena = new byte[0];
                arenaSize = 0;
                size = 0;
            }

            void add(int partitionId, byte[] identifier, int length) {
                checkIdentifier(identifier, length);
                long hash = hash(partitionId, identifier, length);
                if (contains(partitionId, identifier, length, hash)) {
                    return;
                }
                if (size + 1 > (int) (buckets.length * LOAD_FACTOR)) {
                    growBuckets();
                }
                ensureEntryCapacity(size + 1);
                ensureArenaCapacity(length);
                int offset = arenaSize;
                System.arraycopy(identifier, 0, arena, offset, length);
                arenaSize = Math.addExact(arenaSize, length);

                int bucket = bucket(hash);
                hashes[size] = hash;
                partitionIds[size] = partitionId;
                offsets[size] = offset;
                lengths[size] = length;
                next[size] = buckets[bucket];
                buckets[bucket] = size;
                size++;
            }

            boolean contains(int partitionId, byte[] identifier, int length) {
                checkIdentifier(identifier, length);
                return contains(
                        partitionId, identifier, length, hash(partitionId, identifier, length));
            }

            private boolean contains(int partitionId, byte[] identifier, int length, long hash) {
                for (int entry = buckets[bucket(hash)]; entry >= 0; entry = next[entry]) {
                    if (hashes[entry] == hash
                            && partitionIds[entry] == partitionId
                            && lengths[entry] == length
                            && bytesEqual(arena, offsets[entry], identifier, length)) {
                        return true;
                    }
                }
                return false;
            }

            private void growBuckets() {
                checkState(
                        buckets.length < (1 << 30),
                        "Too many deleted identifiers in one manifest group.");
                int[] grown = filledWithMinusOne(buckets.length << 1);
                for (int entry = 0; entry < size; entry++) {
                    int bucket = bucket(hashes[entry], grown.length);
                    next[entry] = grown[bucket];
                    grown[bucket] = entry;
                }
                buckets = grown;
            }

            private void ensureEntryCapacity(int required) {
                if (required <= hashes.length) {
                    return;
                }
                int grown = Math.max(required, hashes.length + (hashes.length >>> 1));
                hashes = Arrays.copyOf(hashes, grown);
                partitionIds = Arrays.copyOf(partitionIds, grown);
                offsets = Arrays.copyOf(offsets, grown);
                lengths = Arrays.copyOf(lengths, grown);
                next = Arrays.copyOf(next, grown);
            }

            private void ensureArenaCapacity(int additional) {
                int required;
                try {
                    required = Math.addExact(arenaSize, additional);
                } catch (ArithmeticException e) {
                    throw new IllegalStateException(
                            "Deleted identifier arena exceeds the Java array limit.", e);
                }
                if (required <= arena.length) {
                    return;
                }
                int grown = Math.max(required, arena.length + (arena.length >>> 1));
                if (grown < 0) {
                    grown = required;
                }
                arena = Arrays.copyOf(arena, grown);
            }

            private int bucket(long hash) {
                return bucket(hash, buckets.length);
            }

            private static int bucket(long hash, int bucketCount) {
                return ((int) (hash ^ (hash >>> 32))) & (bucketCount - 1);
            }

            private static long hash(int partitionId, byte[] bytes, int length) {
                long hash = 0xcbf29ce484222325L;
                hash ^= Integer.toUnsignedLong(partitionId);
                hash *= 0x100000001b3L;
                for (int i = 0; i < length; i++) {
                    hash ^= bytes[i] & 0xFFL;
                    hash *= 0x100000001b3L;
                }
                return hash;
            }

            private static boolean bytesEqual(
                    byte[] left, int leftOffset, byte[] right, int length) {
                for (int i = 0; i < length; i++) {
                    if (left[leftOffset + i] != right[i]) {
                        return false;
                    }
                }
                return true;
            }

            private static void checkIdentifier(byte[] identifier, int length) {
                checkArgument(identifier != null, "Identifier bytes cannot be null.");
                checkArgument(
                        length >= 0 && length <= identifier.length,
                        "Invalid identifier length %s.",
                        length);
            }

            private static int[] filledWithMinusOne(int length) {
                int[] values = new int[length];
                Arrays.fill(values, -1);
                return values;
            }
        }

        private static final class IdentifierScratch {

            private byte[] bytes = new byte[256];
            private int length;

            private void encode(InternalRow outer, InternalRow file, Projection projection) {
                checkState(
                        projection.bucketPosition >= 0
                                && projection.fileNamePosition >= 0
                                && projection.levelPosition >= 0
                                && projection.extraFilesPosition >= 0
                                && projection.embeddedFileIndexPosition >= 0
                                && projection.externalPathPosition >= 0,
                        "The selected projection does not contain a complete file identifier.");
                length = 0;
                putInt(outer.getInt(projection.bucketPosition));
                putInt(file.getInt(projection.levelPosition));
                putString(requiredFileName(file, projection));

                InternalArray extraFiles = file.getArray(projection.extraFilesPosition);
                checkState(extraFiles != null, "Manifest extra files cannot be null.");
                putInt(extraFiles.size());
                for (int i = 0; i < extraFiles.size(); i++) {
                    checkState(!extraFiles.isNullAt(i), "Extra file name cannot be null.");
                    putString(extraFiles.getString(i));
                }

                if (file.isNullAt(projection.embeddedFileIndexPosition)) {
                    putInt(-1);
                } else {
                    putBytes(file.getBinary(projection.embeddedFileIndexPosition));
                }
                if (file.isNullAt(projection.externalPathPosition)) {
                    putInt(-1);
                } else {
                    putString(file.getString(projection.externalPathPosition));
                }
            }

            private byte[] bytes() {
                return bytes;
            }

            private int length() {
                return length;
            }

            private void release() {
                bytes = new byte[0];
                length = 0;
            }

            private void putString(BinaryString value) {
                checkState(value != null, "Manifest string field cannot be null.");
                int valueLength = value.getSizeInBytes();
                putInt(valueLength);
                ensureCapacity(valueLength);
                MemorySegmentUtils.copyToBytes(
                        value.getSegments(), value.getOffset(), bytes, length, valueLength);
                length += valueLength;
            }

            private void putBytes(byte[] value) {
                checkState(value != null, "Manifest binary field cannot be null.");
                putInt(value.length);
                ensureCapacity(value.length);
                System.arraycopy(value, 0, bytes, length, value.length);
                length += value.length;
            }

            private void putInt(int value) {
                ensureCapacity(Integer.BYTES);
                bytes[length++] = (byte) (value >>> 24);
                bytes[length++] = (byte) (value >>> 16);
                bytes[length++] = (byte) (value >>> 8);
                bytes[length++] = (byte) value;
            }

            private void ensureCapacity(int additional) {
                int required = Math.addExact(length, additional);
                if (required <= bytes.length) {
                    return;
                }
                int grown = Math.max(required, bytes.length + (bytes.length >>> 1));
                bytes = Arrays.copyOf(bytes, grown);
            }
        }

        /**
         * Object-free logical range storage.
         *
         * <p>Starts and ends are kept in separate primitive arrays so that the planner can transfer
         * ownership of both arrays directly into the final mapping. The common one-group path
         * allocates the exact range count and requires no copy during that transfer.
         */
        static final class PrimitiveRangeBuffer {

            private long[] starts;
            private long[] ends;
            private int size;
            private boolean sorted = true;

            private PrimitiveRangeBuffer(int expectedRanges) {
                checkArgument(expectedRanges >= 0, "Expected range count cannot be negative.");
                starts = new long[expectedRanges];
                ends = new long[expectedRanges];
            }

            int size() {
                return size;
            }

            int retainedWordCount() {
                return Math.addExact(starts.length, ends.length);
            }

            long start(int index) {
                checkIndex(index);
                return starts[index];
            }

            long end(int index) {
                checkIndex(index);
                return ends[index];
            }

            private void add(long start, long end) {
                checkArgument(start <= end, "Invalid row-id range [%s, %s].", start, end);
                ensureCapacity(Math.addExact(size, 1));
                if (size > 0 && compare(starts[size - 1], ends[size - 1], start, end) > 0) {
                    sorted = false;
                }
                starts[size] = start;
                ends[size] = end;
                size++;
            }

            private void append(PrimitiveRangeBuffer other) {
                checkArgument(other != null, "Ranges to append cannot be null.");
                if (other.size == 0) {
                    return;
                }
                int oldSize = size;
                int combinedSize = Math.addExact(size, other.size);
                ensureCapacity(combinedSize);
                if (oldSize > 0
                        && compare(
                                        starts[oldSize - 1],
                                        ends[oldSize - 1],
                                        other.starts[0],
                                        other.ends[0])
                                > 0) {
                    sorted = false;
                }
                sorted &= other.sorted;
                System.arraycopy(other.starts, 0, starts, oldSize, other.size);
                System.arraycopy(other.ends, 0, ends, oldSize, other.size);
                size = combinedSize;
            }

            private void normalizeOverlapping() {
                if (size <= 1) {
                    sorted = true;
                    return;
                }
                if (!sorted) {
                    sort(0, size - 1);
                    sorted = true;
                }
                int writeIndex = 0;
                for (int readIndex = 1; readIndex < size; readIndex++) {
                    if (starts[readIndex] <= ends[writeIndex]) {
                        ends[writeIndex] = Math.max(ends[writeIndex], ends[readIndex]);
                    } else {
                        writeIndex++;
                        starts[writeIndex] = starts[readIndex];
                        ends[writeIndex] = ends[readIndex];
                    }
                }
                size = writeIndex + 1;
            }

            private OwnedPrimitiveRanges takeOwned() {
                long[] ownedStarts = starts.length == size ? starts : Arrays.copyOf(starts, size);
                long[] ownedEnds = ends.length == size ? ends : Arrays.copyOf(ends, size);
                starts = new long[0];
                ends = new long[0];
                size = 0;
                sorted = true;
                return new OwnedPrimitiveRanges(ownedStarts, ownedEnds);
            }

            private void ensureCapacity(int required) {
                if (required <= starts.length) {
                    return;
                }
                int grown = Math.max(16, starts.length);
                while (grown < required) {
                    int next = grown + (grown >>> 1);
                    if (next <= grown || next < 0) {
                        grown = required;
                        break;
                    }
                    grown = next;
                }
                starts = Arrays.copyOf(starts, grown);
                ends = Arrays.copyOf(ends, grown);
            }

            private void sort(int left, int right) {
                while (left < right) {
                    int middle = left + ((right - left) >>> 1);
                    long pivotStart = starts[middle];
                    long pivotEnd = ends[middle];
                    int lower = left;
                    int current = left;
                    int upper = right;
                    while (current <= upper) {
                        int comparison =
                                compare(starts[current], ends[current], pivotStart, pivotEnd);
                        if (comparison < 0) {
                            swap(lower++, current++);
                        } else if (comparison > 0) {
                            swap(current, upper--);
                        } else {
                            current++;
                        }
                    }

                    if (lower - left < right - upper) {
                        if (left < lower - 1) {
                            sort(left, lower - 1);
                        }
                        left = upper + 1;
                    } else {
                        if (upper + 1 < right) {
                            sort(upper + 1, right);
                        }
                        right = lower - 1;
                    }
                }
            }

            private void swap(int left, int right) {
                if (left == right) {
                    return;
                }
                long start = starts[left];
                long end = ends[left];
                starts[left] = starts[right];
                ends[left] = ends[right];
                starts[right] = start;
                ends[right] = end;
            }

            private void checkIndex(int index) {
                checkArgument(index >= 0 && index < size, "Logical range index is out of bounds.");
            }

            private static int compare(
                    long leftStart, long leftEnd, long rightStart, long rightEnd) {
                int result = Long.compare(leftStart, rightStart);
                return result == 0 ? Long.compare(leftEnd, rightEnd) : result;
            }
        }

        private static final class OwnedPrimitiveRanges {

            private final long[] starts;
            private final long[] ends;

            private OwnedPrimitiveRanges(long[] starts, long[] ends) {
                this.starts = starts;
                this.ends = ends;
            }
        }

        private static final class ByteArrayKey {

            private final byte[] bytes;
            private final int hash;

            private ByteArrayKey(byte[] bytes) {
                this.bytes = bytes;
                this.hash = Arrays.hashCode(bytes);
            }

            @Override
            public boolean equals(Object obj) {
                return obj == this
                        || (obj instanceof ByteArrayKey
                                && Arrays.equals(bytes, ((ByteArrayKey) obj).bytes))
                        || (obj instanceof ByteArrayLookupKey
                                && Arrays.equals(bytes, ((ByteArrayLookupKey) obj).bytes));
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

        private static final class ByteArrayLookupKey {

            private @Nullable byte[] bytes;
            private int hash;

            private ByteArrayLookupKey() {}

            private ByteArrayLookupKey(byte[] bytes) {
                reset(bytes);
            }

            private void reset(byte[] bytes) {
                this.bytes = bytes;
                this.hash = Arrays.hashCode(bytes);
            }

            private void clear() {
                bytes = null;
                hash = 0;
            }

            @Override
            public boolean equals(Object obj) {
                return obj == this
                        || (bytes != null
                                && obj instanceof ByteArrayKey
                                && Arrays.equals(bytes, ((ByteArrayKey) obj).bytes))
                        || (bytes != null
                                && obj instanceof ByteArrayLookupKey
                                && Arrays.equals(bytes, ((ByteArrayLookupKey) obj).bytes));
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }
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
