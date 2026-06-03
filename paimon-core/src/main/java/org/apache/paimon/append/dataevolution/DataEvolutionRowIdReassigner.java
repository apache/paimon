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
import java.util.Set;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Reassigns row IDs for data evolution tables by rewriting metadata only. */
public class DataEvolutionRowIdReassigner {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionRowIdReassigner.class);
    private static final String COMMIT_USER_PREFIX = "reassign-row-id";

    private final FileStoreTable table;
    private final @Nullable PartitionPredicate partitionPredicate;

    public DataEvolutionRowIdReassigner(FileStoreTable table) {
        this(table, null);
    }

    public DataEvolutionRowIdReassigner(
            FileStoreTable table, @Nullable PartitionPredicate partitionPredicate) {
        this.table = table;
        this.partitionPredicate = partitionPredicate;
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
        List<ManifestFileMeta> manifestMetas = manifestList.readDataManifests(latest);
        AssignmentPlan assignment = planAssignment(manifestMetas, manifestFile, nextRowId);
        if (!assignment.hasCurrentFiles) {
            return Result.skipped(
                    latest.id(),
                    nextRowId,
                    partitionFilterEnabled()
                            ? "partition filter matches no current files"
                            : "table has no current files");
        }
        if (assignment.reassignedFileCount == 0) {
            LOG.info(
                    "Skip reassigning row IDs for table {} because partition row IDs are already contiguous.",
                    table.name());
            return Result.skipped(
                    latest.id(), nextRowId, "partition row IDs are already contiguous");
        }

        Pair<String, Long> baseManifestList =
                writeBaseManifestList(
                        manifestMetas, assignment.rewrittenManifestMetas, manifestList);
        Pair<String, Long> deltaManifestList = manifestList.write(Collections.emptyList());

        RewrittenIndexManifest rewrittenIndexManifest = rewriteIndexManifest(latest, assignment);

        try (FileStoreCommitImpl commit =
                (FileStoreCommitImpl) table.store().newCommit(commitUser, table)) {
            boolean success =
                    commit.replaceManifestList(
                            latest,
                            latest.totalRecordCount(),
                            baseManifestList,
                            deltaManifestList,
                            rewrittenIndexManifest.indexManifest,
                            assignment.nextRowId);
            if (!success) {
                throw new RuntimeException(
                        "Failed to reassign row IDs because a newer snapshot has been committed.");
            }
        }

        LOG.info(
                "Reassigned row IDs for table {} from {} to {}, partitions={}, files={}, rows={}.",
                table.name(),
                nextRowId,
                assignment.nextRowId,
                assignment.rowIdMappings.size(),
                assignment.reassignedFileCount,
                assignment.logicalRowCount);
        return new Result(
                latest.id(),
                latest.id() + 1,
                assignment.reassignedFileCount,
                assignment.logicalRowCount,
                rewrittenIndexManifest.indexFileCount,
                nextRowId,
                assignment.nextRowId);
    }

    private AssignmentPlan planAssignment(
            List<ManifestFileMeta> manifestMetas, ManifestFile manifestFile, long firstRowId) {
        List<List<ManifestFileMeta>> manifestGroups = manifestGroupsByPartition(manifestMetas);
        Map<String, List<ManifestFileMeta>> rewrittenManifestMetas = new HashMap<>();
        Map<BinaryRow, RowRangeMappingIndex> rowIdMappings = new LinkedHashMap<>();
        long nextRowId = firstRowId;
        long logicalRowCount = 0;
        long reassignedFileCount = 0;
        boolean hasCurrentFiles = false;

        for (List<ManifestFileMeta> manifestGroup : manifestGroups) {
            if (skipManifestGroupByPartitionFilter(manifestGroup)) {
                continue;
            }

            CurrentManifest currentManifest = currentManifest(manifestGroup, manifestFile);
            List<ManifestEntry> currentEntries = currentManifest.entries();
            if (currentEntries.isEmpty()) {
                continue;
            }
            hasCurrentFiles = true;

            Map<BinaryRow, List<ManifestEntry>> entriesByPartition =
                    entriesByPartition(currentEntries);
            Set<BinaryRow> partitionsToReassign = partitionsToReassign(entriesByPartition);
            if (partitionsToReassign.isEmpty()) {
                continue;
            }

            Assignment groupAssignment =
                    assign(entriesByPartition, partitionsToReassign, nextRowId);
            nextRowId = groupAssignment.nextRowId;
            logicalRowCount += groupAssignment.logicalRowCount;
            reassignedFileCount += groupAssignment.reassignedFileCount;
            for (Map.Entry<BinaryRow, RowRangeMappingIndex> mapping :
                    groupAssignment.rowIdMappings.entrySet()) {
                RowRangeMappingIndex previous =
                        rowIdMappings.put(mapping.getKey(), mapping.getValue());
                checkState(
                        previous == null,
                        "Partition %s appears in multiple manifest groups.",
                        table.store().pathFactory().getPartitionString(mapping.getKey()));
            }

            Map<String, List<ManifestFileMeta>> groupRewrittenManifestMetas =
                    writeManifestReplacements(
                            currentManifest, groupAssignment, partitionsToReassign, manifestFile);
            for (Map.Entry<String, List<ManifestFileMeta>> rewritten :
                    groupRewrittenManifestMetas.entrySet()) {
                List<ManifestFileMeta> previous =
                        rewrittenManifestMetas.put(rewritten.getKey(), rewritten.getValue());
                checkState(
                        previous == null,
                        "Manifest file %s appears in multiple manifest groups.",
                        rewritten.getKey());
            }
        }

        return new AssignmentPlan(
                rewrittenManifestMetas,
                rowIdMappings,
                nextRowId,
                logicalRowCount,
                reassignedFileCount,
                hasCurrentFiles);
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

    private CurrentManifest currentManifest(
            List<ManifestFileMeta> manifestMetas, ManifestFile manifestFile) {
        Set<FileEntry.Identifier> deletedIdentifiers =
                deletedIdentifiers(manifestFile, manifestMetas);

        List<SourcedManifestEntry> currentEntries = new ArrayList<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            if (manifestMeta.numAddedFiles() <= 0) {
                continue;
            }
            List<ManifestEntry> entries =
                    manifestFile.read(manifestMeta.fileName(), manifestMeta.fileSize());
            for (ManifestEntry entry : entries) {
                if (entry.kind() == FileKind.ADD
                        && partitionIncluded(entry.partition())
                        && !deletedIdentifiers.contains(entry.identifier())) {
                    currentEntries.add(new SourcedManifestEntry(manifestMeta, entry));
                }
            }
        }
        return new CurrentManifest(manifestMetas, currentEntries);
    }

    private Set<FileEntry.Identifier> deletedIdentifiers(
            ManifestFile manifestFile, List<ManifestFileMeta> manifestMetas) {
        Set<FileEntry.Identifier> deletedIdentifiers = new HashSet<>();
        for (ManifestFileMeta manifestMeta : manifestMetas) {
            if (manifestMeta.numDeletedFiles() <= 0) {
                continue;
            }
            List<ManifestEntry> entries =
                    manifestFile.read(manifestMeta.fileName(), manifestMeta.fileSize());
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
            CurrentManifest currentManifest,
            Assignment assignment,
            Set<BinaryRow> partitionsToReassign,
            ManifestFile manifestFile) {
        Set<String> manifestsToRewrite =
                manifestsToRewrite(currentManifest.currentEntries, partitionsToReassign);
        Map<FileEntry.Identifier, ManifestEntry> reassignedEntries =
                entriesByIdentifier(assignment.entries);

        Map<String, List<ManifestFileMeta>> rewrittenManifestMetas = new HashMap<>();
        for (ManifestFileMeta manifestMeta : currentManifest.manifestMetas) {
            if (!manifestsToRewrite.contains(manifestMeta.fileName())) {
                continue;
            }

            List<ManifestEntry> rewrittenEntries = new ArrayList<>();
            List<ManifestEntry> entries =
                    manifestFile.read(manifestMeta.fileName(), manifestMeta.fileSize());
            for (ManifestEntry entry : entries) {
                if (entry.kind() == FileKind.ADD) {
                    ManifestEntry reassignedEntry = reassignedEntries.get(entry.identifier());
                    if (reassignedEntry != null) {
                        entry = reassignedEntry;
                    }
                }
                rewrittenEntries.add(entry);
            }
            rewrittenManifestMetas.put(
                    manifestMeta.fileName(), manifestFile.write(rewrittenEntries));
        }
        return rewrittenManifestMetas;
    }

    private Set<String> manifestsToRewrite(
            List<SourcedManifestEntry> currentEntries, Set<BinaryRow> partitionsToReassign) {
        Set<String> manifestsToRewrite = new HashSet<>();
        for (SourcedManifestEntry currentEntry : currentEntries) {
            if (partitionsToReassign.contains(currentEntry.entry.partition())) {
                manifestsToRewrite.add(currentEntry.manifest.fileName());
            }
        }
        return manifestsToRewrite;
    }

    private Map<FileEntry.Identifier, ManifestEntry> entriesByIdentifier(
            List<ManifestEntry> entries) {
        Map<FileEntry.Identifier, ManifestEntry> result = new HashMap<>();
        for (ManifestEntry entry : entries) {
            ManifestEntry previous = result.put(entry.identifier(), entry);
            checkState(previous == null, "Duplicate current manifest entry for file %s.", entry);
        }
        return result;
    }

    private Map<BinaryRow, List<ManifestEntry>> entriesByPartition(List<ManifestEntry> entries) {
        List<ManifestEntry> sorted = new ArrayList<>(entries);
        Collections.sort(sorted, entryComparator());

        Map<BinaryRow, List<ManifestEntry>> entriesByPartition = new LinkedHashMap<>();
        for (ManifestEntry entry : sorted) {
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
            entriesByPartition
                    .computeIfAbsent(entry.partition(), k -> new ArrayList<>())
                    .add(entry);
        }
        return entriesByPartition;
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

    private Assignment assign(
            Map<BinaryRow, List<ManifestEntry>> entriesByPartition,
            Set<BinaryRow> partitionsToReassign,
            long firstRowId) {
        List<ManifestEntry> entries = new ArrayList<>();
        Map<BinaryRow, RowRangeMappingIndex> rowIdMappings = new LinkedHashMap<>();
        long nextRowId = firstRowId;
        long logicalRowCount = 0;
        long reassignedFileCount = 0;
        for (Map.Entry<BinaryRow, List<ManifestEntry>> entry : entriesByPartition.entrySet()) {
            if (partitionsToReassign.contains(entry.getKey())) {
                long partitionFirstRowId = nextRowId;
                PartitionAssignment partitionAssignment =
                        assignPartition(entry.getValue(), nextRowId);
                entries.addAll(partitionAssignment.entries);
                rowIdMappings.put(entry.getKey(), partitionAssignment.rowIdMappings);
                nextRowId = partitionAssignment.nextRowId;
                logicalRowCount += nextRowId - partitionFirstRowId;
                reassignedFileCount += partitionAssignment.entries.size();
            }
        }

        return new Assignment(
                entries, rowIdMappings, nextRowId, logicalRowCount, reassignedFileCount);
    }

    private PartitionAssignment assignPartition(List<ManifestEntry> entries, long firstRowId) {
        RangeHelper<ManifestEntry> rangeHelper =
                new RangeHelper<>(entry -> entry.file().nonNullRowIdRange());
        List<List<ManifestEntry>> groups = rangeHelper.mergeOverlappingRanges(entries);

        List<ManifestEntry> reassigned = new ArrayList<>(entries.size());
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
                reassigned.add(entry.assignFirstRowId(newFirstRowId));
            }

            nextRowId += oldLogicalRange.count();
        }

        return new PartitionAssignment(
                reassigned, RowRangeMappingIndex.create(mappings), nextRowId);
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
            Snapshot latest, AssignmentPlan assignment) {
        if (latest.indexManifest() == null) {
            return new RewrittenIndexManifest(null, 0);
        }

        IndexManifestFile indexManifestFile = table.store().indexManifestFileFactory().create();
        List<IndexManifestEntry> indexEntries = indexManifestFile.read(latest.indexManifest());
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

            globalIndexFileCount++;
            Range newRange = mappingIndex.map(globalIndex.rowRange());
            GlobalIndexMeta newGlobalIndex =
                    new GlobalIndexMeta(
                            newRange.from,
                            newRange.to,
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

    private static class Assignment {
        private final List<ManifestEntry> entries;
        private final Map<BinaryRow, RowRangeMappingIndex> rowIdMappings;
        private final long nextRowId;
        private final long logicalRowCount;
        private final long reassignedFileCount;

        private Assignment(
                List<ManifestEntry> entries,
                Map<BinaryRow, RowRangeMappingIndex> rowIdMappings,
                long nextRowId,
                long logicalRowCount,
                long reassignedFileCount) {
            this.entries = entries;
            this.rowIdMappings = rowIdMappings;
            this.nextRowId = nextRowId;
            this.logicalRowCount = logicalRowCount;
            this.reassignedFileCount = reassignedFileCount;
        }
    }

    private static class AssignmentPlan {
        private final Map<String, List<ManifestFileMeta>> rewrittenManifestMetas;
        private final Map<BinaryRow, RowRangeMappingIndex> rowIdMappings;
        private final long nextRowId;
        private final long logicalRowCount;
        private final long reassignedFileCount;
        private final boolean hasCurrentFiles;

        private AssignmentPlan(
                Map<String, List<ManifestFileMeta>> rewrittenManifestMetas,
                Map<BinaryRow, RowRangeMappingIndex> rowIdMappings,
                long nextRowId,
                long logicalRowCount,
                long reassignedFileCount,
                boolean hasCurrentFiles) {
            this.rewrittenManifestMetas = rewrittenManifestMetas;
            this.rowIdMappings = rowIdMappings;
            this.nextRowId = nextRowId;
            this.logicalRowCount = logicalRowCount;
            this.reassignedFileCount = reassignedFileCount;
            this.hasCurrentFiles = hasCurrentFiles;
        }
    }

    private static class CurrentManifest {
        private final List<ManifestFileMeta> manifestMetas;
        private final List<SourcedManifestEntry> currentEntries;

        private CurrentManifest(
                List<ManifestFileMeta> manifestMetas, List<SourcedManifestEntry> currentEntries) {
            this.manifestMetas = manifestMetas;
            this.currentEntries = currentEntries;
        }

        private List<ManifestEntry> entries() {
            List<ManifestEntry> entries = new ArrayList<>(currentEntries.size());
            for (SourcedManifestEntry currentEntry : currentEntries) {
                entries.add(currentEntry.entry);
            }
            return entries;
        }
    }

    private static class SourcedManifestEntry {
        private final ManifestFileMeta manifest;
        private final ManifestEntry entry;

        private SourcedManifestEntry(ManifestFileMeta manifest, ManifestEntry entry) {
            this.manifest = manifest;
            this.entry = entry;
        }
    }

    private static class PartitionManifestRange {
        private final ManifestFileMeta manifest;
        private final BinaryRow minPartition;
        private final BinaryRow maxPartition;
        private final int originalIndex;

        private PartitionManifestRange(
                ManifestFileMeta manifest,
                BinaryRow minPartition,
                BinaryRow maxPartition,
                int originalIndex) {
            this.manifest = manifest;
            this.minPartition = minPartition;
            this.maxPartition = maxPartition;
            this.originalIndex = originalIndex;
        }
    }

    private static class PartitionAssignment {
        private final List<ManifestEntry> entries;
        private final RowRangeMappingIndex rowIdMappings;
        private final long nextRowId;

        private PartitionAssignment(
                List<ManifestEntry> entries, RowRangeMappingIndex rowIdMappings, long nextRowId) {
            this.entries = entries;
            this.rowIdMappings = rowIdMappings;
            this.nextRowId = nextRowId;
        }
    }
}
