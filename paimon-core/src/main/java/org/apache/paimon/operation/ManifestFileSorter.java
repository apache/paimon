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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.ManifestFileMerger.FullCompactionReadResult;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;

/** Manifest file sorter that sorts and rewrites manifest files by a configured partition field. */
public class ManifestFileSorter {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFileSorter.class);

    /**
     * Try to sort-rewrite the merged manifest list by a configured partition field. If the sort
     * field cannot be resolved or the delta file size is below the full compaction threshold, the
     * input is returned as-is.
     */
    static Optional<List<ManifestFileMeta>> trySortRewrite(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            RowType partitionType,
            CoreOptions options)
            throws Exception {
        // Extract configuration from options
        long suggestedMetaSize = options.manifestTargetSize().getBytes();
        long manifestFullCompactionSize = options.manifestFullCompactionThresholdSize().getBytes();
        Integer manifestReadParallelism = options.scanManifestParallelism();
        String sortPartitionField = options.manifestSortPartitionField();
        int mergeMinCount = options.manifestMergeMinCount();
        // Step 1: Resolve sort field.
        String sortField = resolveSortField(sortPartitionField, partitionType);
        if (sortField == null) {
            throw new IllegalArgumentException(
                    "Cannot resolve sort field for manifest sort rewrite.");
        }
        int sortFieldIndex = partitionType.getFieldNames().indexOf(sortField);
        DataType sortFieldType = partitionType.getTypeAt(sortFieldIndex);

        // Step 2: Classify manifests into defaultCompaction and LSM groups.
        ClassifyResult classified =
                classifyManifests(
                        input,
                        suggestedMetaSize,
                        manifestFullCompactionSize,
                        mergeMinCount,
                        manifestFile,
                        partitionType,
                        manifestReadParallelism);
        List<ManifestFileMeta> defaultCompactionManifests = classified.defaultCompactionManifests;
        List<ManifestFileMeta> lsmFiles = classified.lsmFiles;
        Set<FileEntry.Identifier> deleteEntries = classified.deleteEntries;

        // Step 3: Build LSM Tree and assign levels (only for lsmFiles).
        List<ManifestSortedRun> levelRuns =
                lsmFiles.isEmpty()
                        ? new ArrayList<>()
                        : buildLevelSortedRuns(lsmFiles, sortFieldIndex, sortFieldType);

        // Step 4: Pick runs to compact.
        int sizeAmpThreshold = options.maxSizeAmplificationPercent();
        int sizeRatioThreshold = options.sortedRunSizeRatio();
        ManifestPickStrategy pickStrategy =
                new ManifestPickStrategy(sizeAmpThreshold, sizeRatioThreshold);
        List<ManifestSortedRun> pickedRuns = pickStrategy.pick(levelRuns);

        if (pickedRuns.isEmpty() && defaultCompactionManifests.isEmpty()) {
            LOG.debug(
                    "Manifest sort rewrite skipped: no runs picked and no defaultCompaction files.");
            return Optional.of(input);
        }

        LOG.info(
                "Manifest sort rewrite: input={} files, lsm={} runs, picked={} runs, "
                        + "defaultCompaction={} files.",
                input.size(),
                levelRuns.size(),
                pickedRuns.size(),
                defaultCompactionManifests.size());

        Set<ManifestSortedRun> pickedSet = new HashSet<>(pickedRuns);
        List<ManifestFileMeta> reusedFiles = new ArrayList<>();
        for (ManifestSortedRun run : levelRuns) {
            if (!pickedSet.contains(run)) {
                reusedFiles.addAll(run.files());
            }
        }
        List<ManifestFileMeta> result = new ArrayList<>(reusedFiles);

        // Step 5: Split picked files into sections, sort and rewrite each.
        List<ManifestFileMeta> pickedFiles = new ArrayList<>();
        for (ManifestSortedRun run : pickedRuns) {
            pickedFiles.addAll(run.files());
        }
        pickedFiles.addAll(defaultCompactionManifests);

        Set<ManifestFileMeta> defaultCompactionSet = new HashSet<>(defaultCompactionManifests);

        List<Section> sections =
                splitIntoSections(pickedFiles, sortFieldIndex, sortFieldType, defaultCompactionSet);
        sections = mergeSmallAdjacentSections(sections, suggestedMetaSize);
        long maxRewriteSize = options.manifestSortMaxRewriteSize();
        long openFileCost = options.manifestSortOpenFileCost();
        List<ManifestFileMeta> sortNewFiles = new ArrayList<>();

        List<ManifestFileMeta> rewritten =
                rewriteSections(
                        sections,
                        defaultCompactionSet,
                        manifestFile,
                        sortFieldIndex,
                        sortFieldType,
                        deleteEntries,
                        suggestedMetaSize,
                        maxRewriteSize,
                        openFileCost,
                        sortNewFiles,
                        manifestReadParallelism);
        result.addAll(rewritten);

        newFilesForAbort.addAll(sortNewFiles);
        LOG.info(
                "Manifest sort rewrite completed: sections={}, newFiles={}, resultFiles={}.",
                sections.size(),
                sortNewFiles.size(),
                result.size());
        return Optional.of(result);
    }

    /**
     * Classify manifest files into default-compaction group and LSM group.
     *
     * <p>When full compaction is triggered (totalDeltaFileSize >= threshold), files that must
     * change or overlap with delete partitions go into defaultCompactionManifests; the rest stay as
     * lsmFiles.
     *
     * <p>When full compaction is NOT triggered, adjacent small manifests whose cumulative size
     * reaches suggestedMetaSize are grouped into defaultCompactionManifests (minor-style pick).
     */
    private static ClassifyResult classifyManifests(
            List<ManifestFileMeta> input,
            long suggestedMetaSize,
            long manifestFullCompactionSize,
            int mergeMinCount,
            ManifestFile manifestFile,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism) {
        Filter<ManifestFileMeta> mustChange =
                file -> file.numDeletedFiles() > 0 || file.fileSize() < suggestedMetaSize;

        long totalDeltaFileSize = 0;
        for (ManifestFileMeta file : input) {
            if (mustChange.test(file)) {
                totalDeltaFileSize += file.fileSize();
            }
        }

        List<ManifestFileMeta> defaultCompactionManifests = new ArrayList<>();
        List<ManifestFileMeta> lsmFiles = new LinkedList<>(input);
        Set<FileEntry.Identifier> deleteEntries = null;

        if (totalDeltaFileSize >= manifestFullCompactionSize) {
            // Full compact triggered: read delete entries and classify by predicate.
            deleteEntries =
                    FileEntry.readDeletedEntries(manifestFile, input, manifestReadParallelism);

            PartitionPredicate predicate;
            if (deleteEntries.isEmpty()) {
                predicate = PartitionPredicate.ALWAYS_FALSE;
            } else {
                if (partitionType.getFieldCount() > 0) {
                    Set<BinaryRow> deletePartitions =
                            ManifestFileMerger.computeDeletePartitions(deleteEntries);
                    predicate = PartitionPredicate.fromMultiple(partitionType, deletePartitions);
                } else {
                    predicate = PartitionPredicate.ALWAYS_TRUE;
                }
            }

            Iterator<ManifestFileMeta> iterator = lsmFiles.iterator();
            while (iterator.hasNext()) {
                ManifestFileMeta file = iterator.next();
                if (mustChange.test(file)) {
                    iterator.remove();
                    defaultCompactionManifests.add(file);
                } else if (predicate != null
                        && predicate.test(
                                file.numAddedFiles() + file.numDeletedFiles(),
                                file.partitionStats().minValues(),
                                file.partitionStats().maxValues(),
                                file.partitionStats().nullCounts())) {
                    iterator.remove();
                    defaultCompactionManifests.add(file);
                }
            }
        } else {
            // Minor-style pick: merge adjacent small manifests when no full compact triggered.
            Set<ManifestFileMeta> toRemove = new HashSet<>();
            List<ManifestFileMeta> candidates = new ArrayList<>();
            long candidateSize = 0;
            for (ManifestFileMeta file : input) {
                candidateSize += file.fileSize();
                candidates.add(file);
                if (candidateSize >= suggestedMetaSize) {
                    if (candidates.size() > 1) {
                        defaultCompactionManifests.addAll(candidates);
                        toRemove.addAll(candidates);
                    }
                    candidates.clear();
                    candidateSize = 0;
                }
            }
            if (candidates.size() >= mergeMinCount) {
                defaultCompactionManifests.addAll(candidates);
                toRemove.addAll(candidates);
            }
            if (!toRemove.isEmpty()) {
                lsmFiles.removeIf(toRemove::contains);
            }
        }

        return new ClassifyResult(defaultCompactionManifests, lsmFiles, deleteEntries);
    }

    /**
     * Iterate over sections, decide whether to rewrite each section fully or partially based on the
     * maxRewriteSize threshold and whether the section contains defaultCompaction files.
     *
     * <p>Within threshold: read all metas, sort and rewrite the entire section. Exceeds threshold
     * but contains defaultCompaction files: only rewrite sub-segments around those files. Exceeds
     * threshold with no defaultCompaction files: skip (keep as-is).
     *
     * @return the list of result manifest files (both rewritten and kept-as-is)
     */
    private static List<ManifestFileMeta> rewriteSections(
            List<Section> sections,
            Set<ManifestFileMeta> defaultCompactionSet,
            ManifestFile manifestFile,
            int sortFieldIndex,
            DataType sortFieldType,
            @Nullable Set<FileEntry.Identifier> deleteEntries,
            long suggestedMetaSize,
            long maxRewriteSize,
            long openFileCost,
            List<ManifestFileMeta> sortNewFiles,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        List<ManifestFileMeta> result = new ArrayList<>();
        long processedSize = 0;

        for (Section section : sections) {
            // Single-file section without defaultCompaction: already sorted, skip rewrite.
            if (section.files.size() == 1 && !section.hasDefaultCompactMeta) {
                result.addAll(section.files);
                continue;
            }

            long sectionSize = section.totalSize + (long) section.files.size() * openFileCost;

            boolean exceedsThreshold = processedSize + sectionSize > maxRewriteSize;
            if (exceedsThreshold && !section.hasDefaultCompactMeta) {
                result.addAll(section.files);
                continue;
            }

            if (!exceedsThreshold) {
                processedSize += sectionSize;
                List<ManifestFileMeta> merged =
                        sortAndRewriteSection(
                                section.files,
                                manifestFile,
                                sortFieldIndex,
                                sortFieldType,
                                deleteEntries,
                                manifestReadParallelism);
                sortNewFiles.addAll(merged);
                result.addAll(merged);
            } else {
                rewriteSubSegments(
                        section.files,
                        defaultCompactionSet,
                        manifestFile,
                        sortFieldIndex,
                        sortFieldType,
                        deleteEntries,
                        suggestedMetaSize,
                        sortNewFiles,
                        result,
                        manifestReadParallelism);
            }
        }
        return result;
    }

    /**
     * Rewrite sub-segments within a section that exceeds the rewrite threshold. Only sub-segments
     * containing defaultCompaction files are rewritten; other files are kept as-is.
     */
    private static void rewriteSubSegments(
            List<ManifestFileMeta> section,
            Set<ManifestFileMeta> defaultCompactionSet,
            ManifestFile manifestFile,
            int sortFieldIndex,
            DataType sortFieldType,
            @Nullable Set<FileEntry.Identifier> deleteEntries,
            long suggestedMetaSize,
            List<ManifestFileMeta> sortNewFiles,
            List<ManifestFileMeta> result,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        List<ManifestFileMeta> subSegment = new ArrayList<>();
        long subSegmentSize = 0;
        for (ManifestFileMeta m : section) {
            if (defaultCompactionSet.contains(m)) {
                subSegment.add(m);
                subSegmentSize += m.fileSize();
            } else if (!subSegment.isEmpty()) {
                subSegment.add(m);
                subSegmentSize += m.fileSize();
                if (subSegmentSize >= suggestedMetaSize) {
                    List<ManifestFileMeta> merged =
                            sortAndRewriteSection(
                                    subSegment,
                                    manifestFile,
                                    sortFieldIndex,
                                    sortFieldType,
                                    deleteEntries,
                                    manifestReadParallelism);
                    sortNewFiles.addAll(merged);
                    result.addAll(merged);
                    subSegment = new ArrayList<>();
                    subSegmentSize = 0;
                }
            } else {
                result.add(m);
            }
        }
        // Flush remaining sub-segment
        if (!subSegment.isEmpty()) {
            List<ManifestFileMeta> merged =
                    sortAndRewriteSection(
                            subSegment,
                            manifestFile,
                            sortFieldIndex,
                            sortFieldType,
                            deleteEntries,
                            manifestReadParallelism);
            sortNewFiles.addAll(merged);
            result.addAll(merged);
        }
    }

    /**
     * Build level-sorted runs from a list of manifest files. Sorts files by min partition value,
     * greedy-scans to build non-overlapping SortedRuns, then assigns levels by totalSize (Top-4
     * largest to level 1~4, rest to level 0).
     */
    static List<ManifestSortedRun> buildLevelSortedRuns(
            List<ManifestFileMeta> input, int sortFieldIndex, DataType sortFieldType) {
        input.sort(
                (a, b) -> {
                    int cmp =
                            compareField(
                                    a.partitionStats().minValues(),
                                    b.partitionStats().minValues(),
                                    sortFieldIndex,
                                    sortFieldType);
                    if (cmp != 0) {
                        return cmp;
                    }
                    return compareField(
                            a.partitionStats().maxValues(),
                            b.partitionStats().maxValues(),
                            sortFieldIndex,
                            sortFieldType);
                });

        List<List<ManifestFileMeta>> runFilesList = new ArrayList<>();
        List<ManifestFileMeta> currentRun = new ArrayList<>();
        currentRun.add(input.get(0));
        for (int i = 1; i < input.size(); i++) {
            ManifestFileMeta file = input.get(i);
            ManifestFileMeta last = currentRun.get(currentRun.size() - 1);
            if (compareField(
                            file.partitionStats().minValues(),
                            last.partitionStats().maxValues(),
                            sortFieldIndex,
                            sortFieldType)
                    >= 0) {
                currentRun.add(file);
            } else {
                runFilesList.add(currentRun);
                currentRun = new ArrayList<>();
                currentRun.add(file);
            }
        }
        runFilesList.add(currentRun);

        List<ManifestSortedRun> runs = new ArrayList<>(runFilesList.size());
        for (List<ManifestFileMeta> rf : runFilesList) {
            runs.add(ManifestSortedRun.fromSorted(rf));
        }

        runs.sort(Comparator.comparingLong(ManifestSortedRun::totalSize));
        int n = runs.size();
        for (int i = 0; i < n; i++) {
            if (i >= n - 4) {
                // top-4 largest runs get level 4-1
                runs.get(i).setLevel(i - (n - 4) + 1);
            } else {
                runs.get(i).setLevel(0);
            }
        }
        return runs;
    }

    /**
     * Split picked files into sections. Files with overlapping sort-key intervals go into the same
     * section. Each section is built with pre-computed totalSize and hasDefaultCompactMeta.
     */
    static List<Section> splitIntoSections(
            List<ManifestFileMeta> pickedFiles,
            int sortFieldIndex,
            DataType sortFieldType,
            Set<ManifestFileMeta> defaultCompactionSet) {
        pickedFiles.sort(
                (a, b) -> {
                    int cmp =
                            compareField(
                                    a.partitionStats().minValues(),
                                    b.partitionStats().minValues(),
                                    sortFieldIndex,
                                    sortFieldType);
                    if (cmp != 0) {
                        return cmp;
                    }
                    return compareField(
                            a.partitionStats().maxValues(),
                            b.partitionStats().maxValues(),
                            sortFieldIndex,
                            sortFieldType);
                });

        List<Section> sections = new ArrayList<>();
        List<ManifestFileMeta> currentFiles = new ArrayList<>();
        long currentTotalSize = 0;
        boolean currentHasDefault = false;
        ManifestFileMeta first = pickedFiles.get(0);
        currentFiles.add(first);
        currentTotalSize += first.fileSize();
        currentHasDefault = defaultCompactionSet.contains(first);
        BinaryRow sectionMaxBound = first.partitionStats().maxValues();

        for (int i = 1; i < pickedFiles.size(); i++) {
            ManifestFileMeta file = pickedFiles.get(i);
            if (compareField(
                            file.partitionStats().minValues(),
                            sectionMaxBound,
                            sortFieldIndex,
                            sortFieldType)
                    >= 0) {
                sections.add(new Section(currentFiles, currentTotalSize, currentHasDefault));
                currentFiles = new ArrayList<>();
                currentTotalSize = 0;
                currentFiles.add(file);
                currentTotalSize += file.fileSize();
                currentHasDefault = defaultCompactionSet.contains(file);
                sectionMaxBound = file.partitionStats().maxValues();
            } else {
                currentFiles.add(file);
                currentTotalSize += file.fileSize();
                if (!currentHasDefault && defaultCompactionSet.contains(file)) {
                    currentHasDefault = true;
                }
                if (compareField(
                                file.partitionStats().maxValues(),
                                sectionMaxBound,
                                sortFieldIndex,
                                sortFieldType)
                        > 0) {
                    sectionMaxBound = file.partitionStats().maxValues();
                }
            }
        }
        sections.add(new Section(currentFiles, currentTotalSize, currentHasDefault));
        return sections;
    }

    /**
     * Merge small adjacent sections to avoid producing too many small rewrite batches. If either
     * the pending section or the current section total size is smaller than half of {@code
     * suggestedMetaSize}, they are combined into a single section.
     */
    private static List<Section> mergeSmallAdjacentSections(
            List<Section> sections, long suggestedMetaSize) {
        long smallThreshold = suggestedMetaSize / 2;
        List<Section> merged = new ArrayList<>();
        Section pending = null;

        for (Section section : sections) {
            if (pending == null) {
                pending = section;
            } else {
                if (pending.totalSize < smallThreshold || section.totalSize < smallThreshold) {
                    pending = Section.merge(pending, section);
                } else {
                    merged.add(pending);
                    pending = section;
                }
            }
        }
        if (pending != null) {
            merged.add(pending);
        }
        return merged;
    }

    /**
     * Read all entries from a section's manifest files, sort them in memory by the specified
     * partition field, filter out DELETE entries and cancelled ADD entries, then write surviving
     * entries to new manifest files via the rolling writer.
     *
     * <p>All files participate in sorting, enabling full sort across the entire section.
     *
     * <p>Reading is parallelized via {@code sequentialBatchedExecute} following the same pattern as
     * {@link ManifestFileMerger#tryFullCompaction}.
     */
    private static List<ManifestFileMeta> sortAndRewriteSection(
            List<ManifestFileMeta> section,
            ManifestFile manifestFile,
            int sortFieldIndex,
            DataType sortFieldType,
            @Nullable Set<FileEntry.Identifier> deletedIdentifiers,
            @Nullable Integer manifestReadParallelism)
            throws Exception {

        Set<FileEntry.Identifier> safeDeletedIds =
                deletedIdentifiers != null ? deletedIdentifiers : new HashSet<>();

        // Parallel read: each meta is read independently
        Function<ManifestFileMeta, List<FullCompactionReadResult>> reader =
                meta -> singletonList(readForSortRewrite(meta, manifestFile, safeDeletedIds));

        List<ManifestEntry> entriesToRewrite = new ArrayList<>();
        for (FullCompactionReadResult readResult :
                sequentialBatchedExecute(reader, section, manifestReadParallelism)) {
            entriesToRewrite.addAll(readResult.entries);
        }

        List<ManifestFileMeta> result = new ArrayList<>();
        if (!entriesToRewrite.isEmpty()) {
            entriesToRewrite.sort((a, b) -> compareSortKey(a, b, sortFieldIndex, sortFieldType));

            // When non-full-compact (deletedIdentifiers is null, meaning delete entries
            // were not read), entries may contain both ADD and DELETE. Merge them following
            // FileEntry.mergeEntries logic to cancel paired ADD/DELETE and keep unresolved
            // DELETE entries whose ADD is in a previous manifest file.
            if (deletedIdentifiers == null) {
                LinkedHashMap<FileEntry.Identifier, ManifestEntry> mergedMap =
                        new LinkedHashMap<>();
                FileEntry.mergeEntries(entriesToRewrite, mergedMap);
                entriesToRewrite = new ArrayList<>(mergedMap.values());
            }

            RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                    manifestFile.createRollingWriter();
            Exception exception = null;
            try {
                for (ManifestEntry entry : entriesToRewrite) {
                    writer.write(entry);
                }
            } catch (Exception e) {
                exception = e;
            } finally {
                if (exception != null) {
                    writer.abort();
                    throw exception;
                }
                writer.close();
            }
            result.addAll(writer.result());
        }

        return result;
    }

    /**
     * Compares the value at field {@code k} of two {@link BinaryRow}s according to {@code type}.
     */
    static int compareField(BinaryRow a, BinaryRow b, int k, DataType type) {
        switch (type.getTypeRoot()) {
            case INTEGER:
            case DATE:
                return Integer.compare(a.getInt(k), b.getInt(k));
            case BIGINT:
                return Long.compare(a.getLong(k), b.getLong(k));
            case SMALLINT:
                return Short.compare(a.getShort(k), b.getShort(k));
            case TINYINT:
                return Byte.compare(a.getByte(k), b.getByte(k));
            case FLOAT:
                return Float.compare(a.getFloat(k), b.getFloat(k));
            case DOUBLE:
                return Double.compare(a.getDouble(k), b.getDouble(k));
            case BOOLEAN:
                return Boolean.compare(a.getBoolean(k), b.getBoolean(k));
            case VARCHAR:
            case CHAR:
                return a.getString(k).compareTo(b.getString(k));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return a.getTimestamp(k, type.defaultSize())
                        .compareTo(b.getTimestamp(k, type.defaultSize()));
            case DECIMAL:
                DecimalType dt = (DecimalType) type;
                return a.getDecimal(k, dt.getPrecision(), dt.getScale())
                        .compareTo(b.getDecimal(k, dt.getPrecision(), dt.getScale()));
            default:
                String errorMsg =
                        String.format(
                                "Unsupported partition field type '%s' for manifest sort rewrite. "
                                        + "Supported types: TINYINT, SMALLINT, INTEGER, BIGINT, "
                                        + "FLOAT, DOUBLE, BOOLEAN, CHAR, VARCHAR, DATE, TIMESTAMP, "
                                        + "DECIMAL.",
                                type.getTypeRoot());
                LOG.error(errorMsg);
                throw new UnsupportedOperationException(errorMsg);
        }
    }

    /**
     * Compare two {@link ManifestEntry}s by the composite key {@code (sort-field, fileName)}.
     * {@code fileName} is used as the tie-breaker so that all entries sharing the same sort-field
     * value AND the same data file are emitted contiguously.
     */
    static int compareSortKey(
            ManifestEntry a, ManifestEntry b, int sortFieldIndex, DataType sortFieldType) {
        int c = compareField(a.partition(), b.partition(), sortFieldIndex, sortFieldType);
        if (c != 0) {
            return c;
        }
        // ADD before DELETE, so that mergeEntries can correctly cancel pairs
        int kindCmp = a.kind().compareTo(b.kind());
        if (kindCmp != 0) {
            return kindCmp;
        }
        return a.file().fileName().compareTo(b.file().fileName());
    }

    /**
     * Resolve the partition field to sort manifests by.
     *
     * <p>Resolution rules:
     *
     * <ol>
     *   <li>If {@code manifest-sort.partition-field} is configured, return that value.
     *   <li>Otherwise, default to the first partition field.
     * </ol>
     */
    static String resolveSortField(String sortPartitionField, RowType partitionType) {
        if (sortPartitionField != null && !sortPartitionField.isEmpty()) {
            return sortPartitionField;
        }
        return partitionType.getFieldNames().get(0);
    }

    /**
     * Read a single manifest file for sort rewrite.
     *
     * <p>When {@code deletedIdentifiers} is non-empty (full compaction path), only surviving ADD
     * entries (not cancelled by deletedIdentifiers) are kept, and DELETE entries are dropped
     * because the full compaction has already resolved them.
     *
     * <p>When {@code deletedIdentifiers} is empty (non-full-compaction path), all entries (both ADD
     * and DELETE) are preserved to avoid losing unresolved DELETE entries.
     */
    private static FullCompactionReadResult readForSortRewrite(
            ManifestFileMeta meta,
            ManifestFile manifestFile,
            Set<FileEntry.Identifier> deletedIdentifiers) {
        List<ManifestEntry> entries = new ArrayList<>();
        if (deletedIdentifiers.isEmpty()) {
            entries.addAll(manifestFile.read(meta.fileName(), meta.fileSize()));
        } else {
            for (ManifestEntry entry : manifestFile.read(meta.fileName(), meta.fileSize())) {
                if (!deletedIdentifiers.contains(entry.identifier())) {
                    entries.add(entry);
                }
            }
        }
        return new FullCompactionReadResult(meta, true, entries);
    }

    /** A section of manifest files with pre-computed metadata. */
    static class Section {
        final List<ManifestFileMeta> files;
        final long totalSize;
        final boolean hasDefaultCompactMeta;

        Section(List<ManifestFileMeta> files, long totalSize, boolean hasDefaultCompactMeta) {
            this.files = files;
            this.totalSize = totalSize;
            this.hasDefaultCompactMeta = hasDefaultCompactMeta;
        }

        /** Create a merged section from two sections. */
        static Section merge(Section a, Section b) {
            List<ManifestFileMeta> merged = new ArrayList<>(a.files);
            merged.addAll(b.files);
            return new Section(
                    merged,
                    a.totalSize + b.totalSize,
                    a.hasDefaultCompactMeta || b.hasDefaultCompactMeta);
        }
    }

    /** Result of classifying manifest files into default-compaction and LSM groups. */
    private static class ClassifyResult {
        final List<ManifestFileMeta> defaultCompactionManifests;
        final List<ManifestFileMeta> lsmFiles;
        @Nullable final Set<FileEntry.Identifier> deleteEntries;

        ClassifyResult(
                List<ManifestFileMeta> defaultCompactionManifests,
                List<ManifestFileMeta> lsmFiles,
                @Nullable Set<FileEntry.Identifier> deleteEntries) {
            this.defaultCompactionManifests = defaultCompactionManifests;
            this.lsmFiles = lsmFiles;
            this.deleteEntries = deleteEntries;
        }
    }
}
