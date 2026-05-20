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
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.ManifestFileMerger.FullCompactionReadResult;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;

/** Manifest file sorter that sorts and rewrites manifest files by a configured partition field. */
public class ManifestFileSorter {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFileSorter.class);

    /**
     * Try to sort-rewrite the merged manifest list by a configured partition field. If the sort
     * field cannot be resolved, the input is returned as-is.
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
        Integer manifestReadParallelism = options.scanManifestParallelism();
        String sortPartitionField = options.manifestSortPartitionField();
        long manifestFullCompactionThresholdSize =
                options.manifestFullCompactionThresholdSize().getBytes();
        // Step 1: Resolve sort field.
        String sortField = resolveSortField(sortPartitionField, partitionType);
        if (sortField == null) {
            throw new IllegalArgumentException(
                    "Cannot resolve sort field for manifest sort rewrite.");
        }
        int sortFieldIndex = partitionType.getFieldNames().indexOf(sortField);
        RecordComparator fieldComparator =
                CodeGenUtils.newRecordComparator(
                        partitionType.getFieldTypes(), new int[] {sortFieldIndex});

        // Build fileName -> index mapping from input
        Map<String, Integer> fileNameToIndex = new HashMap<>();
        for (int i = 0; i < input.size(); i++) {
            fileNameToIndex.put(input.get(i).fileName(), i);
        }

        // Build result as 2D list with same size as input
        List<List<ManifestFileMeta>> result = new ArrayList<>(input.size());
        for (int i = 0; i < input.size(); i++) {
            result.add(new ArrayList<>());
        }

        // Step 2: Classify manifests into defaultCompaction and LSM.
        ClassifyResult classified =
                classifyManifests(
                        input,
                        suggestedMetaSize,
                        manifestFile,
                        partitionType,
                        manifestFullCompactionThresholdSize,
                        manifestReadParallelism);
        Map<ManifestFileMeta, Boolean> defaultCompactionMap = classified.defaultCompactionManifests;
        List<ManifestFileMeta> lsmFiles = classified.lsmFiles;
        Set<FileEntry.Identifier> deleteEntries = classified.deleteEntries;

        // Step 3: Build LSM Tree and assign levels (only for lsmFiles).
        List<ManifestSortedRun> levelRuns =
                lsmFiles.isEmpty()
                        ? new ArrayList<>()
                        : buildLevelSortedRuns(lsmFiles, fieldComparator);

        // Step 4: Pick runs to compact.
        ManifestPickStrategy pickStrategy =
                new ManifestPickStrategy(
                        options.maxSizeAmplificationPercent(), options.sortedRunSizeRatio());
        List<ManifestSortedRun> pickedRuns = pickStrategy.pick(levelRuns);

        if (pickedRuns.isEmpty() && defaultCompactionMap.isEmpty()) {
            LOG.debug(
                    "Manifest sort rewrite skipped: no runs picked and no defaultCompaction files.");
            return Optional.empty();
        }

        LOG.info(
                "Manifest sort rewrite: input={} files, lsm={} runs, picked={} runs, "
                        + "defaultCompaction={} files.",
                input.size(),
                levelRuns.size(),
                pickedRuns.size(),
                defaultCompactionMap.size());

        Set<ManifestSortedRun> pickedSet = new HashSet<>(pickedRuns);
        List<ManifestFileMeta> reusedFiles = new ArrayList<>();
        for (ManifestSortedRun run : levelRuns) {
            if (!pickedSet.contains(run)) {
                reusedFiles.addAll(run.files());
            }
        }

        // Place reusedFiles at their original index positions
        for (ManifestFileMeta file : reusedFiles) {
            Integer idx = fileNameToIndex.get(file.fileName());
            if (idx != null) {
                result.get(idx).add(file);
            }
        }

        // Step 5: Split picked files into sections, sort and rewrite each.
        List<ManifestFileMeta> pickedFiles = new ArrayList<>();
        for (ManifestSortedRun run : pickedRuns) {
            pickedFiles.addAll(run.files());
        }
        pickedFiles.addAll(defaultCompactionMap.keySet());

        // Compute minIdx and maxIdx from pickedFiles
        int minIdx = Integer.MAX_VALUE;
        int maxIdx = Integer.MIN_VALUE;
        for (ManifestFileMeta meta : pickedFiles) {
            Integer idx = fileNameToIndex.get(meta.fileName());
            if (idx != null) {
                minIdx = Math.min(minIdx, idx);
                maxIdx = Math.max(maxIdx, idx);
            }
        }
        Pair<Integer, Integer> indexRange = Pair.of(minIdx, maxIdx);

        List<Section> sections =
                splitIntoSections(pickedFiles, fieldComparator, defaultCompactionMap);
        sections = mergeSmallAdjacentSections(sections, suggestedMetaSize);
        rewriteSections(
                sections,
                defaultCompactionMap,
                manifestFile,
                fieldComparator,
                deleteEntries,
                suggestedMetaSize,
                options.manifestMergeMinCount(),
                options.manifestSortMaxRewriteSize(),
                result,
                indexRange,
                newFilesForAbort,
                manifestReadParallelism);

        // Flatten 2D result into a single list
        List<ManifestFileMeta> flatResult = new ArrayList<>();
        for (List<ManifestFileMeta> subList : result) {
            flatResult.addAll(subList);
        }

        LOG.info(
                "Manifest sort rewrite completed: sections={}, newFiles={}, resultFiles={}.",
                sections.size(),
                newFilesForAbort.size(),
                flatResult.size());
        return Optional.of(flatResult);
    }

    /**
     * Classify manifest files into default-compaction group and LSM group.
     *
     * <p>Full compaction: small files and files overlapping delete partitions go into
     * defaultCompactionManifests; the rest stay as lsmFiles.
     *
     * <p>Non-full compaction: delete-overlapping files go to result, small files go to
     * defaultCompactionManifests for minor-style merge.
     */
    private static ClassifyResult classifyManifests(
            List<ManifestFileMeta> input,
            long suggestedMetaSize,
            ManifestFile manifestFile,
            RowType partitionType,
            long sizeTrigger,
            @Nullable Integer manifestReadParallelism) {
        // Calculate total size of files that need compaction to determine full-compaction trigger
        Filter<ManifestFileMeta> mustChange =
                file -> file.numDeletedFiles() > 0 || file.fileSize() < suggestedMetaSize;
        long totalDeltaFileSize = 0;
        for (ManifestFileMeta file : input) {
            if (mustChange.test(file)) {
                totalDeltaFileSize += file.fileSize();
            }
        }
        // Initialize classification containers and read delete entries
        Map<ManifestFileMeta, Boolean> defaultCompactionManifests = new LinkedHashMap<>();
        List<ManifestFileMeta> lsmFiles = new LinkedList<>(input);
        Set<FileEntry.Identifier> deleteEntries = Collections.emptySet();
        PartitionPredicate predicate = null;
        if (totalDeltaFileSize >= sizeTrigger) {
            deleteEntries =
                    FileEntry.readDeletedEntries(manifestFile, input, manifestReadParallelism);

            // Build partition predicate from delete entries for overlap detection
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
        }

        // Classify each file based on size and delete-partition overlap
        Iterator<ManifestFileMeta> iterator = lsmFiles.iterator();
        while (iterator.hasNext()) {
            ManifestFileMeta file = iterator.next();
            boolean small = file.fileSize() < suggestedMetaSize;
            boolean inDeleteRange =
                    predicate != null
                            && predicate.test(
                                    file.numAddedFiles() + file.numDeletedFiles(),
                                    file.partitionStats().minValues(),
                                    file.partitionStats().maxValues(),
                                    file.partitionStats().nullCounts());
            if (small || inDeleteRange) {
                iterator.remove();
                defaultCompactionManifests.put(file, inDeleteRange);
            }
        }
        return new ClassifyResult(defaultCompactionManifests, lsmFiles, deleteEntries);
    }

    /**
     * Build level-sorted runs from a list of manifest files. Sorts files by min partition value,
     * greedy-scans to build non-overlapping SortedRuns, then assigns levels by totalSize (Top-4
     * largest to level 1~4, rest to level 0).
     */
    static List<ManifestSortedRun> buildLevelSortedRuns(
            List<ManifestFileMeta> input, RecordComparator fieldComparator) {
        // Step 1: Sort by min value (if equal, then by max value)
        input.sort(
                (a, b) -> {
                    int cmp =
                            fieldComparator.compare(
                                    a.partitionStats().minValues(), b.partitionStats().minValues());
                    if (cmp != 0) {
                        return cmp;
                    }
                    return fieldComparator.compare(
                            a.partitionStats().maxValues(), b.partitionStats().maxValues());
                });

        // Step 2: Interval graph coloring algorithm - assign files to runs
        // Use priority queue to track runs by their max values
        PriorityQueue<List<ManifestFileMeta>> runs =
                new PriorityQueue<>(
                        (r1, r2) -> {
                            ManifestFileMeta last1 = r1.get(r1.size() - 1);
                            ManifestFileMeta last2 = r2.get(r2.size() - 1);
                            return fieldComparator.compare(
                                    last1.partitionStats().maxValues(),
                                    last2.partitionStats().maxValues());
                        });

        for (ManifestFileMeta file : input) {
            List<ManifestFileMeta> earliestRun = runs.poll();
            if (earliestRun == null) {
                // No existing runs, create a new one
                List<ManifestFileMeta> newRun = new ArrayList<>();
                newRun.add(file);
                runs.offer(newRun);
            } else if (fieldComparator.compare(
                            file.partitionStats().minValues(),
                            earliestRun.get(earliestRun.size() - 1).partitionStats().maxValues())
                    >= 0) {
                // Current file's min >= run's max, append to this run
                earliestRun.add(file);
                runs.offer(earliestRun);
            } else {
                // Overlap detected, put the run back and create a new one
                runs.offer(earliestRun);
                List<ManifestFileMeta> newRun = new ArrayList<>();
                newRun.add(file);
                runs.offer(newRun);
            }
        }

        // Step 3: Convert to ManifestSortedRun list
        List<ManifestSortedRun> result = new ArrayList<>();
        while (!runs.isEmpty()) {
            result.add(ManifestSortedRun.fromSorted(runs.poll()));
        }

        // Step 4: Sort by totalSize and assign levels
        result.sort(Comparator.comparingLong(ManifestSortedRun::totalSize));
        int n = result.size();
        int maxLevel = ManifestPickStrategy.MAX_LEVEL;
        for (int i = 0; i < n; i++) {
            if (i >= n - maxLevel) {
                result.get(i).setLevel(i - (n - maxLevel) + 1);
            } else {
                result.get(i).setLevel(0);
            }
        }
        return result;
    }

    /**
     * Split picked files into sections. Files with overlapping sort-key intervals go into the same
     * section. Each section is built with pre-computed totalSize and hasDefaultCompactMeta.
     */
    static List<Section> splitIntoSections(
            List<ManifestFileMeta> pickedFiles,
            RecordComparator fieldComparator,
            Map<ManifestFileMeta, Boolean> defaultCompactionMap) {
        pickedFiles.sort(
                (a, b) -> {
                    int cmp =
                            fieldComparator.compare(
                                    a.partitionStats().minValues(), b.partitionStats().minValues());
                    if (cmp != 0) {
                        return cmp;
                    }
                    return fieldComparator.compare(
                            a.partitionStats().maxValues(), b.partitionStats().maxValues());
                });

        List<Section> sections = new ArrayList<>();
        List<ManifestFileMeta> currentFiles = new ArrayList<>();
        long currentTotalSize = 0;
        boolean currentHasDefault = false;
        ManifestFileMeta first = pickedFiles.get(0);
        currentFiles.add(first);
        currentTotalSize += first.fileSize();
        currentHasDefault = defaultCompactionMap.containsKey(first);
        BinaryRow sectionMaxBound = first.partitionStats().maxValues();

        for (int i = 1; i < pickedFiles.size(); i++) {
            ManifestFileMeta file = pickedFiles.get(i);
            if (fieldComparator.compare(file.partitionStats().minValues(), sectionMaxBound) >= 0) {
                sections.add(new Section(currentFiles, currentTotalSize, currentHasDefault));
                currentFiles = new ArrayList<>();
                currentTotalSize = 0;
                currentFiles.add(file);
                currentTotalSize += file.fileSize();
                currentHasDefault = defaultCompactionMap.containsKey(file);
                sectionMaxBound = file.partitionStats().maxValues();
            } else {
                currentFiles.add(file);
                currentTotalSize += file.fileSize();
                if (!currentHasDefault && defaultCompactionMap.containsKey(file)) {
                    currentHasDefault = true;
                }
                if (fieldComparator.compare(file.partitionStats().maxValues(), sectionMaxBound)
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
     * the pending section or the current section total size is smaller than {@code
     * suggestedMetaSize}, they are combined into a single section.
     */
    private static List<Section> mergeSmallAdjacentSections(
            List<Section> sections, long suggestedMetaSize) {
        List<Section> merged = new ArrayList<>();
        Section pending = null;

        for (Section section : sections) {
            if (pending == null) {
                pending = section;
            } else {
                if (pending.totalSize < suggestedMetaSize
                        || section.totalSize < suggestedMetaSize) {
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
     * Rewrite sections with a budget-controlled strategy.
     *
     * <ul>
     *   <li>1. Single-file section: pass through (rewrite only if it has delete entries).
     *   <li>2. Within budget: sort and rewrite the entire section.
     *   <li>3. First time exceeding budget: partial rewrite within remaining budget, remaining
     *       files form a new section appended for later processing.
     *   <li>4. After budget exhausted with defaultCompaction files: rewrite sub-segments only.
     *   <li>5. After budget exhausted without defaultCompaction files: keep as-is.
     * </ul>
     */
    private static void rewriteSections(
            List<Section> sections,
            Map<ManifestFileMeta, Boolean> defaultCompactionMap,
            ManifestFile manifestFile,
            RecordComparator fieldComparator,
            Set<FileEntry.Identifier> deleteEntries,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            long maxRewriteSize,
            List<List<ManifestFileMeta>> result,
            Pair<Integer, Integer> indexRange,
            List<ManifestFileMeta> sortNewFiles,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        long processedSize = 0;
        boolean reachedLimit = false;

        for (int i = 0; i < sections.size(); i++) {
            Section section = sections.get(i);
            if (section.files.size() == 1) {
                sortAndRewriteSection(
                        section.files,
                        manifestFile,
                        fieldComparator,
                        deleteEntries,
                        defaultCompactionMap,
                        result,
                        indexRange,
                        sortNewFiles,
                        manifestReadParallelism);
                continue;
            }

            if (processedSize + section.totalSize <= maxRewriteSize) {
                processedSize += section.totalSize;
                sortAndRewriteSection(
                        section.files,
                        manifestFile,
                        fieldComparator,
                        deleteEntries,
                        defaultCompactionMap,
                        result,
                        indexRange,
                        sortNewFiles,
                        manifestReadParallelism);
            } else if (!reachedLimit) {
                // Partial rewrite: split section at the budget boundary.
                long rewriteTotalSize = maxRewriteSize - processedSize;
                processedSize += section.totalSize;
                List<ManifestFileMeta> rewriteFiles = new ArrayList<>();
                List<ManifestFileMeta> remainingFiles = new ArrayList<>();
                long rewriteSize = 0;
                long remainingSize = 0;
                boolean remainingHasDefault = false;

                for (ManifestFileMeta file : section.files) {
                    if (rewriteSize + file.fileSize() <= rewriteTotalSize) {
                        rewriteFiles.add(file);
                        rewriteSize += file.fileSize();
                    } else {
                        remainingFiles.add(file);
                        remainingSize += file.fileSize();
                        if (defaultCompactionMap.containsKey(file)) {
                            remainingHasDefault = true;
                        }
                    }
                }

                sortAndRewriteSection(
                        rewriteFiles,
                        manifestFile,
                        fieldComparator,
                        deleteEntries,
                        defaultCompactionMap,
                        result,
                        indexRange,
                        sortNewFiles,
                        manifestReadParallelism);

                // Append remaining files as a new section for later processing.
                if (!remainingFiles.isEmpty()) {
                    Section remainingSection =
                            new Section(remainingFiles, remainingSize, remainingHasDefault);
                    sections.add(remainingSection);
                }
                reachedLimit = true;
            } else if (section.hasDefaultCompactMeta) {
                rewriteSubSegments(
                        section.files,
                        defaultCompactionMap,
                        manifestFile,
                        fieldComparator,
                        deleteEntries,
                        suggestedMetaSize,
                        suggestedMinMetaCount,
                        result,
                        indexRange,
                        sortNewFiles,
                        manifestReadParallelism);
            } else {
                result.get(indexRange.getLeft()).addAll(section.files);
            }
        }
    }

    /**
     * Batch-rewrite files in a section by splitting them into sub-segments of {@code
     * manifestTargetSize}. Tail sub-segment is only rewritten if it has delete entries or meets
     * {@code suggestedMinMetaCount}.
     */
    private static void rewriteSubSegments(
            List<ManifestFileMeta> section,
            Map<ManifestFileMeta, Boolean> defaultCompactionMap,
            ManifestFile manifestFile,
            RecordComparator fieldComparator,
            Set<FileEntry.Identifier> deleteEntries,
            long manifestTargetSize,
            int suggestedMinMetaCount,
            List<List<ManifestFileMeta>> result,
            Pair<Integer, Integer> indexRange,
            List<ManifestFileMeta> sortNewFiles,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        List<ManifestFileMeta> subSegment = new ArrayList<>();
        long subSegmentSize = 0;
        for (ManifestFileMeta m : section) {
            subSegmentSize += m.fileSize();
            subSegment.add(m);

            if (subSegmentSize >= manifestTargetSize) {
                sortAndRewriteSection(
                        subSegment,
                        manifestFile,
                        fieldComparator,
                        deleteEntries,
                        defaultCompactionMap,
                        result,
                        indexRange,
                        sortNewFiles,
                        manifestReadParallelism);
                subSegment.clear();
                subSegmentSize = 0;
            }
        }
        // Flush tail only if delete entries exist or file count >= minCount.
        if (!subSegment.isEmpty()) {
            if (!deleteEntries.isEmpty() || subSegment.size() >= suggestedMinMetaCount) {
                sortAndRewriteSection(
                        subSegment,
                        manifestFile,
                        fieldComparator,
                        deleteEntries,
                        defaultCompactionMap,
                        result,
                        indexRange,
                        sortNewFiles,
                        manifestReadParallelism);
            } else {
                result.get(indexRange.getLeft()).addAll(subSegment);
            }
        }
    }

    /**
     * Read entries from a section's manifest files, split into ADD and DELETE entries, sort each
     * group separately, write to new manifests, and place ADD meta at result[minIdx] and DELETE
     * meta at result[maxIdx].
     */
    private static void sortAndRewriteSection(
            List<ManifestFileMeta> section,
            ManifestFile manifestFile,
            RecordComparator fieldComparator,
            Set<FileEntry.Identifier> deleteEntries,
            Map<ManifestFileMeta, Boolean> defaultCompactionMap,
            List<List<ManifestFileMeta>> result,
            Pair<Integer, Integer> indexRange,
            List<ManifestFileMeta> sortNewFiles,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        // Skip rewrite for single file not in delete-range.
        if (section.size() == 1 && !defaultCompactionMap.getOrDefault(section.get(0), false)) {
            result.get(indexRange.getLeft()).add(section.get(0));
            return;
        }
        // Read all entries in parallel.
        Function<ManifestFileMeta, List<FullCompactionReadResult>> reader =
                meta -> singletonList(readForSortRewrite(meta, manifestFile, deleteEntries));

        List<ManifestEntry> addEntriesToRewrite = new ArrayList<>();
        List<ManifestEntry> deleteEntriesToRewrite = new ArrayList<>();
        for (FullCompactionReadResult readResult :
                sequentialBatchedExecute(reader, section, manifestReadParallelism)) {
            for (ManifestEntry entry : readResult.entries) {
                if (entry.kind() == FileKind.ADD) {
                    addEntriesToRewrite.add(entry);
                } else {
                    deleteEntriesToRewrite.add(entry);
                }
            }
        }

        // Sort and write ADD entries
        if (!addEntriesToRewrite.isEmpty()) {
            addEntriesToRewrite.sort((a, b) -> compareSortKey(a, b, fieldComparator));
            RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                    manifestFile.createRollingWriter();
            Exception exception = null;
            try {
                writer.write(addEntriesToRewrite);
            } catch (Exception e) {
                exception = e;
            } finally {
                if (exception != null) {
                    writer.abort();
                    throw exception;
                }
                writer.close();
            }
            List<ManifestFileMeta> sorted = writer.result();
            result.get(indexRange.getLeft()).addAll(sorted);
            sortNewFiles.addAll(sorted);
        }

        // Sort and write DELETE entries
        if (!deleteEntriesToRewrite.isEmpty()) {
            deleteEntriesToRewrite.sort((a, b) -> compareSortKey(a, b, fieldComparator));
            RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                    manifestFile.createRollingWriter();
            Exception exception = null;
            try {
                writer.write(deleteEntriesToRewrite);
            } catch (Exception e) {
                exception = e;
            } finally {
                if (exception != null) {
                    writer.abort();
                    throw exception;
                }
                writer.close();
            }
            List<ManifestFileMeta> sorted = writer.result();
            result.get(indexRange.getRight()).addAll(sorted);
            sortNewFiles.addAll(sorted);
        }
    }

    /**
     * Compare two {@link ManifestEntry}s by the composite key {@code (sort-field, kind, fileName)}.
     * {@code fileName} is used as the tie-breaker so that all entries sharing the same sort-field
     * value AND the same data file are emitted contiguously.
     */
    static int compareSortKey(ManifestEntry a, ManifestEntry b, RecordComparator fieldComparator) {
        int c = fieldComparator.compare(a.partition(), b.partition());
        if (c != 0) {
            return c;
        }
        // ADD before DELETE
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
            for (ManifestEntry entry :
                    manifestFile.read(
                            meta.fileName(),
                            meta.fileSize(),
                            FileEntry.addFilter(),
                            Filter.alwaysTrue())) {
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
        /** key: ManifestFileMeta, value: boolean[]{isSmall, isInDeleteRange}. */
        final Map<ManifestFileMeta, Boolean> defaultCompactionManifests;

        final List<ManifestFileMeta> lsmFiles;
        @Nullable final Set<FileEntry.Identifier> deleteEntries;

        ClassifyResult(
                Map<ManifestFileMeta, Boolean> defaultCompactionManifests,
                List<ManifestFileMeta> lsmFiles,
                @Nullable Set<FileEntry.Identifier> deleteEntries) {
            this.defaultCompactionManifests = defaultCompactionManifests;
            this.lsmFiles = lsmFiles;
            this.deleteEntries = deleteEntries;
        }
    }
}
