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
import java.util.LinkedList;
import java.util.List;
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
        long maxRewriteSize = options.manifestSortMaxRewriteSize();
        long openFileCost = options.manifestSortOpenFileCost();

        List<Section> sections =
                splitIntoSections(
                        pickedFiles,
                        sortFieldIndex,
                        sortFieldType,
                        defaultCompactionSet,
                        openFileCost);
        sections = mergeSmallAdjacentSections(sections, suggestedMetaSize);
        System.out.println(
                "After splitIntoSections: sections="
                        + sections.size()
                        + ", pickedFiles="
                        + pickedFiles.size());
        LOG.info("After mergeSmallAdjacentSections: sections={}.", sections.size());

        List<ManifestFileMeta> sortNewFiles = new ArrayList<>();

        List<ManifestFileMeta> rewritten =
                rewriteSections(
                        sections,
                        defaultCompactionSet,
                        manifestFile,
                        sortFieldIndex,
                        sortFieldType,
                        deleteEntries,
                        manifestFullCompactionSize,
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
            ManifestFile manifestFile,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism) {
        Filter<ManifestFileMeta> mustChange =
                file -> file.numDeletedFiles() > 0 || file.fileSize() < suggestedMetaSize;


        List<ManifestFileMeta> defaultCompactionManifests = new ArrayList<>();
        List<ManifestFileMeta> lsmFiles = new LinkedList<>(input);
        Set<FileEntry.Identifier> deleteEntries =
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
            Set<FileEntry.Identifier> deleteEntries,
            long manifestFullCompactionSize,
            long maxRewriteSize,
            long openFileCost,
            List<ManifestFileMeta> sortNewFiles,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        List<ManifestFileMeta> result = new ArrayList<>();
        long processedSize = 0;

        boolean reachedLimit = false;

        for (int i = 0; i < sections.size(); i++) {
            Section section = sections.get(i);
            // Single-file section without defaultCompaction: already sorted, skip rewrite.
            if (section.files.size() == 1) {
                if (!section.hasDefaultCompactMeta || deleteEntries.isEmpty()) {
                    result.addAll(section.files);
                } else {
                    processedSize = processedSize + section.totalSizeWithCost;
                    rewriteSubSegments(
                            section.files,
                            defaultCompactionSet,
                            manifestFile,
                            sortFieldIndex,
                            sortFieldType,
                            deleteEntries,
                            manifestFullCompactionSize,
                            sortNewFiles,
                            result,
                            manifestReadParallelism);
                }
                continue;
            }
            long sectionSize = section.totalSizeWithCost;
            boolean exceedsThreshold = processedSize + sectionSize > maxRewriteSize;

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
            } else if (!reachedLimit) {
                // First time exceeding threshold without defaultCompaction:
                // partial rewrite within remaining budget.
                long remaining = maxRewriteSize - processedSize;
                processedSize += sectionSize;
                // Split section into two parts: files within budget and remaining files
                List<ManifestFileMeta> toRewrite = new ArrayList<>();
                List<ManifestFileMeta> remainingFiles = new ArrayList<>();
                long rewriteSize = 0;
                long remainingSize = 0;
                long remainingSizeWithCost = 0;
                boolean remainingHasDefault = false;

                for (ManifestFileMeta file : section.files) {
                    long fileCost = Math.max(file.fileSize(), openFileCost);
                    if (rewriteSize + fileCost <= remaining) {
                        toRewrite.add(file);
                        rewriteSize += fileCost;
                    } else {
                        remainingFiles.add(file);
                        remainingSize += file.fileSize();
                        remainingSizeWithCost += fileCost;
                        if (defaultCompactionSet.contains(file)) {
                            remainingHasDefault = true;
                        }
                    }
                }

                if (toRewrite.size() > 1) {
                    List<ManifestFileMeta> merged =
                            sortAndRewriteSection(
                                    toRewrite,
                                    manifestFile,
                                    sortFieldIndex,
                                    sortFieldType,
                                    deleteEntries,
                                    manifestReadParallelism);
                    sortNewFiles.addAll(merged);
                    result.addAll(merged);
                } else if (toRewrite.size() == 1) {
                    sortNewFiles.addAll(toRewrite);
                    result.addAll(toRewrite);
                }

                // Create new section for remaining files and append to sections list
                if (!remainingFiles.isEmpty()) {
                    Section remainingSection =
                            new Section(
                                    remainingFiles,
                                    remainingSize,
                                    remainingSizeWithCost,
                                    remainingHasDefault);
                    // Append remaining section to the end of sections list
                    sections.add(remainingSection);
                }
                reachedLimit = true;
            } else if (section.hasDefaultCompactMeta) {
                rewriteSubSegments(
                        section.files,
                        defaultCompactionSet,
                        manifestFile,
                        sortFieldIndex,
                        sortFieldType,
                        deleteEntries,
                        manifestFullCompactionSize,
                        sortNewFiles,
                        result,
                        manifestReadParallelism);
            } else {
                result.addAll(section.files);
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
            long manifestFullCompactionSize,
            List<ManifestFileMeta> sortNewFiles,
            List<ManifestFileMeta> result,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        List<ManifestFileMeta> subSegment = new ArrayList<>();
        long subSegmentSize = 0;
        for (ManifestFileMeta m : section) {
            boolean shouldAccumulate =
                    defaultCompactionSet.contains(m)
                            && subSegmentSize + m.fileSize() < manifestFullCompactionSize;

            if (shouldAccumulate) {
                // Continue accumulating
                subSegment.add(m);
                subSegmentSize += m.fileSize();
            } else {
                // Need to break the segment
                if (!subSegment.isEmpty()) {
                    // Process accumulated subSegment first
                    subSegment.add(m);
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
                    subSegment.clear();
                    subSegmentSize = 0;
                } else {
                    // Directly add to result
                    result.add(m);
                }
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
     * Partial rewrite of a section: only rewrite files that fit within the remaining budget. Files
     * beyond the budget are kept as-is.
     */
    private static void partialRewriteSection(
            List<ManifestFileMeta> sectionFiles,
            long remaining,
            long openFileCost,
            ManifestFile manifestFile,
            int sortFieldIndex,
            DataType sortFieldType,
            @Nullable Set<FileEntry.Identifier> deleteEntries,
            List<ManifestFileMeta> sortNewFiles,
            List<ManifestFileMeta> result,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        List<ManifestFileMeta> toRewrite = new ArrayList<>();
        int splitIndex = 0;
        long partialSize = 0;
        for (int i = 0; i < sectionFiles.size(); i++) {
            long fileCost = Math.max(sectionFiles.get(i).fileSize(), openFileCost);
            if (partialSize + fileCost > remaining) {
                break;
            }
            toRewrite.add(sectionFiles.get(i));
            partialSize += fileCost;
            splitIndex = i + 1;
        }
        if (toRewrite.size() > 1) {
            List<ManifestFileMeta> merged =
                    sortAndRewriteSection(
                            toRewrite,
                            manifestFile,
                            sortFieldIndex,
                            sortFieldType,
                            deleteEntries,
                            manifestReadParallelism);
            sortNewFiles.addAll(merged);
            result.addAll(merged);
        } else {
            result.addAll(toRewrite);
        }
        for (int i = splitIndex; i < sectionFiles.size(); i++) {
            result.add(sectionFiles.get(i));
        }
    }

    /**
     * Build level-sorted runs from a list of manifest files. Sorts files by min partition value,
     * greedy-scans to build non-overlapping SortedRuns, then assigns levels by totalSize (Top-4
     * largest to level 1~4, rest to level 0).
     */
    static List<ManifestSortedRun> buildLevelSortedRuns(
            List<ManifestFileMeta> input, int sortFieldIndex, DataType sortFieldType) {
        // Step 1: Sort by min value (if equal, then by max value)
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

        // Step 2: Interval graph coloring algorithm - assign files to runs
        // Use priority queue to track runs by their max values
        PriorityQueue<List<ManifestFileMeta>> runs =
                new PriorityQueue<>(
                        (r1, r2) -> {
                            ManifestFileMeta last1 = r1.get(r1.size() - 1);
                            ManifestFileMeta last2 = r2.get(r2.size() - 1);
                            return compareField(
                                    last1.partitionStats().maxValues(),
                                    last2.partitionStats().maxValues(),
                                    sortFieldIndex,
                                    sortFieldType);
                        });

        for (ManifestFileMeta file : input) {
            boolean addedToExisting = false;

            // Try to find a run where current file's min >= run's max
            if (!runs.isEmpty()) {
                List<ManifestFileMeta> earliestRun = runs.peek();
                ManifestFileMeta last = earliestRun.get(earliestRun.size() - 1);

                if (compareField(
                                file.partitionStats().minValues(),
                                last.partitionStats().maxValues(),
                                sortFieldIndex,
                                sortFieldType)
                        >= 0) {
                    // Current file can be added to this run
                    runs.poll();
                    earliestRun.add(file);
                    runs.offer(earliestRun);
                    addedToExisting = true;
                }
            }

            if (!addedToExisting) {
                // Create a new run
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
        int maxLevel = 4;
        for (int i = 0; i < n; i++) {
            if (i >= n - maxLevel) {
                result.get(i).setLevel(i - (n - maxLevel) + 1);
            } else {
                result.get(i).setLevel(0);
            }
        }
        System.out.println("run num: " + result.size());
        return result;
    }

    /**
     * Split picked files into sections. Files with overlapping sort-key intervals go into the same
     * section. Each section is built with pre-computed totalSize and hasDefaultCompactMeta.
     */
    static List<Section> splitIntoSections(
            List<ManifestFileMeta> pickedFiles,
            int sortFieldIndex,
            DataType sortFieldType,
            Set<ManifestFileMeta> defaultCompactionSet,
            long openFileCost) {
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
        long currentTotalSizeWithCost = 0;
        boolean currentHasDefault = false;
        ManifestFileMeta first = pickedFiles.get(0);
        currentFiles.add(first);
        currentTotalSize += first.fileSize();
        currentTotalSizeWithCost += Math.max(first.fileSize(), openFileCost);
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
                sections.add(
                        new Section(
                                currentFiles,
                                currentTotalSize,
                                currentTotalSizeWithCost,
                                currentHasDefault));
                currentFiles = new ArrayList<>();
                currentTotalSize = 0;
                currentTotalSizeWithCost = 0;
                currentFiles.add(file);
                currentTotalSize += file.fileSize();
                currentTotalSizeWithCost += Math.max(file.fileSize(), openFileCost);
                currentHasDefault = defaultCompactionSet.contains(file);
                sectionMaxBound = file.partitionStats().maxValues();
            } else {
                currentFiles.add(file);
                currentTotalSize += file.fileSize();
                currentTotalSizeWithCost += Math.max(file.fileSize(), openFileCost);
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
        sections.add(
                new Section(
                        currentFiles,
                        currentTotalSize,
                        currentTotalSizeWithCost,
                        currentHasDefault));
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
            Set<FileEntry.Identifier> deletedIdentifiers,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        if (section.size() == 1 && deletedIdentifiers.isEmpty()) {
            return section;
        }
        long totalStart = System.currentTimeMillis();
        long readTime = 0;
        long sortTime = 0;
        long writeTime = 0;

        // Parallel read: each meta is read independently
        long readStart = System.currentTimeMillis();
        Function<ManifestFileMeta, List<FullCompactionReadResult>> reader =
                meta -> singletonList(readForSortRewrite(meta, manifestFile, deletedIdentifiers));

        List<ManifestEntry> entriesToRewrite = new ArrayList<>();
        for (FullCompactionReadResult readResult :
                sequentialBatchedExecute(reader, section, manifestReadParallelism)) {
            entriesToRewrite.addAll(readResult.entries);
        }
        readTime = System.currentTimeMillis() - readStart;

        List<ManifestFileMeta> result = new ArrayList<>();
        if (!entriesToRewrite.isEmpty()) {
            long sortStart = System.currentTimeMillis();
            entriesToRewrite.sort((a, b) -> compareSortKey(a, b, sortFieldIndex, sortFieldType));
            sortTime = System.currentTimeMillis() - sortStart;

            long writeStart = System.currentTimeMillis();
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
            writeTime = System.currentTimeMillis() - writeStart;
        }

        long totalTime = System.currentTimeMillis() - totalStart;
        if (totalTime > 0) {
            System.out.println(
                    String.format(
                            "[sortAndRewriteSection] Total: %d ms, Read: %d ms (%.1f%%), Sort: %d ms (%.1f%%), Write: %d ms (%.1f%%), Entries: %d, Files: %d",
                            totalTime,
                            readTime,
                            100.0 * readTime / totalTime,
                            sortTime,
                            100.0 * sortTime / totalTime,
                            writeTime,
                            100.0 * writeTime / totalTime,
                            entriesToRewrite.size(),
                            result.size()));
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
        final long totalSizeWithCost;
        final boolean hasDefaultCompactMeta;

        Section(
                List<ManifestFileMeta> files,
                long totalSize,
                long totalSizeWithCost,
                boolean hasDefaultCompactMeta) {
            this.files = files;
            this.totalSize = totalSize;
            this.totalSizeWithCost = totalSizeWithCost;
            this.hasDefaultCompactMeta = hasDefaultCompactMeta;
        }

        /** Create a merged section from two sections. */
        static Section merge(Section a, Section b) {
            List<ManifestFileMeta> merged = new ArrayList<>(a.files);
            merged.addAll(b.files);
            return new Section(
                    merged,
                    a.totalSize + b.totalSize,
                    a.totalSizeWithCost + b.totalSizeWithCost,
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
