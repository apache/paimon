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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
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

/**
 * Manifest file sorter that sorts and rewrites manifest files by a configured partition field, or
 * by RowID for data evolution tables.
 */
public class ManifestFileSorter {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFileSorter.class);

    /** Context object that carries shared state across compaction methods. */
    static class CompactionContext {
        final boolean fullCompaction;
        final ManifestSortKey sortKey;
        final ManifestEntryExternalSort.ExternalSortConfig externalSortConfig;
        final Set<FileEntry.Identifier> deleteEntries;
        /**
         * Manifest files that need unsorted compaction.
         *
         * <p>Key: manifest file metadata
         *
         * <p>Value: true if fullCompaction is true and the file overlaps with delete partitions. It
         * means the file needs to eliminate delete entries file
         */
        final Map<ManifestFileMeta, Boolean> compactWithoutSort;

        final List<ManifestAdjacentSortedRun> levelRuns;
        final List<ManifestAdjacentSortedRun> pickedRuns;

        CompactionContext(
                boolean fullCompaction,
                ManifestSortKey sortKey,
                ManifestEntryExternalSort.ExternalSortConfig externalSortConfig,
                Set<FileEntry.Identifier> deleteEntries,
                Map<ManifestFileMeta, Boolean> compactWithoutSort,
                List<ManifestAdjacentSortedRun> levelRuns,
                List<ManifestAdjacentSortedRun> pickedRuns) {
            this.fullCompaction = fullCompaction;
            this.sortKey = sortKey;
            this.externalSortConfig = externalSortConfig;
            this.deleteEntries = deleteEntries;
            this.compactWithoutSort = compactWithoutSort;
            this.levelRuns = levelRuns;
            this.pickedRuns = pickedRuns;
        }

        /** Check whether the given manifest file is marked for unsorted compaction. */
        boolean isMarkedForUnsortedCompaction(ManifestFileMeta file) {
            return compactWithoutSort.containsKey(file);
        }
    }

    /** Result of classifying manifest files. */
    private static class ClassifyResult {
        final List<ManifestFileMeta> lsmFiles;
        final Set<FileEntry.Identifier> deleteEntries;
        /**
         * Manifest files that need unsorted compaction.
         *
         * <p>Key: manifest file metadata
         *
         * <p>Value: true if fullCompaction is true and the file overlaps with delete partitions. It
         * means the file needs to eliminate delete entries file
         */
        final Map<ManifestFileMeta, Boolean> compactWithoutSort;

        ClassifyResult(
                List<ManifestFileMeta> lsmFiles,
                Set<FileEntry.Identifier> deleteEntries,
                Map<ManifestFileMeta, Boolean> compactWithoutSort) {
            this.lsmFiles = lsmFiles;
            this.deleteEntries = deleteEntries;
            this.compactWithoutSort = compactWithoutSort;
        }
    }

    /**
     * Try to sort-rewrite the merged manifest list by a configured partition field. If the sort
     * field cannot be resolved, the input is returned as-is.
     *
     * <p>Dispatches to {@link #tryFullCompaction} when totalDeltaFileSize >= sizeTrigger, or {@link
     * #tryMinorCompaction} otherwise.
     */
    static List<ManifestFileMeta> trySortCompaction(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            RowType partitionType,
            CoreOptions options,
            @Nullable IOManager ioManager)
            throws Exception {
        String sortPartitionField = options.manifestSortPartitionField();
        long suggestedMetaSize = options.manifestTargetSize().getBytes();
        int suggestedMinMetaCount = options.manifestMergeMinCount();
        long fullCompactionThreshold = options.manifestFullCompactionThresholdSize().getBytes();
        long maxRewriteSize = options.manifestSortMaxRewriteSize();
        int maxSizeAmplificationPercent = options.maxSizeAmplificationPercent();
        int sortedRunSizeRatio = options.sortedRunSizeRatio();
        Integer manifestReadParallelism = options.scanManifestParallelism();
        ManifestEntryExternalSort.ExternalSortConfig externalSortConfig =
                ManifestEntryExternalSort.ExternalSortConfig.from(options, ioManager);

        Optional<List<ManifestFileMeta>> fullCompacted =
                tryFullCompaction(
                        input,
                        newFilesForAbort,
                        manifestFile,
                        partitionType,
                        sortPartitionField,
                        options.dataEvolutionEnabled(),
                        suggestedMetaSize,
                        suggestedMinMetaCount,
                        fullCompactionThreshold,
                        maxRewriteSize,
                        maxSizeAmplificationPercent,
                        sortedRunSizeRatio,
                        externalSortConfig,
                        manifestReadParallelism);
        if (fullCompacted.isPresent()) {
            return fullCompacted.get();
        }
        return tryMinorCompaction(
                input,
                newFilesForAbort,
                manifestFile,
                partitionType,
                sortPartitionField,
                options.dataEvolutionEnabled(),
                suggestedMetaSize,
                suggestedMinMetaCount,
                maxRewriteSize,
                maxSizeAmplificationPercent,
                sortedRunSizeRatio,
                externalSortConfig,
                manifestReadParallelism);
    }

    /**
     * Full compaction path: totalDeltaFileSize >= sizeTrigger.
     *
     * <p>Does not build index mapping. rewriteSection writes all entries (ADD+DELETE merged)
     * together without separating them.
     */
    private static Optional<List<ManifestFileMeta>> tryFullCompaction(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            RowType partitionType,
            String sortPartitionField,
            boolean dataEvolutionEnabled,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            long fullCompactionThreshold,
            long maxRewriteSize,
            int maxSizeAmplificationPercent,
            int sortedRunSizeRatio,
            ManifestEntryExternalSort.ExternalSortConfig externalSortConfig,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        // Step 1: Check if full compaction threshold is met
        long totalDeltaFileSize = 0;
        for (ManifestFileMeta file : input) {
            if (file.numDeletedFiles() > 0 || file.fileSize() < suggestedMetaSize) {
                totalDeltaFileSize += file.fileSize();
            }
        }
        if (totalDeltaFileSize < fullCompactionThreshold) {
            return Optional.empty();
        }
        // Step 2: Prepare compaction context
        CompactionContext ctx =
                prepareCompaction(
                        input,
                        true,
                        manifestFile,
                        partitionType,
                        sortPartitionField,
                        dataEvolutionEnabled,
                        suggestedMetaSize,
                        maxSizeAmplificationPercent,
                        sortedRunSizeRatio,
                        externalSortConfig,
                        manifestReadParallelism);
        List<ManifestAdjacentSortedRun> levelRuns = ctx.levelRuns;
        List<ManifestAdjacentSortedRun> pickedRuns = ctx.pickedRuns;

        if (pickedRuns.isEmpty() && ctx.compactWithoutSort.isEmpty()) {
            LOG.debug(
                    "Manifest sort full compact skipped: no runs picked and no compactWithoutSort files.");
            return Optional.empty();
        }

        LOG.info(
                "Manifest sort full compact: input={} files, lsm={} runs, picked={} runs, "
                        + "compactWithoutSort={} files.",
                input.size(),
                levelRuns.size(),
                pickedRuns.size(),
                ctx.compactWithoutSort.size());

        // Step 3: Collect reused files (not picked) and picked files
        Set<ManifestAdjacentSortedRun> pickedSet = new HashSet<>(pickedRuns);
        List<ManifestFileMeta> result = new ArrayList<>();
        for (ManifestAdjacentSortedRun run : levelRuns) {
            if (!pickedSet.contains(run)) {
                result.addAll(run.files());
            }
        }
        List<ManifestFileMeta> pickedFiles = new ArrayList<>();
        for (ManifestAdjacentSortedRun run : pickedRuns) {
            pickedFiles.addAll(run.files());
        }
        pickedFiles.addAll(ctx.compactWithoutSort.keySet());

        // Step 4: Split into sections and merge small adjacent sections
        List<Section> sections = splitIntoSections(pickedFiles, ctx);
        sections = mergeSmallAdjacentSections(sections, suggestedMetaSize);

        LOG.info(
                "Manifest sort full compact: pickedFiles={}, sections={}.",
                pickedFiles.size(),
                sections.size());

        // Step 5: Rewrite sections
        FullCompactOutput output = new FullCompactOutput(result);
        rewriteSections(
                sections,
                output,
                newFilesForAbort,
                ctx,
                manifestFile,
                suggestedMetaSize,
                suggestedMinMetaCount,
                maxRewriteSize,
                manifestReadParallelism);

        LOG.info(
                "Manifest sort full compact completed: input={}, resultFiles={}.",
                input.size(),
                result.size());
        return Optional.of(result);
    }

    /**
     * Minor compaction path: totalDeltaFileSize < sizeTrigger.
     *
     * <p>Builds index mapping to preserve original positions. rewriteSection separates ADD and
     * DELETE entries, placing ADD at result[minIdx] and DELETE at result[maxIdx].
     */
    private static List<ManifestFileMeta> tryMinorCompaction(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            RowType partitionType,
            String sortPartitionField,
            boolean dataEvolutionEnabled,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            long maxRewriteSize,
            int maxSizeAmplificationPercent,
            int sortedRunSizeRatio,
            ManifestEntryExternalSort.ExternalSortConfig externalSortConfig,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        // Step 1: Prepare compaction context (early-return if nothing to compact)
        CompactionContext ctx =
                prepareCompaction(
                        input,
                        false,
                        manifestFile,
                        partitionType,
                        sortPartitionField,
                        dataEvolutionEnabled,
                        suggestedMetaSize,
                        maxSizeAmplificationPercent,
                        sortedRunSizeRatio,
                        externalSortConfig,
                        manifestReadParallelism);
        List<ManifestAdjacentSortedRun> levelRuns = ctx.levelRuns;
        List<ManifestAdjacentSortedRun> pickedRuns = ctx.pickedRuns;

        if (pickedRuns.isEmpty() && ctx.compactWithoutSort.isEmpty()) {
            LOG.debug(
                    "Manifest sort minor compact skipped: no runs picked and no compactWithoutSort files.");
            return input;
        }

        LOG.info(
                "Manifest sort minor compact: input={} files, lsm={} runs, picked={} runs, "
                        + "compactWithoutSort={} files.",
                input.size(),
                levelRuns.size(),
                pickedRuns.size(),
                ctx.compactWithoutSort.size());

        // Step 2: Build fileName -> index mapping and initialize 2D result
        Map<String, Integer> fileNameToIndex = new HashMap<>();
        List<List<ManifestFileMeta>> result = new ArrayList<>(input.size());
        for (int i = 0; i < input.size(); i++) {
            fileNameToIndex.put(input.get(i).fileName(), i);
            result.add(new ArrayList<>());
        }

        // Step 3: Collect reused files and picked files
        Set<ManifestAdjacentSortedRun> pickedSet = new HashSet<>(pickedRuns);
        for (ManifestAdjacentSortedRun run : levelRuns) {
            if (!pickedSet.contains(run)) {
                for (ManifestFileMeta file : run.files()) {
                    Integer idx = fileNameToIndex.get(file.fileName());
                    if (idx != null) {
                        result.get(idx).add(file);
                    }
                }
            }
        }

        List<ManifestFileMeta> pickedFiles = new ArrayList<>();
        for (ManifestAdjacentSortedRun run : pickedRuns) {
            pickedFiles.addAll(run.files());
        }
        pickedFiles.addAll(ctx.compactWithoutSort.keySet());

        // Step 4: Compute index range
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

        // Step 5: Split into sections and merge small adjacent sections
        List<Section> sections = splitIntoSections(pickedFiles, ctx);
        sections = mergeSmallAdjacentSections(sections, suggestedMetaSize);

        LOG.info(
                "Manifest sort minor compact: pickedFiles={}, sections={}.",
                pickedFiles.size(),
                sections.size());

        // Step 6: Rewrite sections
        MinorCompactOutput output = new MinorCompactOutput(result, indexRange, fileNameToIndex);
        rewriteSections(
                sections,
                output,
                newFilesForAbort,
                ctx,
                manifestFile,
                suggestedMetaSize,
                suggestedMinMetaCount,
                maxRewriteSize,
                manifestReadParallelism);

        // Step 7: Flatten 2D result into a single list
        List<ManifestFileMeta> flatResult = new ArrayList<>();
        for (List<ManifestFileMeta> subList : result) {
            flatResult.addAll(subList);
        }

        LOG.info(
                "Manifest sort minor compact completed: input={}, resultFiles={}.",
                input.size(),
                flatResult.size());
        return flatResult;
    }

    /**
     * Prepare compaction context: resolve sort field, classify manifests, build level runs, and
     * pick runs for compaction.
     *
     * @return CompactionContext containing all shared state
     */
    private static CompactionContext prepareCompaction(
            List<ManifestFileMeta> input,
            boolean fullCompaction,
            ManifestFile manifestFile,
            RowType partitionType,
            String sortPartitionField,
            boolean dataEvolutionEnabled,
            long suggestedMetaSize,
            int maxSizeAmplificationPercent,
            int sortedRunSizeRatio,
            ManifestEntryExternalSort.ExternalSortConfig externalSortConfig,
            @Nullable Integer manifestReadParallelism) {

        // Step 1: Resolve sort key. Data evolution tables prefer RowID ranges when available.
        ManifestSortKey sortKey =
                createSortKey(dataEvolutionEnabled, input, sortPartitionField, partitionType);

        // Step 2: Classify manifests into LSM files and collect delete entries.
        ClassifyResult classifyResult =
                classifyManifests(
                        input,
                        fullCompaction,
                        manifestFile,
                        partitionType,
                        suggestedMetaSize,
                        manifestReadParallelism);
        List<ManifestFileMeta> lsmFiles = classifyResult.lsmFiles;

        // Step 3: Build level-sorted runs from LSM files based on partition order.
        List<ManifestAdjacentSortedRun> levelRuns =
                lsmFiles.isEmpty() ? new ArrayList<>() : buildLevelSortedRuns(lsmFiles, sortKey);

        // Step 4: Pick runs for compaction using size amplification and ratio strategy.
        ManifestPickStrategy pickStrategy =
                new ManifestPickStrategy(maxSizeAmplificationPercent, sortedRunSizeRatio);
        List<ManifestAdjacentSortedRun> pickedRuns = pickStrategy.pick(levelRuns);

        return new CompactionContext(
                fullCompaction,
                sortKey,
                externalSortConfig,
                classifyResult.deleteEntries,
                classifyResult.compactWithoutSort,
                levelRuns,
                pickedRuns);
    }

    /**
     * Classify manifest files into default-compaction group and LSM group.
     *
     * <p>Full compaction: small files and files overlapping delete partitions go into
     * compactWithoutSort; the rest are returned as lsmFiles.
     *
     * <p>Non-full compaction: small files go to compactWithoutSort for minor-style merge; the rest
     * are returned as lsmFiles.
     *
     * @return ClassifyResult containing lsmFiles, deleteEntries, and compactWithoutSort
     */
    private static ClassifyResult classifyManifests(
            List<ManifestFileMeta> input,
            boolean fullCompaction,
            ManifestFile manifestFile,
            RowType partitionType,
            long suggestedMetaSize,
            @Nullable Integer manifestReadParallelism) {
        // Initialize classification containers and read delete entries
        Map<ManifestFileMeta, Boolean> compactWithoutSort = new LinkedHashMap<>();
        List<ManifestFileMeta> lsmFiles = new LinkedList<>(input);
        Set<FileEntry.Identifier> classifiedDeleteEntries = Collections.emptySet();
        PartitionPredicate predicate = null;
        if (fullCompaction) {
            classifiedDeleteEntries =
                    FileEntry.readDeletedEntries(manifestFile, input, manifestReadParallelism);

            // Build partition predicate from delete entries for overlap detection.
            if (classifiedDeleteEntries.isEmpty()) {
                predicate = PartitionPredicate.ALWAYS_FALSE;
            } else {
                if (partitionType.getFieldCount() > 0) {
                    Set<BinaryRow> deletePartitions =
                            ManifestFileMerger.computeDeletePartitions(classifiedDeleteEntries);
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
                compactWithoutSort.put(file, inDeleteRange);
            }
        }

        return new ClassifyResult(lsmFiles, classifiedDeleteEntries, compactWithoutSort);
    }

    /**
     * Build level-sorted runs from a list of manifest files. Sorts files by min partition value,
     * greedy-scans to build non-overlapping SortedRuns, then assigns levels by totalSize (Top-4
     * largest to level 1~4, rest to level 0).
     */
    static List<ManifestAdjacentSortedRun> buildLevelSortedRuns(
            List<ManifestFileMeta> input, ManifestSortKey sortKey) {
        // Step 1: Sort by min value (if equal, then by max value)
        input.sort(
                (a, b) -> {
                    int cmp = sortKey.compareMin(a, b);
                    if (cmp != 0) {
                        return cmp;
                    }
                    return sortKey.compareMax(a, b);
                });

        // Step 2: Interval graph coloring algorithm - assign files to runs
        // Use priority queue to track runs by their max values
        PriorityQueue<List<ManifestFileMeta>> runs =
                new PriorityQueue<>(
                        (r1, r2) -> {
                            ManifestFileMeta last1 = r1.get(r1.size() - 1);
                            ManifestFileMeta last2 = r2.get(r2.size() - 1);
                            return sortKey.compareMax(last1, last2);
                        });

        for (ManifestFileMeta file : input) {
            List<ManifestFileMeta> earliestRun = runs.poll();
            if (earliestRun == null) {
                // No existing runs, create a new one
                List<ManifestFileMeta> newRun = new ArrayList<>();
                newRun.add(file);
                runs.offer(newRun);
            } else if (sortKey.isAfterMax(file, earliestRun.get(earliestRun.size() - 1))) {
                // Current file's min is after the run's max, append to this run
                // Note: When min == max (boundary equality), files are considered
                // non-overlapping for partition sort and can be placed in the same SortedRun.
                // RowID sort uses inclusive ranges, so boundary equality is treated as overlap.
                //
                // See ManifestAdjacentSortedRun class comment for the full boundary equality
                // semantics.
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

        // Step 3: Convert to ManifestAdjacentSortedRun list
        List<ManifestAdjacentSortedRun> result = new ArrayList<>();
        while (!runs.isEmpty()) {
            result.add(ManifestAdjacentSortedRun.fromSorted(runs.poll()));
        }

        // Step 4: Sort by totalSize and assign levels
        result.sort(Comparator.comparingLong(ManifestAdjacentSortedRun::totalSize));
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
     * section. Each section is built with pre-computed totalSize and hasUnsortedCompactMeta.
     */
    static List<Section> splitIntoSections(
            List<ManifestFileMeta> pickedFiles, CompactionContext ctx) {
        ManifestSortKey sortKey = ctx.sortKey;
        pickedFiles.sort(
                (a, b) -> {
                    int cmp = sortKey.compareMin(a, b);
                    if (cmp != 0) {
                        return cmp;
                    }
                    return sortKey.compareMax(a, b);
                });

        List<Section> sections = new ArrayList<>();
        List<ManifestFileMeta> currentSectionFiles = new ArrayList<>();
        long currentSectionTotalSize = 0;
        ManifestFileMeta first = pickedFiles.get(0);

        currentSectionFiles.add(first);
        currentSectionTotalSize += first.fileSize();
        boolean currentSectionHasUnsortedCompactMeta = ctx.isMarkedForUnsortedCompaction(first);
        ManifestFileMeta sectionMaxFile = first;

        for (int i = 1; i < pickedFiles.size(); i++) {
            ManifestFileMeta file = pickedFiles.get(i);
            // The sort key decides boundary handling. Partition sorting keeps the historical
            // boundary-equality behavior, while RowID sorting treats ranges as inclusive.
            if (sortKey.isAfterMax(file, sectionMaxFile)) {
                sections.add(
                        new Section(
                                currentSectionFiles,
                                currentSectionTotalSize,
                                currentSectionHasUnsortedCompactMeta));
                // start a new section
                currentSectionFiles = new ArrayList<>();
                currentSectionTotalSize = 0;
                currentSectionFiles.add(file);
                currentSectionTotalSize += file.fileSize();
                currentSectionHasUnsortedCompactMeta = ctx.isMarkedForUnsortedCompaction(file);
                sectionMaxFile = file;
            } else {
                currentSectionFiles.add(file);
                currentSectionTotalSize += file.fileSize();
                if (!currentSectionHasUnsortedCompactMeta
                        && ctx.isMarkedForUnsortedCompaction(file)) {
                    currentSectionHasUnsortedCompactMeta = true;
                }
                if (sortKey.compareMax(file, sectionMaxFile) > 0) {
                    sectionMaxFile = file;
                }
            }
        }
        sections.add(
                new Section(
                        currentSectionFiles,
                        currentSectionTotalSize,
                        currentSectionHasUnsortedCompactMeta));
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
     * Rewrite sections with budget control.
     *
     * <p><b>Semantics of manifest-sort.max-rewrite-size:</b> This budget applies only to the sorted
     * rewrite portion. When the cumulative size reaches the limit:
     *
     * <ul>
     *   <li>First overflow: The current section is split. The rewritable part is sorted and
     *       rewritten. The remaining part is appended back to the sections queue for later
     *       processing.
     *   <li>Subsequent overflows: If the section has files in compactWithoutSort (needs unsorted
     *       compaction), unsortedCompactSection is called to process it in smaller chunks.
     *       Otherwise, the section is skipped.
     * </ul>
     *
     * <p>This design ensures that the budget only limits the aggressive sort rewrite, while still
     * allowing necessary cleanup operations (delete entry elimination, small file merge) through
     * the unsortedCompactSection fallback path.
     */
    private static void rewriteSections(
            List<Section> sections,
            RewriteOutput output,
            List<ManifestFileMeta> sortNewFiles,
            CompactionContext ctx,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            long maxRewriteSize,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        // Total data size that has been sort-rewritten so far, used to enforce maxRewriteSize.
        long currentRewrittenSize = 0;
        boolean budgetExhausted = false; // Whether currentRewrittenSize reaches maxRewriteSize.

        for (int i = 0; i < sections.size(); i++) {
            Section section = sections.get(i);

            // A single-file section is always handled directly, regardless of the budget.
            if (section.files.size() == 1) {
                rewriteSection(
                        section.files,
                        output,
                        sortNewFiles,
                        ctx,
                        manifestFile,
                        manifestReadParallelism);
                continue;
            }

            // Phase 1: budget not yet exhausted -- perform aggressive sort rewrite.
            if (!budgetExhausted) {
                // Phase 1a: section fits within the remaining budget -- sort and rewrite it
                // wholly.
                if (currentRewrittenSize + section.totalSize <= maxRewriteSize) {
                    currentRewrittenSize += section.totalSize;
                    rewriteSection(
                            section.files,
                            output,
                            sortNewFiles,
                            ctx,
                            manifestFile,
                            manifestReadParallelism);
                } else {
                    // Phase 1b: first overflow -- split the section at the budget boundary,
                    // rewrite the affordable head, and append the remaining tail back for later
                    // (Phase 2) handling.
                    long remainingBudget = maxRewriteSize - currentRewrittenSize;
                    currentRewrittenSize += section.totalSize;
                    Section remaining =
                            splitSectionAndRewriteHead(
                                    section,
                                    remainingBudget,
                                    output,
                                    sortNewFiles,
                                    ctx,
                                    manifestFile,
                                    manifestReadParallelism);
                    if (remaining != null) {
                        // global ManifestMeta section order by sort key is not a required invariant
                        sections.add(remaining);
                    }
                    budgetExhausted = true;
                }
            } else {
                // Phase 2: budget already exhausted -- only do unsorted compact, skip aggressive
                // sort rewrite.
                rewriteSectionBeyondBudget(
                        section,
                        output,
                        sortNewFiles,
                        ctx,
                        manifestFile,
                        suggestedMetaSize,
                        suggestedMinMetaCount,
                        manifestReadParallelism);
            }
        }
    }

    /**
     * Split a section at the rewrite budget boundary: sort and rewrite the head part that fits
     * within the remaining budget, and return the remaining tail as a new Section (or null if the
     * whole section fits and no tail is left).
     *
     * <p>The budget is applied at manifest-file granularity. The first file that exceeds the
     * remaining budget is still included, and at least two files are rewritten. Otherwise a very
     * small {@code manifest-sort.max-rewrite-size} would make no progress.
     */
    private static Section splitSectionAndRewriteHead(
            Section section,
            long remainingBudget,
            RewriteOutput output,
            List<ManifestFileMeta> sortNewFiles,
            CompactionContext ctx,
            ManifestFile manifestFile,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        List<ManifestFileMeta> headFiles = new ArrayList<>();
        List<ManifestFileMeta> tailFiles = new ArrayList<>();
        long headSize = 0;
        long tailSize = 0;
        // Whether tail section has files in compactWithoutSort, if true, the section need to
        // be rewritten.
        boolean tailHasUnsortedCompactMeta = false;

        for (ManifestFileMeta file : section.files) {
            // Rewrite budget is enforced at manifest-file granularity. Include the first file that
            // crosses the byte budget, and keep at least two files in the rewrite head; otherwise a
            // too-small budget may produce an empty or single-file head and make no sort progress.
            if (headSize <= remainingBudget || headFiles.size() < 2) {
                headFiles.add(file);
                headSize += file.fileSize();
            } else {
                tailFiles.add(file);
                tailSize += file.fileSize();
                if (ctx.isMarkedForUnsortedCompaction(file)) {
                    tailHasUnsortedCompactMeta = true;
                }
            }
        }

        rewriteSection(headFiles, output, sortNewFiles, ctx, manifestFile, manifestReadParallelism);

        if (tailFiles.isEmpty()) {
            return null;
        }
        return new Section(tailFiles, tailSize, tailHasUnsortedCompactMeta);
    }

    /**
     * Handle a section after the sort rewrite budget is exhausted. Sections that contain
     * default-compaction files (small files / delete entries) still go through
     * unsortedCompactSection for necessary cleanup; otherwise they are kept unchanged.
     */
    private static void rewriteSectionBeyondBudget(
            Section section,
            RewriteOutput output,
            List<ManifestFileMeta> sortNewFiles,
            CompactionContext ctx,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        if (section.hasUnsortedCompactMeta) {
            unsortedCompactSection(
                    section.files,
                    output,
                    sortNewFiles,
                    ctx,
                    manifestFile,
                    suggestedMetaSize,
                    suggestedMinMetaCount,
                    manifestReadParallelism);
        } else {
            output.addAllUnchanged(section.files);
        }
    }

    /**
     * Rewrite a section in smaller sub-segments when it exceeds the sort rewrite budget.
     *
     * <p><b>Semantics difference from old minor merge:</b> In the old ManifestFileMerger path, the
     * trailing candidates are kept unchanged when their count is below manifest.merge-min-count. In
     * this sort path, unsortedCompactSection is triggered when compactWithoutSort is non-empty,
     * regardless of the manifest count. This is because files in compactWithoutSort either:
     *
     * <ul>
     *   <li>Are small files needing consolidation
     *   <li>Contain delete entries that must be eliminated
     * </ul>
     *
     * <p>The manifest.merge-min-count threshold is still applied to the final sub-segment's tail,
     * acting as a conservative gate to avoid unnecessary rewrite when there are no delete entries
     * and the tail is too small.
     */
    private static void unsortedCompactSection(
            List<ManifestFileMeta> section,
            RewriteOutput output,
            List<ManifestFileMeta> sortNewFiles,
            CompactionContext ctx,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        List<ManifestFileMeta> candidates = new ArrayList<>();
        long candidatesSize = 0;
        for (ManifestFileMeta m : section) {
            candidatesSize += m.fileSize();
            candidates.add(m);

            if (candidatesSize >= suggestedMetaSize) {
                rewriteSection(
                        candidates,
                        output,
                        sortNewFiles,
                        ctx,
                        manifestFile,
                        manifestReadParallelism);
                candidates.clear();
                candidatesSize = 0;
            }
        }
        // Flush tail only if delete entries exist or file count >= minCount.
        if (!candidates.isEmpty()) {
            if (!ctx.deleteEntries.isEmpty() || candidates.size() >= suggestedMinMetaCount) {
                rewriteSection(
                        candidates,
                        output,
                        sortNewFiles,
                        ctx,
                        manifestFile,
                        manifestReadParallelism);
            } else {
                output.addAllUnchanged(candidates);
            }
        }
    }

    /**
     * Rewrite a section. Dispatches to full or minor compact path.
     *
     * <p>sortNewFiles is the same reference as newFilesForAbort, ensuring newly written files are
     * cleaned up on exception by the caller's catch block.
     */
    private static void rewriteSection(
            List<ManifestFileMeta> section,
            RewriteOutput output,
            List<ManifestFileMeta> sortNewFiles,
            CompactionContext ctx,
            ManifestFile manifestFile,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        // Skip rewrite for single file not in delete-range.
        if (section.size() == 1 && !ctx.compactWithoutSort.getOrDefault(section.get(0), false)) {
            output.addUnchanged(section.get(0));
            return;
        }

        // Add-only minor sections can use the full rewrite path to avoid keeping DELETE entries in
        // memory.
        if (ctx.fullCompaction || containsNoDeleteEntries(section)) {
            rewriteFull(section, output, sortNewFiles, ctx, manifestFile, manifestReadParallelism);
        } else {
            rewriteMinor(section, output, sortNewFiles, ctx, manifestFile, manifestReadParallelism);
        }
    }

    /**
     * Full compaction path: read all surviving entries (ADD merged with DELETE), sort them
     * together, and write to output as a single sorted stream.
     */
    private static void rewriteFull(
            List<ManifestFileMeta> section,
            RewriteOutput output,
            List<ManifestFileMeta> sortNewFiles,
            CompactionContext ctx,
            ManifestFile manifestFile,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        List<ManifestFileMeta> sorted =
                ManifestEntryExternalSort.sortAndWriteFullEntries(
                        section,
                        ctx.sortKey,
                        ctx.externalSortConfig,
                        manifestFile,
                        sortNewFiles,
                        ctx.deleteEntries,
                        manifestReadParallelism);
        if (!sorted.isEmpty()) {
            output.addSortedFiles(sorted);
        }
    }

    /**
     * Minor compaction path: collect DELETE entries in memory while external-sorting all entries,
     * then write surviving ADD entries from the sorted stream and remaining DELETE entries from
     * memory.
     */
    private static void rewriteMinor(
            List<ManifestFileMeta> section,
            RewriteOutput output,
            List<ManifestFileMeta> sortNewFiles,
            CompactionContext ctx,
            ManifestFile manifestFile,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        Pair<List<ManifestFileMeta>, List<ManifestFileMeta>> sorted =
                ManifestEntryExternalSort.sortAndWriteMinorEntries(
                        section,
                        ctx.sortKey,
                        ctx.externalSortConfig,
                        manifestFile,
                        sortNewFiles,
                        manifestReadParallelism);

        if (!sorted.getLeft().isEmpty()) {
            output.addSortedFiles(sorted.getLeft());
        }

        if (!sorted.getRight().isEmpty()) {
            output.addDeleteFiles(sorted.getRight());
        }
    }

    private static boolean containsNoDeleteEntries(List<ManifestFileMeta> section) {
        for (ManifestFileMeta meta : section) {
            if (meta.numDeletedFiles() > 0) {
                return false;
            }
        }
        return true;
    }

    private static ManifestSortKey createSortKey(
            boolean dataEvolutionEnabled,
            List<ManifestFileMeta> input,
            String sortPartitionField,
            RowType partitionType) {
        if (dataEvolutionEnabled && ManifestFileMeta.allContainsRowId(input)) {
            // RowID sorting uses the configured partition field as the primary key when specified,
            // otherwise it uses the full partition row to preserve partition locality. It then
            // orders files by RowID.
            int[] partitionSortFields =
                    createPartitionSortFields(sortPartitionField, partitionType);
            RecordComparator partitionComparator =
                    createPartitionComparator(partitionType, partitionSortFields);
            return new RowIdSortKey(partitionComparator, partitionType, partitionSortFields);
        }

        if (partitionType.getFieldCount() == 0) {
            throw new IllegalArgumentException(
                    "Cannot resolve sort key for manifest sort rewrite.");
        }

        String sortField = resolveSortField(sortPartitionField, partitionType);
        int sortFieldIndex = partitionType.getFieldNames().indexOf(sortField);
        if (sortFieldIndex < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot resolve sort field '%s' for manifest sort rewrite.",
                            sortField));
        }

        RecordComparator fieldComparator =
                CodeGenUtils.newRecordComparator(
                        partitionType.getFieldTypes(), new int[] {sortFieldIndex});
        return new PartitionSortKey(fieldComparator, partitionType, sortFieldIndex);
    }

    private static int[] createPartitionSortFields(
            String sortPartitionField, RowType partitionType) {
        if (sortPartitionField != null && !sortPartitionField.isEmpty()) {
            int sortFieldIndex = partitionType.getFieldNames().indexOf(sortPartitionField);
            if (sortFieldIndex < 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot resolve sort field '%s' for manifest sort rewrite.",
                                sortPartitionField));
            }
            return new int[] {sortFieldIndex};
        }

        int fieldCount = partitionType.getFieldCount();
        int[] sortFields = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            sortFields[i] = i;
        }
        return sortFields;
    }

    @Nullable
    private static RecordComparator createPartitionComparator(
            RowType partitionType, int[] sortFields) {
        if (sortFields.length == 0) {
            return null;
        }
        return CodeGenUtils.newRecordComparator(partitionType.getFieldTypes(), sortFields);
    }

    interface ManifestSortKey {

        int compareMin(ManifestFileMeta a, ManifestFileMeta b);

        int compareMax(ManifestFileMeta a, ManifestFileMeta b);

        boolean isAfterMax(ManifestFileMeta file, ManifestFileMeta maxFile);

        RowType externalSortRowType();

        int[] externalSortKeyFields();

        InternalRow toExternalSortRow(ManifestEntry entry, byte[] entryBytes);

        byte[] entryBytes(BinaryRow row);
    }

    private static class PartitionSortKey implements ManifestSortKey {

        private final RecordComparator fieldComparator;
        private final InternalRow.FieldGetter sortFieldGetter;
        private final RowType externalSortRowType;
        private final int[] externalSortKeyFields;
        private final int sortFieldNum;

        private PartitionSortKey(
                RecordComparator fieldComparator, RowType partitionType, int sortFieldIndex) {
            this.fieldComparator = fieldComparator;
            DataType sortFieldType = partitionType.getTypeAt(sortFieldIndex);
            this.sortFieldGetter = InternalRow.createFieldGetter(sortFieldType, sortFieldIndex);
            this.sortFieldNum = 3;
            this.externalSortRowType =
                    DataTypes.ROW(
                            sortFieldType,
                            DataTypes.TINYINT(),
                            DataTypes.STRING(),
                            DataTypes.BYTES());
            this.externalSortKeyFields = createSequentialFields(sortFieldNum);
        }

        @Override
        public int compareMin(ManifestFileMeta a, ManifestFileMeta b) {
            return fieldComparator.compare(
                    a.partitionStats().minValues(), b.partitionStats().minValues());
        }

        @Override
        public int compareMax(ManifestFileMeta a, ManifestFileMeta b) {
            return fieldComparator.compare(
                    a.partitionStats().maxValues(), b.partitionStats().maxValues());
        }

        @Override
        public boolean isAfterMax(ManifestFileMeta file, ManifestFileMeta maxFile) {
            return fieldComparator.compare(
                            file.partitionStats().minValues(), maxFile.partitionStats().maxValues())
                    >= 0;
        }

        @Override
        public RowType externalSortRowType() {
            return externalSortRowType;
        }

        @Override
        public int[] externalSortKeyFields() {
            return externalSortKeyFields;
        }

        @Override
        public InternalRow toExternalSortRow(ManifestEntry entry, byte[] entryBytes) {
            GenericRow row = new GenericRow(externalSortRowType.getFieldCount());
            row.setField(0, sortFieldGetter.getFieldOrNull(entry.partition()));
            row.setField(1, entry.kind().toByteValue());
            row.setField(2, BinaryString.fromString(entry.file().fileName()));
            row.setField(3, entryBytes);
            return row;
        }

        @Override
        public byte[] entryBytes(BinaryRow row) {
            return row.getBinary(sortFieldNum);
        }
    }

    private static class RowIdSortKey implements ManifestSortKey {

        @Nullable private final RecordComparator partitionComparator;
        private final InternalRow.FieldGetter[] partitionFieldGetters;
        private final RowType externalSortRowType;
        private final int[] externalSortKeyFields;
        private final int sortFieldNum;

        private RowIdSortKey(
                @Nullable RecordComparator partitionComparator,
                RowType partitionType,
                int[] partitionSortFields) {
            this.partitionComparator = partitionComparator;
            this.partitionFieldGetters =
                    createPartitionFieldGetters(partitionType, partitionSortFields);

            List<DataType> fieldTypes = new ArrayList<>();
            for (int partitionSortField : partitionSortFields) {
                fieldTypes.add(partitionType.getTypeAt(partitionSortField));
            }
            fieldTypes.add(DataTypes.BIGINT());
            fieldTypes.add(DataTypes.BIGINT());
            fieldTypes.add(DataTypes.BIGINT());
            fieldTypes.add(DataTypes.TINYINT());
            fieldTypes.add(DataTypes.STRING());
            fieldTypes.add(DataTypes.BYTES());
            this.externalSortRowType = DataTypes.ROW(fieldTypes.toArray(new DataType[0]));
            this.sortFieldNum = externalSortRowType.getFieldCount() - 1;
            this.externalSortKeyFields = createSequentialFields(sortFieldNum);
        }

        @Override
        public int compareMin(ManifestFileMeta a, ManifestFileMeta b) {
            int c = comparePartitionMin(a, b);
            if (c != 0) {
                return c;
            }
            return Long.compare(nonNullMinRowId(a), nonNullMinRowId(b));
        }

        @Override
        public int compareMax(ManifestFileMeta a, ManifestFileMeta b) {
            int c = comparePartitionMax(a, b);
            if (c != 0) {
                return c;
            }
            return Long.compare(nonNullMaxRowId(a), nonNullMaxRowId(b));
        }

        @Override
        public boolean isAfterMax(ManifestFileMeta file, ManifestFileMeta maxFile) {
            if (partitionComparator != null) {
                int c =
                        partitionComparator.compare(
                                file.partitionStats().minValues(),
                                maxFile.partitionStats().maxValues());
                if (c != 0) {
                    return c > 0;
                }
            }
            return Long.compare(nonNullMinRowId(file), nonNullMaxRowId(maxFile)) > 0;
        }

        @Override
        public RowType externalSortRowType() {
            return externalSortRowType;
        }

        @Override
        public int[] externalSortKeyFields() {
            return externalSortKeyFields;
        }

        @Override
        public InternalRow toExternalSortRow(ManifestEntry entry, byte[] entryBytes) {
            GenericRow row = new GenericRow(externalSortRowType.getFieldCount());
            int pos = 0;
            for (InternalRow.FieldGetter partitionFieldGetter : partitionFieldGetters) {
                row.setField(pos++, partitionFieldGetter.getFieldOrNull(entry.partition()));
            }
            row.setField(pos++, entry.file().nonNullFirstRowId());
            row.setField(pos++, rowIdRangeEnd(entry));
            row.setField(pos++, Long.MAX_VALUE - entry.file().maxSequenceNumber());
            row.setField(pos++, entry.kind().toByteValue());
            row.setField(pos++, BinaryString.fromString(entry.file().fileName()));
            row.setField(pos, entryBytes);
            return row;
        }

        @Override
        public byte[] entryBytes(BinaryRow row) {
            return row.getBinary(sortFieldNum);
        }

        private int comparePartitionMin(ManifestFileMeta a, ManifestFileMeta b) {
            if (partitionComparator == null) {
                return 0;
            }
            return partitionComparator.compare(
                    a.partitionStats().minValues(), b.partitionStats().minValues());
        }

        private int comparePartitionMax(ManifestFileMeta a, ManifestFileMeta b) {
            if (partitionComparator == null) {
                return 0;
            }
            return partitionComparator.compare(
                    a.partitionStats().maxValues(), b.partitionStats().maxValues());
        }

        private static long nonNullMinRowId(ManifestFileMeta meta) {
            Long minRowId = meta.minRowId();
            if (minRowId == null) {
                throw new IllegalArgumentException(
                        String.format("Manifest file '%s' has no min RowID.", meta.fileName()));
            }
            return minRowId;
        }

        private static long nonNullMaxRowId(ManifestFileMeta meta) {
            Long maxRowId = meta.maxRowId();
            if (maxRowId == null) {
                throw new IllegalArgumentException(
                        String.format("Manifest file '%s' has no max RowID.", meta.fileName()));
            }
            return maxRowId;
        }

        private static long rowIdRangeEnd(ManifestEntry entry) {
            return entry.file().nonNullFirstRowId() + entry.file().rowCount() - 1;
        }
    }

    private static int[] createSequentialFields(int fieldCount) {
        int[] fields = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fields[i] = i;
        }
        return fields;
    }

    private static InternalRow.FieldGetter[] createPartitionFieldGetters(
            RowType partitionType, int[] partitionSortFields) {
        InternalRow.FieldGetter[] fieldGetters =
                new InternalRow.FieldGetter[partitionSortFields.length];
        for (int i = 0; i < partitionSortFields.length; i++) {
            int fieldIndex = partitionSortFields[i];
            fieldGetters[i] =
                    InternalRow.createFieldGetter(partitionType.getTypeAt(fieldIndex), fieldIndex);
        }
        return fieldGetters;
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

    /** Strategy interface for writing compaction results. */
    interface RewriteOutput {
        void addUnchanged(ManifestFileMeta file);

        void addAllUnchanged(List<ManifestFileMeta> files);

        void addSortedFiles(List<ManifestFileMeta> files);

        void addDeleteFiles(List<ManifestFileMeta> files);
    }

    private static class FullCompactOutput implements RewriteOutput {
        private final List<ManifestFileMeta> result;

        FullCompactOutput(List<ManifestFileMeta> result) {
            this.result = result;
        }

        @Override
        public void addUnchanged(ManifestFileMeta file) {
            result.add(file);
        }

        @Override
        public void addAllUnchanged(List<ManifestFileMeta> files) {
            result.addAll(files);
        }

        @Override
        public void addSortedFiles(List<ManifestFileMeta> files) {
            result.addAll(files);
        }

        @Override
        public void addDeleteFiles(List<ManifestFileMeta> files) {
            result.addAll(files);
        }
    }

    private static class MinorCompactOutput implements RewriteOutput {
        private final List<List<ManifestFileMeta>> result;
        private final Pair<Integer, Integer> indexRange;
        private final Map<String, Integer> fileNameToIndex;

        MinorCompactOutput(
                List<List<ManifestFileMeta>> result,
                Pair<Integer, Integer> indexRange,
                Map<String, Integer> fileNameToIndex) {
            this.result = result;
            this.indexRange = indexRange;
            this.fileNameToIndex = fileNameToIndex;
        }

        @Override
        public void addUnchanged(ManifestFileMeta file) {
            Integer idx = fileNameToIndex.get(file.fileName());
            result.get(idx).add(file);
        }

        @Override
        public void addAllUnchanged(List<ManifestFileMeta> files) {
            for (ManifestFileMeta file : files) {
                addUnchanged(file);
            }
        }

        @Override
        public void addSortedFiles(List<ManifestFileMeta> files) {
            result.get(indexRange.getLeft()).addAll(files);
        }

        @Override
        public void addDeleteFiles(List<ManifestFileMeta> files) {
            result.get(indexRange.getRight()).addAll(files);
        }
    }

    /** A section of manifest files with pre-computed metadata. */
    static class Section {
        final List<ManifestFileMeta> files;
        final long totalSize;
        final boolean hasUnsortedCompactMeta;

        Section(List<ManifestFileMeta> files, long totalSize, boolean hasUnsortedCompactMeta) {
            this.files = files;
            this.totalSize = totalSize;
            this.hasUnsortedCompactMeta = hasUnsortedCompactMeta;
        }

        /** Create a merged section from two sections. */
        static Section merge(Section a, Section b) {
            List<ManifestFileMeta> merged = new ArrayList<>(a.files);
            merged.addAll(b.files);
            return new Section(
                    merged,
                    a.totalSize + b.totalSize,
                    a.hasUnsortedCompactMeta || b.hasUnsortedCompactMeta);
        }
    }
}
