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

    // Immutable fields set at construction time
    private final ManifestFile manifestFile;
    private final RowType partitionType;
    private final String sortPartitionField;

    private final long suggestedMetaSize;
    private final int suggestedMinMetaCount;
    private final long fullCompactionThreshold;
    private final long maxRewriteSize;
    private final int maxSizeAmplificationPercent;
    private final int sortedRunSizeRatio;
    @Nullable private final Integer manifestReadParallelism;

    // Mutable fields set during prepareCompaction
    private boolean fullCompaction;
    private RecordComparator fieldComparator;
    private Set<FileEntry.Identifier> deleteEntries;
    private Map<ManifestFileMeta, Boolean> defaultCompactionMap;

    ManifestFileSorter(ManifestFile manifestFile, RowType partitionType, CoreOptions options) {
        this.manifestFile = manifestFile;
        this.partitionType = partitionType;
        this.sortPartitionField = options.manifestSortPartitionField();
        this.suggestedMetaSize = options.manifestTargetSize().getBytes();
        this.suggestedMinMetaCount = options.manifestMergeMinCount();
        this.fullCompactionThreshold = options.manifestFullCompactionThresholdSize().getBytes();
        this.maxRewriteSize = options.manifestSortMaxRewriteSize();
        this.maxSizeAmplificationPercent = options.maxSizeAmplificationPercent();
        this.sortedRunSizeRatio = options.sortedRunSizeRatio();
        this.manifestReadParallelism = options.scanManifestParallelism();
    }

    /**
     * Try to sort-rewrite the merged manifest list by a configured partition field. If the sort
     * field cannot be resolved, the input is returned as-is.
     *
     * <p>Dispatches to {@link #tryFullCompact} when totalDeltaFileSize >= sizeTrigger, or {@link
     * #tryMinorCompact} otherwise.
     */
    List<ManifestFileMeta> trySortRewrite(
            List<ManifestFileMeta> input, List<ManifestFileMeta> newFilesForAbort)
            throws Exception {
        Optional<List<ManifestFileMeta>> fullCompacted = tryFullCompact(input, newFilesForAbort);
        if (fullCompacted.isPresent()) {
            return fullCompacted.get();
        }
        return tryMinorCompact(input, newFilesForAbort);
    }

    /**
     * Full compaction path: totalDeltaFileSize >= sizeTrigger.
     *
     * <p>Does not build index mapping. sortAndRewriteSection writes all entries (ADD+DELETE merged)
     * together without separating them.
     */
    private Optional<List<ManifestFileMeta>> tryFullCompact(
            List<ManifestFileMeta> input, List<ManifestFileMeta> newFilesForAbort)
            throws Exception {
        // Step 1: Check if full compaction threshold is met
        long totalDeltaFileSize = 0;
        for (ManifestFileMeta file : input) {
            if (file.numDeletedFiles() > 0 || file.fileSize() < suggestedMetaSize) {
                totalDeltaFileSize += file.fileSize();
            }
        }
        if (totalDeltaFileSize < fullCompactionThreshold) {
            this.fullCompaction = false;
            return Optional.empty();
        }
        this.fullCompaction = true;
        // Step 2: Prepare compaction context
        Pair<List<ManifestAdjacentSortedRun>, List<ManifestAdjacentSortedRun>> runsPair =
                prepareCompaction(input);
        List<ManifestAdjacentSortedRun> levelRuns = runsPair.getLeft();
        List<ManifestAdjacentSortedRun> pickedRuns = runsPair.getRight();

        if (pickedRuns.isEmpty() && defaultCompactionMap.isEmpty()) {
            LOG.debug(
                    "Manifest sort full compact skipped: no runs picked and no defaultCompaction files.");
            return Optional.empty();
        }

        LOG.info(
                "Manifest sort full compact: input={} files, lsm={} runs, picked={} runs, "
                        + "defaultCompaction={} files.",
                input.size(),
                levelRuns.size(),
                pickedRuns.size(),
                defaultCompactionMap.size());

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
        pickedFiles.addAll(defaultCompactionMap.keySet());

        // Step 4: Split into sections and merge small adjacent sections
        List<Section> sections =
                splitIntoSections(pickedFiles, fieldComparator, defaultCompactionMap);
        sections = mergeSmallAdjacentSections(sections, suggestedMetaSize);

        // Step 5: Rewrite sections
        FullCompactOutput output = new FullCompactOutput(result);
        rewriteSections(sections, output, newFilesForAbort);

        LOG.info(
                "Manifest sort full compact completed: sections={}, newFiles={}, resultFiles={}.",
                sections.size(),
                newFilesForAbort.size(),
                result.size());
        return Optional.of(result);
    }

    /**
     * Minor compaction path: totalDeltaFileSize < sizeTrigger.
     *
     * <p>Builds index mapping to preserve original positions. sortAndRewriteSection separates ADD
     * and DELETE entries, placing ADD at result[minIdx] and DELETE at result[maxIdx].
     */
    private List<ManifestFileMeta> tryMinorCompact(
            List<ManifestFileMeta> input, List<ManifestFileMeta> newFilesForAbort)
            throws Exception {
        // Step 1: Prepare compaction context (early-return if nothing to compact)
        Pair<List<ManifestAdjacentSortedRun>, List<ManifestAdjacentSortedRun>> runsPair =
                prepareCompaction(input);
        List<ManifestAdjacentSortedRun> levelRuns = runsPair.getLeft();
        List<ManifestAdjacentSortedRun> pickedRuns = runsPair.getRight();

        if (pickedRuns.isEmpty() && defaultCompactionMap.isEmpty()) {
            LOG.debug(
                    "Manifest sort minor compact skipped: no runs picked and no defaultCompaction files.");
            return input;
        }

        LOG.info(
                "Manifest sort minor compact: input={} files, lsm={} runs, picked={} runs, "
                        + "defaultCompaction={} files.",
                input.size(),
                levelRuns.size(),
                pickedRuns.size(),
                defaultCompactionMap.size());

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
        pickedFiles.addAll(defaultCompactionMap.keySet());

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
        List<Section> sections =
                splitIntoSections(pickedFiles, fieldComparator, defaultCompactionMap);
        sections = mergeSmallAdjacentSections(sections, suggestedMetaSize);

        // Step 6: Rewrite sections
        MinorCompactOutput output = new MinorCompactOutput(result, indexRange, fileNameToIndex);
        rewriteSections(sections, output, newFilesForAbort);

        // Step 7: Flatten 2D result into a single list
        List<ManifestFileMeta> flatResult = new ArrayList<>();
        for (List<ManifestFileMeta> subList : result) {
            flatResult.addAll(subList);
        }

        LOG.info(
                "Manifest sort minor compact completed: sections={}, newFiles={}, resultFiles={}.",
                sections.size(),
                newFilesForAbort.size(),
                flatResult.size());
        return flatResult;
    }

    /**
     * Prepare compaction context: resolve sort field, classify manifests, build level runs, and
     * pick runs for compaction. Sets instance fields: fullCompaction, fieldComparator,
     * deleteEntries, defaultCompactionMap.
     *
     * @return Pair of (levelRuns, pickedRuns)
     */
    private Pair<List<ManifestAdjacentSortedRun>, List<ManifestAdjacentSortedRun>>
            prepareCompaction(List<ManifestFileMeta> input) {

        String sortField = resolveSortField(sortPartitionField, partitionType);
        if (sortField == null) {
            throw new IllegalArgumentException(
                    "Cannot resolve sort field for manifest sort rewrite.");
        }
        int sortFieldIndex = partitionType.getFieldNames().indexOf(sortField);
        this.fieldComparator =
                CodeGenUtils.newRecordComparator(
                        partitionType.getFieldTypes(), new int[] {sortFieldIndex});

        List<ManifestFileMeta> lsmFiles = classifyManifests(input);
        List<ManifestAdjacentSortedRun> levelRuns =
                lsmFiles.isEmpty()
                        ? new ArrayList<>()
                        : buildLevelSortedRuns(lsmFiles, fieldComparator);

        ManifestPickStrategy pickStrategy =
                new ManifestPickStrategy(maxSizeAmplificationPercent, sortedRunSizeRatio);
        List<ManifestAdjacentSortedRun> pickedRuns = pickStrategy.pick(levelRuns);

        return Pair.of(levelRuns, pickedRuns);
    }

    /**
     * Classify manifest files into default-compaction group and LSM group. Sets instance fields
     * {@link #deleteEntries} and {@link #defaultCompactionMap}.
     *
     * <p>Full compaction: small files and files overlapping delete partitions go into
     * defaultCompactionMap; the rest are returned as lsmFiles.
     *
     * <p>Non-full compaction: small files go to defaultCompactionMap for minor-style merge; the
     * rest are returned as lsmFiles.
     *
     * @return lsmFiles that should participate in LSM-tree compaction
     */
    private List<ManifestFileMeta> classifyManifests(List<ManifestFileMeta> input) {
        // Initialize classification containers and read delete entries
        Map<ManifestFileMeta, Boolean> classifiedDefaultMap = new LinkedHashMap<>();
        List<ManifestFileMeta> lsmFiles = new LinkedList<>(input);
        Set<FileEntry.Identifier> classifiedDeleteEntries = Collections.emptySet();
        PartitionPredicate predicate = null;
        if (fullCompaction) {
            classifiedDeleteEntries =
                    FileEntry.readDeletedEntries(manifestFile, input, manifestReadParallelism);

            // Build partition predicate from delete entries for overlap detection
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
                classifiedDefaultMap.put(file, inDeleteRange);
            }
        }

        // Set instance fields
        this.deleteEntries = classifiedDeleteEntries;
        this.defaultCompactionMap = classifiedDefaultMap;
        return lsmFiles;
    }

    /**
     * Build level-sorted runs from a list of manifest files. Sorts files by min partition value,
     * greedy-scans to build non-overlapping SortedRuns, then assigns levels by totalSize (Top-4
     * largest to level 1~4, rest to level 0).
     */
    static List<ManifestAdjacentSortedRun> buildLevelSortedRuns(
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
                // Note: When min == max (boundary equality), files are considered
                // non-overlapping and can be placed in the same SortedRun. This allows
                // building fewer SortedRuns, improving compaction efficiency while
                // maintaining correct sort order.
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
            // Note: Boundary equality (file.min == sectionMaxBound) results in separate
            // sections. This avoids merge-sort overhead while maintaining partition filtering
            // capability. Files with non-overlapping boundaries (including equal boundaries)
            // can be processed independently without significantly impacting partition pruning
            // efficiency.
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

    /** Rewrite sections with budget control. */
    private void rewriteSections(
            List<Section> sections, RewriteOutput output, List<ManifestFileMeta> sortNewFiles)
            throws Exception {
        long processedSize = 0;
        boolean reachedLimit = false;

        for (int i = 0; i < sections.size(); i++) {
            Section section = sections.get(i);
            if (section.files.size() == 1) {
                sortAndRewriteSection(section.files, output, sortNewFiles);
                continue;
            }

            if (processedSize + section.totalSize <= maxRewriteSize) {
                processedSize += section.totalSize;
                sortAndRewriteSection(section.files, output, sortNewFiles);
            } else if (!reachedLimit) {
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

                sortAndRewriteSection(rewriteFiles, output, sortNewFiles);

                if (!remainingFiles.isEmpty()) {
                    Section remainingSection =
                            new Section(remainingFiles, remainingSize, remainingHasDefault);
                    sections.add(remainingSection);
                }
                reachedLimit = true;
            } else if (section.hasDefaultCompactMeta) {
                rewriteSubSegments(section.files, output, sortNewFiles);
            } else {
                output.addAllUnchanged(section.files);
            }
        }
    }

    /** Rewrite sub-segments within a section that exceeded the budget. */
    private void rewriteSubSegments(
            List<ManifestFileMeta> section,
            RewriteOutput output,
            List<ManifestFileMeta> sortNewFiles)
            throws Exception {
        List<ManifestFileMeta> subSegment = new ArrayList<>();
        long subSegmentSize = 0;
        for (ManifestFileMeta m : section) {
            subSegmentSize += m.fileSize();
            subSegment.add(m);

            if (subSegmentSize >= suggestedMetaSize) {
                sortAndRewriteSection(subSegment, output, sortNewFiles);
                subSegment.clear();
                subSegmentSize = 0;
            }
        }
        // Flush tail only if delete entries exist or file count >= minCount.
        if (!subSegment.isEmpty()) {
            if (!deleteEntries.isEmpty() || subSegment.size() >= suggestedMinMetaCount) {
                sortAndRewriteSection(subSegment, output, sortNewFiles);
            } else {
                output.addAllUnchanged(subSegment);
            }
        }
    }

    /**
     * Sort and rewrite a section. Dispatches to full or minor compact path.
     *
     * <p>sortNewFiles is the same reference as newFilesForAbort, ensuring newly written files are
     * cleaned up on exception by the caller's catch block.
     */
    private void sortAndRewriteSection(
            List<ManifestFileMeta> section,
            RewriteOutput output,
            List<ManifestFileMeta> sortNewFiles)
            throws Exception {
        // Skip rewrite for single file not in delete-range.
        if (section.size() == 1 && !defaultCompactionMap.getOrDefault(section.get(0), false)) {
            output.addUnchanged(section.get(0));
            return;
        }

        if (fullCompaction) {
            sortAndRewriteFull(section, output, sortNewFiles);
        } else {
            sortAndRewriteMinor(section, output, sortNewFiles);
        }
    }

    /**
     * Full compaction path: read all surviving entries (ADD merged with DELETE), sort them
     * together, and write to output as a single sorted stream.
     */
    private void sortAndRewriteFull(
            List<ManifestFileMeta> section,
            RewriteOutput output,
            List<ManifestFileMeta> sortNewFiles)
            throws Exception {
        // Read surviving ADD entries: filter out entries cancelled by deleteEntries.
        Function<ManifestFileMeta, List<ManifestEntry>> reader =
                meta -> {
                    List<ManifestEntry> batch = new ArrayList<>();
                    for (ManifestEntry entry :
                            manifestFile.read(
                                    meta.fileName(),
                                    meta.fileSize(),
                                    FileEntry.addFilter(),
                                    Filter.alwaysTrue())) {
                        if (!deleteEntries.contains(entry.identifier())) {
                            batch.add(entry);
                        }
                    }
                    return batch;
                };

        List<ManifestEntry> entries = new ArrayList<>();
        for (ManifestEntry entry :
                sequentialBatchedExecute(reader, section, manifestReadParallelism)) {
            entries.add(entry);
        }

        if (!entries.isEmpty()) {
            List<ManifestFileMeta> sorted = sortAndWriteEntries(entries);
            output.addSortedFiles(sorted);
            sortNewFiles.addAll(sorted);
        }
    }

    /**
     * Minor compaction path: read entries with ADD/DELETE classified in a single pass per file,
     * then sort each group independently and write them to output.
     *
     * <p>Each file is read in parallel (via sequentialBatchedExecute). The reader classifies
     * entries into ADD and DELETE within each file, returning a Pair. Results are merged in the
     * main thread.
     */
    private void sortAndRewriteMinor(
            List<ManifestFileMeta> section,
            RewriteOutput output,
            List<ManifestFileMeta> sortNewFiles)
            throws Exception {
        // Read and classify ADD/DELETE in one pass per file.
        Function<ManifestFileMeta, List<Pair<List<ManifestEntry>, List<ManifestEntry>>>> reader =
                meta -> {
                    List<ManifestEntry> addBatch = new ArrayList<>();
                    List<ManifestEntry> deleteBatch = new ArrayList<>();
                    for (ManifestEntry entry :
                            manifestFile.read(meta.fileName(), meta.fileSize())) {
                        if (entry.kind() == FileKind.ADD) {
                            addBatch.add(entry);
                        } else {
                            deleteBatch.add(entry);
                        }
                    }
                    return singletonList(Pair.of(addBatch, deleteBatch));
                };

        List<ManifestEntry> addEntries = new ArrayList<>();
        List<ManifestEntry> minorDeleteEntries = new ArrayList<>();
        for (Pair<List<ManifestEntry>, List<ManifestEntry>> pair :
                sequentialBatchedExecute(reader, section, manifestReadParallelism)) {
            addEntries.addAll(pair.getLeft());
            minorDeleteEntries.addAll(pair.getRight());
        }

        if (!addEntries.isEmpty()) {
            List<ManifestFileMeta> sorted = sortAndWriteEntries(addEntries);
            output.addSortedFiles(sorted);
            sortNewFiles.addAll(sorted);
        }

        if (!minorDeleteEntries.isEmpty()) {
            List<ManifestFileMeta> sorted = sortAndWriteEntries(minorDeleteEntries);
            output.addDeleteFiles(sorted);
            sortNewFiles.addAll(sorted);
        }
    }

    /** Sort entries and write them to a new manifest file with proper error handling. */
    private List<ManifestFileMeta> sortAndWriteEntries(List<ManifestEntry> entries)
            throws Exception {
        entries.sort((a, b) -> compareSortKey(a, b, fieldComparator));
        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                manifestFile.createRollingWriter();
        Exception exception = null;
        try {
            writer.write(entries);
        } catch (Exception e) {
            exception = e;
        } finally {
            if (exception != null) {
                writer.abort();
                throw exception;
            }
            writer.close();
        }
        return writer.result();
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
}
