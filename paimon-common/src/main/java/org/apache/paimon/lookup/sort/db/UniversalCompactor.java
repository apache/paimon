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

package org.apache.paimon.lookup.sort.db;

import org.apache.paimon.lookup.sort.SortLookupStoreFactory;
import org.apache.paimon.lookup.sort.SortLookupStoreReader;
import org.apache.paimon.lookup.sort.SortLookupStoreWriter;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.sst.BlockIterator;
import org.apache.paimon.sst.SstFileReader;
import org.apache.paimon.utils.BloomFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.function.Predicate;

import static org.apache.paimon.lookup.sort.db.LocalKvDb.UNKNOWN_NUM_ENTRIES;
import static org.apache.paimon.lookup.sort.db.LocalKvDb.isTombstone;

/**
 * Handles Universal Compaction for the LSM-Tree, inspired by RocksDB's Universal Compaction.
 *
 * <p>Universal Compaction treats all SST files as a flat list of sorted runs ordered from newest to
 * oldest. Instead of compacting level-by-level, it picks contiguous runs to merge based on size
 * ratios between adjacent runs.
 *
 * <p>Compaction trigger: when the number of level-0 files reaches {@code
 * level0FileNumCompactionTrigger}.
 *
 * <p>Run selection (from newest to oldest):
 *
 * <ol>
 *   <li><b>Size-ratio based</b>: Starting from the newest run, accumulate runs as long as {@code
 *       accumulatedSize / nextRunSize < sizeRatio%}. If at least 2 runs are accumulated, merge
 *       them.
 *   <li><b>Fallback</b>: If size-ratio selection finds fewer than 2 candidates, merge all runs.
 * </ol>
 *
 * <p>Tombstones (empty values) are only removed when all runs are being merged (equivalent to
 * compacting to the bottom level).
 */
public class UniversalCompactor {

    private static final Logger LOG = LoggerFactory.getLogger(UniversalCompactor.class);
    private static final byte[] TOMBSTONE = new byte[0];

    private final Comparator<MemorySlice> keyComparator;
    private final SortLookupStoreFactory storeFactory;
    private final LongFunction<BloomFilter.Builder> bloomFilterBuilderFactory;
    private final long maxOutputFileSize;
    private final int level0FileNumCompactTrigger;
    private final int sizeRatioPercent;
    @Nullable private final Predicate<byte[]> expiredValuePredicate;
    @Nullable private final LocalKvDb.MergeOperator mergeOperator;
    private final FileDeleter fileDeleter;

    public UniversalCompactor(
            Comparator<MemorySlice> keyComparator,
            SortLookupStoreFactory storeFactory,
            LongFunction<BloomFilter.Builder> bloomFilterBuilderFactory,
            long maxOutputFileSize,
            int level0FileNumCompactTrigger,
            int sizeRatioPercent,
            @Nullable Predicate<byte[]> expiredValuePredicate,
            @Nullable LocalKvDb.MergeOperator mergeOperator,
            FileDeleter fileDeleter) {
        this.keyComparator = keyComparator;
        this.storeFactory = storeFactory;
        this.bloomFilterBuilderFactory = bloomFilterBuilderFactory;
        this.maxOutputFileSize = maxOutputFileSize;
        this.level0FileNumCompactTrigger = level0FileNumCompactTrigger;
        this.sizeRatioPercent = sizeRatioPercent;
        this.expiredValuePredicate = expiredValuePredicate;
        this.mergeOperator = mergeOperator;
        this.fileDeleter = fileDeleter;
    }

    /**
     * Check if compaction is needed and perform it if so.
     *
     * <p>All sorted runs are collected from all levels into a flat list ordered newest-first. If
     * the number of level-0 files reaches {@code level0FileNumCompactionTrigger}, a compaction is
     * triggered.
     *
     * <p>When the size-ratio branch is chosen, the candidate range is guaranteed to include all
     * level-0 runs and the level-1 run (if present). This ensures level 0 is fully cleared after
     * compaction and the merged result can be placed into level 1 without overflow.
     *
     * @param levels the multi-level SST file storage (used as a flat sorted-run list)
     * @param maxLevels the maximum number of levels (only level 0 is used for sorted runs)
     * @param fileSupplier supplier for new SST file paths
     */
    public void maybeCompact(
            List<List<SstFileMetadata>> levels, int maxLevels, FileSupplier fileSupplier)
            throws IOException {
        int levelZeroFileCount = levels.get(0).size();
        if (levelZeroFileCount < level0FileNumCompactTrigger) {
            return;
        }

        List<SortedRun> sortedRuns = collectSortedRuns(levels, maxLevels);

        LOG.info(
                "Universal compaction triggered: {} L0 files (threshold: {}), {} total sorted runs",
                levelZeroFileCount,
                level0FileNumCompactTrigger,
                sortedRuns.size());

        // Try size-ratio based selection: accumulate runs from newest (index 0) to oldest
        int candidateEnd = pickSizeRatioCandidates(sortedRuns);

        // Ensure all L0 runs and the L1 run (if present) are included in the merge.
        int minCandidateEnd = 0;
        for (SortedRun run : sortedRuns) {
            if (!run.files.isEmpty() && run.files.get(0).getLevel() <= 1) {
                minCandidateEnd++;
            } else {
                break;
            }
        }
        candidateEnd = Math.max(candidateEnd, minCandidateEnd);

        if (candidateEnd >= 2) {
            // Merge runs [0, candidateEnd)
            LOG.info(
                    "Size-ratio compaction: merging {} newest runs out of {}",
                    candidateEnd,
                    sortedRuns.size());
            List<SortedRun> toMerge = new ArrayList<>(sortedRuns.subList(0, candidateEnd));
            List<SortedRun> remaining =
                    new ArrayList<>(sortedRuns.subList(candidateEnd, sortedRuns.size()));
            boolean dropTombstones = remaining.isEmpty();

            // Find the highest level not occupied by remaining runs
            int outputLevel = findHighestFreeLevel(levels, maxLevels, toMerge);
            MergeResult mergeResult =
                    mergeSortedRuns(toMerge, dropTombstones, fileSupplier, outputLevel);

            // Clear levels used by merged runs and place merged result
            clearLevelsOfRuns(levels, toMerge);
            levels.get(outputLevel).addAll(mergeResult.mergedRun.files);
            deleteOldFiles(toMerge, mergeResult.skippedFiles);
        } else {
            // Fallback: merge all runs
            LOG.info("Fallback full compaction: merging all {} runs", sortedRuns.size());
            mergeAllRuns(levels, maxLevels, sortedRuns, fileSupplier);
        }
    }

    /**
     * Force a full compaction of all sorted runs into a single run.
     *
     * @param levels the multi-level SST file storage
     * @param maxLevels the maximum number of levels
     * @param fileSupplier supplier for new SST file paths
     */
    public void fullCompact(
            List<List<SstFileMetadata>> levels, int maxLevels, FileSupplier fileSupplier)
            throws IOException {
        List<SortedRun> sortedRuns = collectSortedRuns(levels, maxLevels);
        if (sortedRuns.isEmpty() || (sortedRuns.size() == 1 && expiredValuePredicate == null)) {
            return;
        }

        LOG.info("Full compaction: merging all {} sorted runs", sortedRuns.size());
        mergeAllRuns(levels, maxLevels, sortedRuns, fileSupplier);
    }

    /**
     * Merge all sorted runs into a single run, placing the result at the highest level. Tombstones
     * are dropped since there are no older runs below.
     */
    private void mergeAllRuns(
            List<List<SstFileMetadata>> levels,
            int maxLevels,
            List<SortedRun> sortedRuns,
            FileSupplier fileSupplier)
            throws IOException {
        int outputLevel = maxLevels - 1;
        MergeResult mergeResult = mergeSortedRuns(sortedRuns, true, fileSupplier, outputLevel);

        for (int i = 0; i < maxLevels; i++) {
            levels.get(i).clear();
        }
        levels.get(outputLevel).addAll(mergeResult.mergedRun.files);
        deleteOldFiles(sortedRuns, mergeResult.skippedFiles);
    }

    // -------------------------------------------------------------------------
    //  Universal Compaction internals
    // -------------------------------------------------------------------------

    /**
     * Collect all SST files into a flat list of sorted runs, ordered newest-first.
     *
     * <p>Level 0 files are each treated as an individual sorted run (keys may overlap). Level 1+
     * files within the same level form a single sorted run (keys do not overlap within a level).
     */
    private List<SortedRun> collectSortedRuns(List<List<SstFileMetadata>> levels, int maxLevels) {
        List<SortedRun> runs = new ArrayList<>();

        // Level 0: each file is its own sorted run, newest first (L0 files are stored newest-first)
        List<SstFileMetadata> levelZeroFiles = levels.get(0);
        for (SstFileMetadata file : levelZeroFiles) {
            List<SstFileMetadata> singleFile = new ArrayList<>();
            singleFile.add(file);
            runs.add(new SortedRun(singleFile));
        }

        // Level 1+: all files in a level form one sorted run
        for (int level = 1; level < maxLevels; level++) {
            List<SstFileMetadata> levelFiles = levels.get(level);
            if (!levelFiles.isEmpty()) {
                runs.add(new SortedRun(new ArrayList<>(levelFiles)));
            }
        }

        return runs;
    }

    /**
     * Pick candidates for size-ratio based compaction.
     *
     * <p>Starting from the newest run (index 0), accumulate runs as long as: {@code accumulatedSize
     * / nextRunSize * 100 < sizeRatioPercent}.
     *
     * @return the number of runs to merge (from index 0). Returns 0 or 1 if no suitable candidates.
     */
    private int pickSizeRatioCandidates(List<SortedRun> sortedRuns) {
        if (sortedRuns.size() < 2) {
            return 0;
        }

        long accumulatedSize = sortedRuns.get(0).totalSize();
        int candidateCount = 1;

        for (int i = 1; i < sortedRuns.size(); i++) {
            long nextRunSize = sortedRuns.get(i).totalSize();

            // Check: accumulatedSize / nextRunSize * 100 < sizeRatioPercent
            // Rewritten to avoid floating point: accumulatedSize * 100 < sizeRatioPercent *
            // nextRunSize
            if (accumulatedSize * 100 < (long) sizeRatioPercent * nextRunSize) {
                accumulatedSize += nextRunSize;
                candidateCount++;
            } else {
                break;
            }
        }

        return candidateCount;
    }

    /**
     * Merge multiple sorted runs into a single sorted run using a min-heap based multi-way merge.
     *
     * @param runsToMerge the sorted runs to merge (ordered newest-first)
     * @param dropTombstones whether to drop tombstone entries
     * @param fileSupplier supplier for new SST file paths
     * @param outputLevel the level to assign to all output files
     * @return the merged sorted run with output level set on all files
     */
    private MergeResult mergeSortedRuns(
            List<SortedRun> runsToMerge,
            boolean dropTombstones,
            FileSupplier fileSupplier,
            int outputLevel)
            throws IOException {

        List<SstFileMetadata> allFiles = new ArrayList<>();
        for (SortedRun run : runsToMerge) {
            allFiles.addAll(run.files);
        }

        long totalInputSize = 0;
        for (SstFileMetadata meta : allFiles) {
            totalInputSize += meta.getFileSize();
        }
        LOG.info(
                "Starting merge: {} runs, {} total files, {} total bytes, dropTombstones={}",
                runsToMerge.size(),
                allFiles.size(),
                totalInputSize,
                dropTombstones);

        long mergeStartTime = System.currentTimeMillis();

        List<List<SstFileMetadata>> groups = groupFilesByKeyOverlap(allFiles);
        List<List<SstFileMetadata>> mergedGroups = mergeSmallAdjacentGroups(groups);
        Map<File, Integer> fileToRunSequence = buildFileToRunSequenceMap(runsToMerge);

        List<SstFileMetadata> outputFiles = new ArrayList<>();
        Set<File> skippedFileSet = new HashSet<>();
        int skippedGroupCount = 0;
        int mergedGroupCount = 0;

        if (mergeOperator == null) {
            for (List<SstFileMetadata> group : mergedGroups) {
                if (group.size() == 1) {
                    SstFileMetadata singleFile = group.get(0);
                    boolean canSkip =
                            expiredValuePredicate == null
                                    && (!dropTombstones || !singleFile.hasTombstones());
                    if (canSkip) {
                        SstFileMetadata promoted = singleFile.withLevel(outputLevel);
                        outputFiles.add(promoted);
                        skippedFileSet.add(promoted.getFile());
                        skippedGroupCount++;
                        continue;
                    }
                }

                mergedGroupCount++;
                outputFiles.addAll(
                        mergeFileGroup(
                                group,
                                dropTombstones,
                                fileSupplier,
                                fileToRunSequence,
                                outputLevel,
                                null));
            }
        } else {
            CompactionSstOutput output =
                    new CompactionSstOutput(outputFiles, fileSupplier, outputLevel, dropTombstones);
            try {
                for (List<SstFileMetadata> group : mergedGroups) {
                    mergedGroupCount++;
                    mergeFileGroup(
                            group,
                            dropTombstones,
                            fileSupplier,
                            fileToRunSequence,
                            outputLevel,
                            output);
                }
                output.finish();
            } catch (IOException | RuntimeException | Error e) {
                output.abort(e);
                throw e;
            }
        }

        outputFiles.sort((a, b) -> keyComparator.compare(a.getMinKey(), b.getMinKey()));

        long mergeElapsedMs = System.currentTimeMillis() - mergeStartTime;
        long totalOutputSize = 0;
        for (SstFileMetadata meta : outputFiles) {
            totalOutputSize += meta.getFileSize();
        }
        LOG.info(
                "Merge completed in {} ms: {} input files ({} bytes) -> {} output files "
                        + "({} bytes), {} groups merged, {} groups skipped",
                mergeElapsedMs,
                allFiles.size(),
                totalInputSize,
                outputFiles.size(),
                totalOutputSize,
                mergedGroupCount,
                skippedGroupCount);

        return new MergeResult(new SortedRun(outputFiles), skippedFileSet, outputLevel);
    }

    /**
     * Group files into connected components by key-range overlap using a sweep-line algorithm.
     * Files are sorted by minKey, then adjacent files whose key ranges overlap are placed in the
     * same group.
     */
    private List<List<SstFileMetadata>> groupFilesByKeyOverlap(List<SstFileMetadata> allFiles) {
        List<SstFileMetadata> sortedFiles = new ArrayList<>(allFiles);
        sortedFiles.sort((a, b) -> keyComparator.compare(a.getMinKey(), b.getMinKey()));

        List<List<SstFileMetadata>> groups = new ArrayList<>();
        List<SstFileMetadata> currentGroup = new ArrayList<>();
        MemorySlice currentGroupMaxKey = null;

        for (SstFileMetadata file : sortedFiles) {
            if (currentGroup.isEmpty()) {
                currentGroup.add(file);
                currentGroupMaxKey = file.getMaxKey();
            } else if (keyComparator.compare(file.getMinKey(), currentGroupMaxKey) <= 0) {
                currentGroup.add(file);
                if (keyComparator.compare(file.getMaxKey(), currentGroupMaxKey) > 0) {
                    currentGroupMaxKey = file.getMaxKey();
                }
            } else {
                groups.add(currentGroup);
                currentGroup = new ArrayList<>();
                currentGroup.add(file);
                currentGroupMaxKey = file.getMaxKey();
            }
        }
        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }
        return groups;
    }

    /**
     * Merge small adjacent groups to avoid producing too many small files. If either the pending
     * group or the current group is smaller than half of {@link #maxOutputFileSize}, they are
     * combined into a single group.
     */
    private List<List<SstFileMetadata>> mergeSmallAdjacentGroups(
            List<List<SstFileMetadata>> groups) {
        long smallFileThreshold = maxOutputFileSize / 2;
        List<List<SstFileMetadata>> mergedGroups = new ArrayList<>();
        List<SstFileMetadata> pendingGroup = null;

        for (List<SstFileMetadata> group : groups) {
            if (pendingGroup == null) {
                pendingGroup = new ArrayList<>(group);
            } else {
                long pendingSize = groupTotalSize(pendingGroup);
                long groupSize = groupTotalSize(group);
                if (pendingSize < smallFileThreshold || groupSize < smallFileThreshold) {
                    pendingGroup.addAll(group);
                } else {
                    mergedGroups.add(pendingGroup);
                    pendingGroup = new ArrayList<>(group);
                }
            }
        }
        if (pendingGroup != null) {
            mergedGroups.add(pendingGroup);
        }
        return mergedGroups;
    }

    /**
     * Build a mapping from each file to its run sequence number. Older runs receive lower sequence
     * numbers so that during dedup the newest entry (highest sequence) wins.
     */
    private static Map<File, Integer> buildFileToRunSequenceMap(List<SortedRun> runsToMerge) {
        Map<File, Integer> fileToRunSequence = new HashMap<>();
        for (int runIdx = 0; runIdx < runsToMerge.size(); runIdx++) {
            int sequence = runsToMerge.size() - 1 - runIdx;
            for (SstFileMetadata meta : runsToMerge.get(runIdx).files) {
                fileToRunSequence.put(meta.getFile(), sequence);
            }
        }
        return fileToRunSequence;
    }

    private static long groupTotalSize(List<SstFileMetadata> group) {
        long size = 0;
        for (SstFileMetadata meta : group) {
            size += meta.getFileSize();
        }
        return size;
    }

    /**
     * Merge a group of files using min-heap based multi-way merge.
     *
     * @param group the files to merge (may be from different runs)
     * @param dropTombstones whether to drop tombstone entries
     * @param fileSupplier supplier for new SST file paths
     * @param fileToRunSequence maps each file to its run sequence number for dedup ordering
     * @param outputLevel the level to assign to output files
     * @param sharedOutput optional output shared across file groups to preserve merge state
     * @return the list of merged output files
     */
    private List<SstFileMetadata> mergeFileGroup(
            List<SstFileMetadata> group,
            boolean dropTombstones,
            FileSupplier fileSupplier,
            Map<File, Integer> fileToRunSequence,
            int outputLevel,
            @Nullable CompactionSstOutput sharedOutput)
            throws IOException {

        // Sort files by run sequence (older first) for correct dedup ordering
        List<SstFileMetadata> orderedFiles = new ArrayList<>(group);
        orderedFiles.sort(
                (a, b) -> {
                    int seqA = fileToRunSequence.getOrDefault(a.getFile(), 0);
                    int seqB = fileToRunSequence.getOrDefault(b.getFile(), 0);
                    return Integer.compare(seqA, seqB);
                });

        List<SstFileMetadata> result = new ArrayList<>();
        List<SortLookupStoreReader> openReaders = new ArrayList<>();
        PriorityQueue<MergeEntry> minHeap =
                new PriorityQueue<>(
                        (a, b) -> {
                            int keyCompare = keyComparator.compare(a.key, b.key);
                            if (keyCompare != 0) {
                                return keyCompare;
                            }
                            return Integer.compare(b.sequence, a.sequence);
                        });

        CompactionSstOutput output =
                sharedOutput == null
                        ? new CompactionSstOutput(result, fileSupplier, outputLevel, dropTombstones)
                        : sharedOutput;
        Throwable failure = null;
        try {
            for (int seq = 0; seq < orderedFiles.size(); seq++) {
                SortLookupStoreReader reader =
                        storeFactory.createReader(orderedFiles.get(seq).getFile());
                openReaders.add(reader);
                SstFileReader.SstFileIterator fileIterator = reader.createIterator();
                MergeSource source = new MergeSource(fileIterator, seq);
                if (source.advance()) {
                    minHeap.add(source.currentEntry());
                }
            }
            MemorySlice previousKey = null;

            while (!minHeap.isEmpty()) {
                MergeEntry entry = minHeap.poll();

                if (previousKey != null && keyComparator.compare(entry.key, previousKey) == 0) {
                    if (entry.source.advance()) {
                        minHeap.add(entry.source.currentEntry());
                    }
                    continue;
                }

                previousKey = entry.key;

                if (entry.source.advance()) {
                    minHeap.add(entry.source.currentEntry());
                }

                output.put(entry.key, entry.value);
            }

            if (sharedOutput == null) {
                output.finish();
            }
        } catch (IOException | RuntimeException | Error e) {
            failure = e;
            output.abort(e);
            throw e;
        } finally {
            IOException closeFailure = null;
            for (SortLookupStoreReader reader : openReaders) {
                try {
                    reader.close();
                } catch (IOException e) {
                    if (closeFailure == null) {
                        closeFailure = e;
                    } else {
                        closeFailure.addSuppressed(e);
                    }
                }
            }
            if (closeFailure != null) {
                if (failure == null) {
                    throw closeFailure;
                }
                failure.addSuppressed(closeFailure);
            }
        }

        return result;
    }

    /**
     * Find the highest level that is either empty or occupied by one of the runs being merged.
     * Starts from {@code maxLevels - 1} and works downward, stopping at level 1 minimum (level 0 is
     * reserved for new flushes).
     */
    private static int findHighestFreeLevel(
            List<List<SstFileMetadata>> levels, int maxLevels, List<SortedRun> runsBeingMerged) {
        Set<Integer> mergedLevels = new HashSet<>();
        for (SortedRun run : runsBeingMerged) {
            for (SstFileMetadata file : run.files) {
                mergedLevels.add(file.getLevel());
            }
        }

        // Start from the highest level and work down; a level is usable if it is empty or
        // all its files belong to the runs being merged
        for (int level = maxLevels - 1; level >= 1; level--) {
            if (levels.get(level).isEmpty() || mergedLevels.contains(level)) {
                return level;
            }
        }
        return 1;
    }

    /** Clear the level lists for all levels that contain files from the given runs. */
    private static void clearLevelsOfRuns(
            List<List<SstFileMetadata>> levels, List<SortedRun> runs) {
        Set<Integer> levelsToClear = new HashSet<>();
        for (SortedRun run : runs) {
            for (SstFileMetadata file : run.files) {
                levelsToClear.add(file.getLevel());
            }
        }
        for (int level : levelsToClear) {
            if (level >= 0 && level < levels.size()) {
                levels.get(level).clear();
            }
        }
    }

    /** Writes compacted records, combining adjacent values before enforcing output file sizes. */
    private final class CompactionSstOutput {

        private final List<SstFileMetadata> result;
        private final FileSupplier fileSupplier;
        private final int outputLevel;
        private final boolean dropTombstones;
        private final RecordCombiningWriter combiningWriter;

        @Nullable private SortLookupStoreWriter currentWriter;
        @Nullable private File currentSstFile;
        @Nullable private MemorySlice currentFileMinKey;
        @Nullable private MemorySlice currentFileMaxKey;
        private long currentBatchSize;
        private long currentTombstoneCount;

        private CompactionSstOutput(
                List<SstFileMetadata> result,
                FileSupplier fileSupplier,
                int outputLevel,
                boolean dropTombstones) {
            this.result = result;
            this.fileSupplier = fileSupplier;
            this.outputLevel = outputLevel;
            this.dropTombstones = dropTombstones;
            this.combiningWriter =
                    new RecordCombiningWriter(mergeOperator, this::writeCombinedRecord);
        }

        private void put(MemorySlice key, byte[] value) throws IOException {
            combiningWriter.put(key, value);
        }

        private void finish() throws IOException {
            combiningWriter.finish();
            closeCurrentWriter();
        }

        private void abort(Throwable failure) {
            if (currentWriter != null) {
                try {
                    currentWriter.close();
                } catch (IOException suppressed) {
                    failure.addSuppressed(suppressed);
                }
                currentWriter = null;
            }
        }

        private void writeCombinedRecord(MemorySlice key, byte[] value) throws IOException {
            // Evaluate expiration after combining so a TTL-aware merge can refresh the result.
            boolean tombstone = isTombstone(value);
            boolean expired =
                    !tombstone
                            && expiredValuePredicate != null
                            && expiredValuePredicate.test(value);
            if (dropTombstones && (tombstone || expired)) {
                return;
            }
            byte[] outputValue = expired ? TOMBSTONE : value;

            if (currentWriter == null) {
                currentSstFile = fileSupplier.newSstFile();
                currentWriter =
                        storeFactory.createWriter(
                                currentSstFile,
                                bloomFilterBuilderFactory.apply(UNKNOWN_NUM_ENTRIES));
                currentFileMinKey = key;
                currentBatchSize = 0;
                currentTombstoneCount = 0;
            }

            currentWriter.put(key.copyBytes(), outputValue);
            currentFileMaxKey = key;
            currentBatchSize += key.length() + outputValue.length;
            if (isTombstone(outputValue)) {
                currentTombstoneCount++;
            }

            if (currentBatchSize >= maxOutputFileSize) {
                closeCurrentWriter();
            }
        }

        private void closeCurrentWriter() throws IOException {
            if (currentWriter == null) {
                return;
            }
            currentWriter.close();
            result.add(
                    new SstFileMetadata(
                            currentSstFile,
                            currentFileMinKey,
                            currentFileMaxKey,
                            currentTombstoneCount,
                            outputLevel));
            currentWriter = null;
            currentSstFile = null;
            currentFileMinKey = null;
            currentFileMaxKey = null;
        }
    }

    /** Delete old SST files from the merged sorted runs, skipping files that were preserved. */
    private void deleteOldFiles(List<SortedRun> oldRuns, Set<File> skippedFiles) {
        for (SortedRun run : oldRuns) {
            for (SstFileMetadata meta : run.files) {
                if (!skippedFiles.contains(meta.getFile())) {
                    fileDeleter.deleteFile(meta.getFile());
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    //  Sorted Run
    // -------------------------------------------------------------------------

    /** Result of a merge operation, containing the merged run, skipped files and output level. */
    private static final class MergeResult {
        final SortedRun mergedRun;
        final Set<File> skippedFiles;
        final int outputLevel;

        MergeResult(SortedRun mergedRun, Set<File> skippedFiles, int outputLevel) {
            this.mergedRun = mergedRun;
            this.skippedFiles = skippedFiles;
            this.outputLevel = outputLevel;
        }
    }

    /** A sorted run is a list of SST files whose key ranges do not overlap. */
    static final class SortedRun {

        final List<SstFileMetadata> files;

        SortedRun(List<SstFileMetadata> files) {
            this.files = files;
        }

        long totalSize() {
            long size = 0;
            for (SstFileMetadata meta : files) {
                size += meta.getFileSize();
            }
            return size;
        }
    }

    // -------------------------------------------------------------------------
    //  Merge helpers
    // -------------------------------------------------------------------------

    /**
     * Wraps an SST file iterator as a merge source, lazily reading blocks and entries. Each source
     * has a sequence number to resolve key conflicts (higher sequence = newer data).
     */
    private static final class MergeSource {

        private final SstFileReader.SstFileIterator fileIterator;
        private final int sequence;
        private BlockIterator currentBlock;
        private MergeEntry current;

        MergeSource(SstFileReader.SstFileIterator fileIterator, int sequence) throws IOException {
            this.fileIterator = fileIterator;
            this.sequence = sequence;
            this.currentBlock = fileIterator.readBatch();
        }

        boolean advance() throws IOException {
            while (true) {
                if (currentBlock != null && currentBlock.hasNext()) {
                    Map.Entry<MemorySlice, MemorySlice> entry = currentBlock.next();
                    current =
                            new MergeEntry(
                                    entry.getKey(), entry.getValue().copyBytes(), sequence, this);
                    return true;
                }
                currentBlock = fileIterator.readBatch();
                if (currentBlock == null) {
                    current = null;
                    return false;
                }
            }
        }

        MergeEntry currentEntry() {
            return current;
        }
    }

    /** A single key-value entry from a merge source, used as a heap element. */
    private static final class MergeEntry {

        final MemorySlice key;
        final byte[] value;
        final int sequence;
        final MergeSource source;

        MergeEntry(MemorySlice key, byte[] value, int sequence, MergeSource source) {
            this.key = key;
            this.value = value;
            this.sequence = sequence;
            this.source = source;
        }
    }

    /** Functional interface for supplying new SST file paths. */
    public interface FileSupplier {
        File newSstFile();
    }

    /** Functional interface for deleting SST files, allowing callers to clean up resources. */
    public interface FileDeleter {
        void deleteFile(File file);
    }
}
