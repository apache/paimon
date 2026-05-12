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

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Util for merging manifest files. */
public class ManifestFileMerger {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFileMerger.class);

    /**
     * Merge several {@link ManifestFileMeta}s. {@link ManifestEntry}s representing first adding and
     * then deleting the same data file will cancel each other.
     *
     * <p>NOTE: This method is atomic.
     */
    public static List<ManifestFileMeta> merge(
            List<ManifestFileMeta> input,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            long manifestFullCompactionSize,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism) {
        // these are the newly created manifest files, clean them up if exception occurs
        List<ManifestFileMeta> newFilesForAbort = new ArrayList<>();

        try {
            Optional<List<ManifestFileMeta>> fullCompacted =
                    tryFullCompaction(
                            input,
                            newFilesForAbort,
                            manifestFile,
                            suggestedMetaSize,
                            manifestFullCompactionSize,
                            partitionType,
                            manifestReadParallelism);
            return fullCompacted.orElseGet(
                    () ->
                            tryMinorCompaction(
                                    input,
                                    newFilesForAbort,
                                    manifestFile,
                                    suggestedMetaSize,
                                    suggestedMinMetaCount,
                                    manifestReadParallelism));
        } catch (Throwable e) {
            // exception occurs, clean up and rethrow
            for (ManifestFileMeta manifest : newFilesForAbort) {
                manifestFile.delete(manifest.fileName());
            }
            throw new RuntimeException(e);
        }
    }

    private static List<ManifestFileMeta> tryMinorCompaction(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            @Nullable Integer manifestReadParallelism) {
        List<ManifestFileMeta> result = new ArrayList<>();
        List<ManifestFileMeta> candidates = new ArrayList<>();
        long totalSize = 0;
        // merge existing small manifest files
        for (ManifestFileMeta manifest : input) {
            totalSize += manifest.fileSize();
            candidates.add(manifest);
            if (totalSize >= suggestedMetaSize) {
                // reach suggested file size, perform merging and produce new file
                mergeCandidates(
                        candidates,
                        manifestFile,
                        result,
                        newFilesForAbort,
                        manifestReadParallelism);
                candidates.clear();
                totalSize = 0;
            }
        }

        // merge the last bit of manifests if there are too many
        if (candidates.size() >= suggestedMinMetaCount) {
            mergeCandidates(
                    candidates, manifestFile, result, newFilesForAbort, manifestReadParallelism);
        } else {
            result.addAll(candidates);
        }
        return result;
    }

    private static void mergeCandidates(
            List<ManifestFileMeta> candidates,
            ManifestFile manifestFile,
            List<ManifestFileMeta> result,
            List<ManifestFileMeta> newMetas,
            @Nullable Integer manifestReadParallelism) {
        if (candidates.size() == 1) {
            result.add(candidates.get(0));
            return;
        }

        Map<FileEntry.Identifier, ManifestEntry> map = new LinkedHashMap<>();
        FileEntry.mergeEntries(manifestFile, candidates, map, manifestReadParallelism);
        if (!map.isEmpty()) {
            List<ManifestFileMeta> merged = manifestFile.write(new ArrayList<>(map.values()));
            result.addAll(merged);
            newMetas.addAll(merged);
        }
    }

    public static Optional<List<ManifestFileMeta>> trySortCompaction(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            RowType partitionType,
            int rewriteManifestCount,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        checkArgument(
                rewriteManifestCount > 0,
                "Manifest sort rewrite manifest count must be greater than 0.");

        if (partitionType.getFieldCount() == 0 || input.size() <= 1) {
            return Optional.empty();
        }

        for (ManifestFileMeta file : input) {
            if (file.numDeletedFiles() > 0) {
                return Optional.empty();
            }
        }

        RecordComparator partitionComparator =
                CodeGenUtils.newRecordComparator(partitionType.getFieldTypes());
        List<ManifestFileMeta> compactCandidates = compactCandidates(input, partitionComparator);
        if (compactCandidates.size() <= 1) {
            return Optional.empty();
        }

        List<ManifestSortedRun> runs = partitionSortedRuns(compactCandidates, partitionComparator);
        if (runs.size() <= 1) {
            return Optional.empty();
        }

        List<ManifestSortedRun> pickedRuns = pickRuns(runs, rewriteManifestCount);
        if (pickedRuns.isEmpty()) {
            return Optional.empty();
        }

        List<ManifestFileMeta> pickedFiles = new ArrayList<>();
        for (ManifestSortedRun run : pickedRuns) {
            pickedFiles.addAll(run.files);
        }
        List<List<ManifestFileMeta>> rewriteGroups =
                rewriteGroups(pickedFiles, input, partitionComparator);
        LinkedHashSet<String> rewriteFileNames = new LinkedHashSet<>();
        for (List<ManifestFileMeta> rewriteGroup : rewriteGroups) {
            addFileNames(rewriteGroup, rewriteFileNames);
        }
        if (rewriteFileNames.size() <= 1) {
            return Optional.empty();
        }

        LOG.info(
                "Start Manifest File Sort Compaction: sortedRuns: {}, pickedRuns: {}, pickedFiles: {}",
                runs.size(),
                pickedRuns.size(),
                pickedFiles.size());

        int insertPos = -1;
        for (int i = 0; i < input.size(); i++) {
            ManifestFileMeta file = input.get(i);
            if (rewriteFileNames.contains(file.fileName())) {
                if (insertPos < 0) {
                    insertPos = i;
                }
            }
        }

        List<ManifestFileMeta> rewritten = new ArrayList<>();
        for (List<ManifestFileMeta> rewriteGroup : rewriteGroups) {
            Map<FileEntry.Identifier, ManifestEntry> map = new LinkedHashMap<>();
            FileEntry.mergeEntries(manifestFile, rewriteGroup, map, manifestReadParallelism);
            List<ManifestEntry> entries = new ArrayList<>(map.values());
            sortEntriesByPartition(entries, partitionComparator);
            if (!entries.isEmpty()) {
                List<ManifestFileMeta> groupRewritten = manifestFile.write(entries);
                rewritten.addAll(groupRewritten);
                newFilesForAbort.addAll(groupRewritten);
            }
        }

        List<ManifestFileMeta> result = new ArrayList<>();
        for (int i = 0; i < input.size(); i++) {
            if (i == insertPos) {
                result.addAll(rewritten);
            }
            ManifestFileMeta file = input.get(i);
            if (!rewriteFileNames.contains(file.fileName())) {
                result.add(file);
            }
        }
        return Optional.of(result);
    }

    private static List<ManifestFileMeta> compactCandidates(
            List<ManifestFileMeta> input, RecordComparator partitionComparator) {
        List<ManifestFileMeta> result = new ArrayList<>();
        for (ManifestFileMeta file : input) {
            if (hasMultiplePartitions(file, partitionComparator)) {
                result.add(file);
            }
        }
        return result;
    }

    private static List<ManifestSortedRun> partitionSortedRuns(
            List<ManifestFileMeta> input, RecordComparator partitionComparator) {
        List<ManifestFileMeta> files = new ArrayList<>(input);
        files.sort(
                (left, right) -> {
                    int result =
                            partitionComparator.compare(minPartition(left), minPartition(right));
                    return result == 0
                            ? partitionComparator.compare(maxPartition(left), maxPartition(right))
                            : result;
                });

        PriorityQueue<ManifestSortedRunBuilder> queue =
                new PriorityQueue<>(
                        (left, right) ->
                                partitionComparator.compare(
                                        maxPartition(left.last()), maxPartition(right.last())));
        for (ManifestFileMeta file : files) {
            ManifestSortedRunBuilder run = queue.poll();
            if (run == null) {
                queue.add(new ManifestSortedRunBuilder(file));
            } else if (partitionComparator.compare(minPartition(file), maxPartition(run.last()))
                    > 0) {
                run.add(file);
                queue.add(run);
            } else {
                queue.add(new ManifestSortedRunBuilder(file));
                queue.add(run);
            }
        }

        List<ManifestSortedRun> runs = new ArrayList<>();
        for (ManifestSortedRunBuilder builder : queue) {
            runs.add(builder.toRun());
        }
        runs.sort(
                (left, right) ->
                        partitionComparator.compare(
                                minPartition(left.files.get(0)), minPartition(right.files.get(0))));
        return runs;
    }

    private static List<ManifestSortedRun> pickRuns(
            List<ManifestSortedRun> runs, int rewriteManifestCount) {
        List<ManifestSortedRun> sortedRuns = new ArrayList<>(runs);
        sortedRuns.sort(
                Comparator.comparingLong(ManifestSortedRun::totalFileSize)
                        .thenComparingInt(ManifestSortedRun::fileCount));

        ManifestSortedRun firstRun = sortedRuns.get(0);
        if (firstRun.fileCount() > rewriteManifestCount) {
            return Collections.emptyList();
        }

        List<ManifestSortedRun> result = new ArrayList<>();
        result.add(firstRun);
        long candidateSize = firstRun.totalFileSize();
        int candidateFileCount = firstRun.fileCount();
        for (int i = 1; i < sortedRuns.size(); i++) {
            ManifestSortedRun next = sortedRuns.get(i);
            if (candidateSize < next.totalFileSize()
                    || candidateFileCount + next.fileCount() > rewriteManifestCount) {
                break;
            }
            result.add(next);
            candidateSize += next.totalFileSize();
            candidateFileCount += next.fileCount();
        }
        return result.size() > 1 ? result : Collections.emptyList();
    }

    private static List<List<ManifestFileMeta>> rewriteGroups(
            List<ManifestFileMeta> rewriteFiles,
            List<ManifestFileMeta> input,
            RecordComparator partitionComparator) {
        rewriteFiles = new ArrayList<>(rewriteFiles);
        rewriteFiles.sort(
                (left, right) -> {
                    int result =
                            partitionComparator.compare(minPartition(left), minPartition(right));
                    return result == 0
                            ? partitionComparator.compare(maxPartition(left), maxPartition(right))
                            : result;
                });

        List<Set<String>> groupNames = new ArrayList<>();
        LinkedHashSet<String> currentNames = new LinkedHashSet<>();
        BinaryRow currentMax = null;
        for (ManifestFileMeta file : rewriteFiles) {
            if (!currentNames.isEmpty()
                    && partitionComparator.compare(minPartition(file), currentMax) > 0) {
                groupNames.add(currentNames);
                currentNames = new LinkedHashSet<>();
                currentMax = null;
            }
            currentNames.add(file.fileName());
            if (currentMax == null
                    || partitionComparator.compare(maxPartition(file), currentMax) > 0) {
                currentMax = maxPartition(file);
            }
        }
        if (!currentNames.isEmpty()) {
            groupNames.add(currentNames);
        }

        List<List<ManifestFileMeta>> result = new ArrayList<>();
        for (Set<String> names : groupNames) {
            List<ManifestFileMeta> group = new ArrayList<>();
            for (ManifestFileMeta file : input) {
                if (names.contains(file.fileName())) {
                    group.add(file);
                }
            }
            if (group.size() > 1) {
                result.add(group);
            }
        }
        return result;
    }

    private static void sortEntriesByPartition(
            List<ManifestEntry> entries, RecordComparator partitionComparator) {
        entries.sort(
                (left, right) -> {
                    int result = partitionComparator.compare(left.partition(), right.partition());
                    if (result != 0) {
                        return result;
                    }

                    result = Integer.compare(left.bucket(), right.bucket());
                    if (result != 0) {
                        return result;
                    }

                    result = Integer.compare(left.level(), right.level());
                    if (result != 0) {
                        return result;
                    }

                    return left.fileName().compareTo(right.fileName());
                });
    }

    private static boolean hasMultiplePartitions(
            ManifestFileMeta file, RecordComparator partitionComparator) {
        return partitionComparator.compare(minPartition(file), maxPartition(file)) < 0;
    }

    private static BinaryRow minPartition(ManifestFileMeta file) {
        return file.partitionStats().minValues();
    }

    private static BinaryRow maxPartition(ManifestFileMeta file) {
        return file.partitionStats().maxValues();
    }

    private static void addFileNames(List<ManifestFileMeta> files, Set<String> names) {
        for (ManifestFileMeta file : files) {
            names.add(file.fileName());
        }
    }

    public static Optional<List<ManifestFileMeta>> tryFullCompaction(
            List<ManifestFileMeta> inputs,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            long sizeTrigger,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        checkArgument(sizeTrigger > 0, "Manifest full compaction size trigger cannot be zero.");

        // 1. should trigger full compaction

        Filter<ManifestFileMeta> mustChange =
                file -> file.numDeletedFiles() > 0 || file.fileSize() < suggestedMetaSize;
        long totalManifestSize = 0;
        long deltaDeleteFileNum = 0;
        long totalDeltaFileSize = 0;
        for (ManifestFileMeta file : inputs) {
            totalManifestSize += file.fileSize();
            if (mustChange.test(file)) {
                totalDeltaFileSize += file.fileSize();
                deltaDeleteFileNum += file.numDeletedFiles();
            }
        }

        if (totalDeltaFileSize < sizeTrigger) {
            return Optional.empty();
        }

        // 2. do full compaction

        LOG.info(
                "Start Manifest File Full Compaction: totalManifestSize: {}, deltaDeleteFileNum {}, totalDeltaFileSize {}",
                totalManifestSize,
                deltaDeleteFileNum,
                totalDeltaFileSize);

        // 2.1. read all delete entries

        Set<FileEntry.Identifier> deleteEntries =
                FileEntry.readDeletedEntries(manifestFile, inputs, manifestReadParallelism);

        // 2.2. try to skip base files by partition filter

        PartitionPredicate predicate;
        if (deleteEntries.isEmpty()) {
            predicate = PartitionPredicate.ALWAYS_FALSE;
        } else {
            if (partitionType.getFieldCount() > 0) {
                Set<BinaryRow> deletePartitions = computeDeletePartitions(deleteEntries);
                predicate = PartitionPredicate.fromMultiple(partitionType, deletePartitions);
            } else {
                predicate = PartitionPredicate.ALWAYS_TRUE;
            }
        }

        List<ManifestFileMeta> result = new ArrayList<>();
        List<ManifestFileMeta> toBeMerged = new LinkedList<>(inputs);

        if (predicate != null) {
            Iterator<ManifestFileMeta> iterator = toBeMerged.iterator();
            while (iterator.hasNext()) {
                ManifestFileMeta file = iterator.next();
                if (mustChange.test(file)) {
                    continue;
                }
                if (!predicate.test(
                        file.numAddedFiles() + file.numDeletedFiles(),
                        file.partitionStats().minValues(),
                        file.partitionStats().maxValues(),
                        file.partitionStats().nullCounts())) {
                    iterator.remove();
                    result.add(file);
                }
            }
        }

        // 2.2. merge

        if (toBeMerged.size() <= 1) {
            return Optional.empty();
        }

        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                manifestFile.createRollingWriter();
        Function<ManifestFileMeta, List<FullCompactionReadResult>> reader =
                file ->
                        singletonList(
                                readForFullCompaction(
                                        file, manifestFile, mustChange, deleteEntries));
        Exception exception = null;
        try {
            for (FullCompactionReadResult readResult :
                    sequentialBatchedExecute(reader, toBeMerged, manifestReadParallelism)) {
                if (readResult.requireChange) {
                    writer.write(readResult.entries);
                } else {
                    result.add(readResult.file);
                }
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

        List<ManifestFileMeta> merged = writer.result();
        result.addAll(merged);
        newFilesForAbort.addAll(merged);
        return Optional.of(result);
    }

    private static FullCompactionReadResult readForFullCompaction(
            ManifestFileMeta file,
            ManifestFile manifestFile,
            Filter<ManifestFileMeta> mustChange,
            Set<FileEntry.Identifier> deleteEntries) {
        List<ManifestEntry> entries = new ArrayList<>();
        boolean requireChange = mustChange.test(file);
        for (ManifestEntry entry :
                manifestFile.read(
                        file.fileName(),
                        file.fileSize(),
                        FileEntry.addFilter(),
                        Filter.alwaysTrue())) {
            if (deleteEntries.contains(entry.identifier())) {
                requireChange = true;
            } else {
                entries.add(entry);
            }
        }

        return new FullCompactionReadResult(file, requireChange, entries);
    }

    private static Set<BinaryRow> computeDeletePartitions(Set<FileEntry.Identifier> deleteEntries) {
        Set<BinaryRow> partitions = new HashSet<>();
        for (FileEntry.Identifier identifier : deleteEntries) {
            partitions.add(identifier.partition);
        }
        return partitions;
    }

    private static class FullCompactionReadResult {

        private final ManifestFileMeta file;
        private final boolean requireChange;
        private final List<ManifestEntry> entries;

        private FullCompactionReadResult(
                ManifestFileMeta file, boolean requireChange, List<ManifestEntry> entries) {
            this.file = file;
            this.requireChange = requireChange;
            this.entries = entries;
        }
    }

    private static class ManifestSortedRun {

        private final List<ManifestFileMeta> files;
        private final long totalFileSize;

        private ManifestSortedRun(List<ManifestFileMeta> files) {
            this.files = files;
            long totalFileSize = 0;
            for (ManifestFileMeta file : files) {
                totalFileSize += file.fileSize();
            }
            this.totalFileSize = totalFileSize;
        }

        private int fileCount() {
            return files.size();
        }

        private long totalFileSize() {
            return totalFileSize;
        }
    }

    private static class ManifestSortedRunBuilder {

        private final List<ManifestFileMeta> files;

        private ManifestSortedRunBuilder(ManifestFileMeta file) {
            this.files = new ArrayList<>();
            this.files.add(file);
        }

        private void add(ManifestFileMeta file) {
            files.add(file);
        }

        private ManifestFileMeta last() {
            return files.get(files.size() - 1);
        }

        private ManifestSortedRun toRun() {
            return new ManifestSortedRun(files);
        }
    }
}
