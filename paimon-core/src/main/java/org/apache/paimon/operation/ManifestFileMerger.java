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
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.options.Options;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Manifest file merger with standard merge logic and optional sort rewrite. */
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
            RowType partitionType,
            CoreOptions options) {
        // Extract configuration from options
        long suggestedMetaSize = options.manifestTargetSize().getBytes();
        int suggestedMinMetaCount = options.manifestMergeMinCount();
        long manifestFullCompactionSize = options.manifestFullCompactionThresholdSize().getBytes();
        Integer manifestReadParallelism = options.scanManifestParallelism();
        Options tableOptions = options.toConfiguration();

        // these are the newly created manifest files, clean them up if exception occurs
        List<ManifestFileMeta> newFilesForAbort = new ArrayList<>();

        try {
            Optional<List<ManifestFileMeta>> merged;

            // If manifest-sort.enable is enabled and there are partition fields, use trySortRewrite
            if (tableOptions.getBoolean("manifest-sort.enable", false)
                    && partitionType.getFieldCount() > 0) {
                merged =
                        trySortRewrite(
                                input, newFilesForAbort, manifestFile, partitionType, options);
            } else {
                // Otherwise try full compaction first, then minor compaction if needed
                merged =
                        tryFullCompaction(
                                input,
                                newFilesForAbort,
                                manifestFile,
                                suggestedMetaSize,
                                manifestFullCompactionSize,
                                partitionType,
                                manifestReadParallelism);
            }

            return merged.orElseGet(
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

    // ==================== Manifest Sort Rewrite ====================

    /**
     * Try to sort-rewrite the merged manifest list by a configured partition field. If the sort
     * field cannot be resolved or the delta file size is below the full compaction threshold, the
     * input is returned as-is.
     */
    private static Optional<List<ManifestFileMeta>> trySortRewrite(
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
        Options tableOptions = options.toConfiguration();

        // Step 1: Resolve sort field.
        String sortField = resolveSortField(tableOptions.toMap(), partitionType);
        if (sortField == null) {
            LOG.warn(
                    "Cannot resolve sort field for manifest sort rewrite. "
                            + "Skipping sort. Configure 'manifest-sort.partition-field'"
                            + " for multi-partition tables.");
            return Optional.of(input);
        }
        int sortFieldIndex = partitionType.getFieldNames().indexOf(sortField);
        DataType sortFieldType = partitionType.getTypeAt(sortFieldIndex);

        // Step 2: Check full compact trigger.
        Filter<ManifestFileMeta> mustChange =
                file -> file.numDeletedFiles() > 0 || file.fileSize() < suggestedMetaSize;

        long totalDeltaFileSize = 0;
        for (ManifestFileMeta file : input) {
            if (mustChange.test(file)) {
                totalDeltaFileSize += file.fileSize();
            }
        }

        List<ManifestFileMeta> fullCompactionManifests = new ArrayList<>();
        List<ManifestFileMeta> lsmFiles = new LinkedList<>(input);
        Set<FileEntry.Identifier> deleteEntries = null;
        if (totalDeltaFileSize >= manifestFullCompactionSize) {
            // Step 3: Read delete entries and build partition predicate.
            deleteEntries =
                    FileEntry.readDeletedEntries(manifestFile, input, manifestReadParallelism);

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

            // Step 4: Classify input into level0 runs and LSM files.
            Iterator<ManifestFileMeta> iterator = lsmFiles.iterator();
            while (iterator.hasNext()) {
                ManifestFileMeta file = iterator.next();
                if (mustChange.test(file)) {
                    iterator.remove();
                    fullCompactionManifests.add(file);
                } else if (predicate != null
                        && predicate.test(
                                file.numAddedFiles() + file.numDeletedFiles(),
                                file.partitionStats().minValues(),
                                file.partitionStats().maxValues(),
                                file.partitionStats().nullCounts())) {
                    iterator.remove();
                    fullCompactionManifests.add(file);
                }
            }
        }

        // Process full compaction manifests separately: sort, deduplicate, and rewrite
        List<ManifestFileMeta> fullCompactionRewritten = new ArrayList<>();
        if (!fullCompactionManifests.isEmpty()) {
            fullCompactionRewritten =
                    sortAndRewriteFullCompaction(
                            fullCompactionManifests,
                            manifestFile,
                            sortFieldIndex,
                            sortFieldType,
                            suggestedMetaSize,
                            deleteEntries);
            newFilesForAbort.addAll(fullCompactionRewritten);
        }

        // Step 5: Build LSM Tree and assign levels (only for lsmFiles).
        List<ManifestSortedRun> levelRuns =
                lsmFiles.isEmpty()
                        ? new ArrayList<>()
                        : buildLevelSortedRuns(lsmFiles, sortFieldIndex, sortFieldType);

        // Step 6: Pick runs to compact.
        int sizeAmpThreshold = tableOptions.getInteger("manifest-sort.size-amp-threshold", 2);
        int sizeRatioThreshold = tableOptions.getInteger("manifest-sort.size-ratio-threshold", 10);
        ManifestPickStrategy pickStrategy =
                new ManifestPickStrategy(sizeAmpThreshold, sizeRatioThreshold);
        List<ManifestSortedRun> pickedRuns = pickStrategy.pick(levelRuns);

        Set<ManifestSortedRun> pickedSet = new HashSet<>(pickedRuns);
        List<ManifestFileMeta> reusedFiles = new ArrayList<>();
        for (ManifestSortedRun run : levelRuns) {
            if (!pickedSet.contains(run)) {
                reusedFiles.addAll(run.files());
            }
        }
        List<ManifestFileMeta> result = new ArrayList<>(reusedFiles);
        if (pickedRuns.isEmpty()) {
            result.addAll(fullCompactionRewritten);
            return Optional.of(new ArrayList<>(result));
        }

        // Step 7: Split picked files into sections, sort and rewrite each.
        List<ManifestFileMeta> pickedFiles = new ArrayList<>();
        for (ManifestSortedRun run : pickedRuns) {
            pickedFiles.addAll(run.files());
        }

        List<List<ManifestFileMeta>> sections =
                splitIntoSections(pickedFiles, sortFieldIndex, sortFieldType);
        long maxRewriteSize =
                parseLongOption(tableOptions, "manifest-sort.max-rewrite-size", Long.MAX_VALUE);
        long processedSize = 0;


        List<ManifestFileMeta> sortNewFiles = new ArrayList<>();
        for (List<ManifestFileMeta> section : sections) {
            long sectionSize = 0;
            for (ManifestFileMeta m : section) {
                sectionSize += m.fileSize();
            }
            if (processedSize + sectionSize > maxRewriteSize) {
                result.addAll(section);
                continue;
            }
            processedSize += sectionSize;

            List<ManifestFileMeta> merged =
                    sortAndRewriteSection(
                            section, manifestFile, sortFieldIndex, sortFieldType, null);
            sortNewFiles.addAll(merged);
            result.addAll(merged);
        }
        newFilesForAbort.addAll(sortNewFiles);
        result.addAll(fullCompactionRewritten);
        return Optional.of(result);
    }

    // ==================== Sort Rewrite Helpers ====================

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
        return a.file().fileName().compareTo(b.file().fileName());
    }

    /**
     * Resolve the partition field to sort manifests by.
     *
     * <p>Resolution rules:
     *
     * <ol>
     *   <li>If {@code manifest-sort.partition-field} is configured, return that value.
     *   <li>Otherwise, if the table has exactly one partition field, return that field name.
     *   <li>Otherwise return {@code null}.
     * </ol>
     */
    @Nullable
    static String resolveSortField(Map<String, String> tableOptions, RowType partitionType) {
        String configured = tableOptions.get("manifest-sort.partition-field");
        if (configured != null && !configured.isEmpty()) {
            return configured;
        }
        if (partitionType.getFieldCount() == 1) {
            return partitionType.getFieldNames().get(0);
        }
        return null;
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
                    > 0) {
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
                runs.get(i).setLevel(n - i);
            } else {
                runs.get(i).setLevel(0);
            }
        }
        return runs;
    }

    /**
     * Split picked files into sections. Files with overlapping sort-key intervals go into the same
     * section.
     */
    static List<List<ManifestFileMeta>> splitIntoSections(
            List<ManifestFileMeta> pickedFiles, int sortFieldIndex, DataType sortFieldType) {
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

        List<List<ManifestFileMeta>> sections = new ArrayList<>();
        List<ManifestFileMeta> currentSection = new ArrayList<>();
        currentSection.add(pickedFiles.get(0));
        BinaryRow sectionMaxBound = pickedFiles.get(0).partitionStats().maxValues();
        for (int i = 1; i < pickedFiles.size(); i++) {
            ManifestFileMeta file = pickedFiles.get(i);
            if (compareField(
                            file.partitionStats().minValues(),
                            sectionMaxBound,
                            sortFieldIndex,
                            sortFieldType)
                    > 0) {
                sections.add(currentSection);
                currentSection = new ArrayList<>();
                currentSection.add(file);
                sectionMaxBound = file.partitionStats().maxValues();
            } else {
                currentSection.add(file);
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
        sections.add(currentSection);
        return sections;
    }

    /**
     * Sort and rewrite full compaction manifests. Files are sorted by min partition value, then
     * processed in batches. A batch stops when total size reaches threshold or when current max
     * doesn't overlap with next min. Each batch is sorted, deduplicated (DELETE entries removed),
     * and written to new manifest files.
     */
    private static List<ManifestFileMeta> sortAndRewriteFullCompaction(
            List<ManifestFileMeta> fullCompactionManifests,
            ManifestFile manifestFile,
            int sortFieldIndex,
            DataType sortFieldType,
            long suggestedMetaSize,
            @Nullable Set<FileEntry.Identifier> deletedIdentifiers)
            throws Exception {

        // Sort by min partition value
        fullCompactionManifests.sort(
                (a, b) ->
                        compareField(
                                a.partitionStats().minValues(),
                                b.partitionStats().minValues(),
                                sortFieldIndex,
                                sortFieldType));

        List<ManifestFileMeta> result = new ArrayList<>();
        List<ManifestFileMeta> batch = new ArrayList<>();
        long batchSize = 0;

        for (int i = 0; i < fullCompactionManifests.size(); i++) {
            ManifestFileMeta current = fullCompactionManifests.get(i);
            boolean shouldFlush = false;

            // Check if batch size reaches threshold
            if (batchSize + current.fileSize() >= suggestedMetaSize && !batch.isEmpty()) {
                shouldFlush = true;
            }

            // Check if current max overlaps with next min
            if (i < fullCompactionManifests.size() - 1 && !batch.isEmpty()) {
                ManifestFileMeta next = fullCompactionManifests.get(i + 1);
                int cmp =
                        compareField(
                                current.partitionStats().maxValues(),
                                next.partitionStats().minValues(),
                                sortFieldIndex,
                                sortFieldType);
                if (cmp < 0) {
                    shouldFlush = true;
                }
            }

            batch.add(current);
            batchSize += current.fileSize();

            if (shouldFlush || i == fullCompactionManifests.size() - 1) {
                // Process batch: sort entries, remove DELETE, write out
                List<ManifestFileMeta> rewritten =
                        sortAndRewriteSection(
                                batch,
                                manifestFile,
                                sortFieldIndex,
                                sortFieldType,
                                deletedIdentifiers);
                result.addAll(rewritten);
                batch.clear();
                batchSize = 0;
            }
        }

        return result;
    }

    /**
     * Read all entries from a section's manifest files, sort them in memory by the specified
     * partition field, filter out DELETE entries and cancelled ADD entries, then write surviving
     * entries to the rolling writer.
     */
    private static List<ManifestFileMeta> sortAndRewriteSection(
            List<ManifestFileMeta> section,
            ManifestFile manifestFile,
            int sortFieldIndex,
            DataType sortFieldType,
            @Nullable Set<FileEntry.Identifier> deletedIdentifiers)
            throws Exception {

        List<ManifestEntry> allEntries = new ArrayList<>();
        for (ManifestFileMeta meta : section) {
            allEntries.addAll(manifestFile.read(meta.fileName(), meta.fileSize()));
        }

        allEntries.sort((a, b) -> compareSortKey(a, b, sortFieldIndex, sortFieldType));

        Set<FileEntry.Identifier> safeDeletedIds =
                deletedIdentifiers != null ? deletedIdentifiers : new HashSet<>();

        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                manifestFile.createRollingWriter();
        try {
            for (ManifestEntry entry : allEntries) {
                if (entry.kind() == FileKind.ADD && !safeDeletedIds.contains(entry.identifier())) {
                    writer.write(entry);
                }
            }
        } finally {
            writer.close();
        }
        return writer.result();
    }

    /** Parse a long option from table options with a default value. */
    private static long parseLongOption(Options options, String key, long defaultValue) {
        String value = options.get(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            LOG.warn(
                    "Invalid long value '{}' for option '{}', using default {}.",
                    value,
                    key,
                    defaultValue);
            return defaultValue;
        }
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
}
