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

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.manifest.CompactFileIdentifierSet;
import org.apache.paimon.manifest.DeletedRowIdSet;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestAvroReader;
import org.apache.paimon.manifest.ManifestAvroReader.RawBlock;
import org.apache.paimon.manifest.ManifestAvroReader.RowIterator;
import org.apache.paimon.manifest.ManifestAvroWriter.EncodedBlockMeta;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionDictionary;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Streaming merge of the naturally sorted runs in data-evolution manifest files. */
final class ManifestEntryRunMerge {

    private static final int FRAGMENTED_RUN_THRESHOLD = 64;
    private static final long MAX_IN_MEMORY_FRAGMENTED_ENTRIES = 25_000L;
    private static final int MAX_STREAM_CURSORS = 128;
    private static final int MAX_STREAM_READ_AMPLIFICATION = 8;

    private ManifestEntryRunMerge() {}

    /**
     * Returns null when the input is too fragmented for a bounded streaming merge. The caller must
     * fall back to the spillable external sorter in that case.
     */
    @Nullable
    static List<ManifestFileMeta> sortAndWriteFullEntries(
            List<ManifestFileMeta> section,
            ManifestFileSorter.RowIdEntrySortKey sortKey,
            RowType partitionType,
            ManifestFile manifestFile,
            List<ManifestFileMeta> newFilesForAbort,
            CompactFileIdentifierSet deletedIdentifiers,
            DeletedRowIdSet deletedRowIds,
            int maxNumFileHandles,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        ManifestEntryRunMergeEntry.Filter filter =
                new ManifestEntryRunMergeEntry.Filter(deletedIdentifiers, deletedRowIds, true);
        ManifestEntryRunMergePlan plan =
                discoverRuns(
                        section,
                        sortKey,
                        partitionType,
                        manifestFile,
                        filter,
                        maxNumFileHandles,
                        manifestReadParallelism);
        if (plan == null) {
            return null;
        }
        return plan.mergeToManifest(sortKey, manifestFile, filter, newFilesForAbort);
    }

    /**
     * Returns null when the input is too fragmented for a bounded streaming merge or primitive
     * manifest reading is unavailable. The caller must fall back to the spillable external sorter.
     */
    @Nullable
    static Pair<List<ManifestFileMeta>, List<ManifestFileMeta>> sortAndWriteMinorEntries(
            List<ManifestFileMeta> section,
            ManifestFileSorter.RowIdEntrySortKey sortKey,
            RowType partitionType,
            ManifestFile manifestFile,
            List<ManifestFileMeta> newFilesForAbort,
            int maxNumFileHandles,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        CompactFileIdentifierSet deletedIdentifiers = new CompactFileIdentifierSet();
        DeletedRowIdSet deletedRowIds = new DeletedRowIdSet();
        ManifestEntryRunMergeEntry.Filter.Minor filter =
                new ManifestEntryRunMergeEntry.Filter.Minor(
                        deletedIdentifiers, deletedRowIds, true);
        try {
            ManifestEntryRunMergePlan plan;
            try {
                plan =
                        discoverRuns(
                                section,
                                sortKey,
                                partitionType,
                                manifestFile,
                                filter,
                                maxNumFileHandles,
                                manifestReadParallelism);
            } finally {
                deletedRowIds.releaseRangeIndex();
            }
            if (plan == null) {
                return null;
            }
            return plan.mergeMinorToManifest(
                    sortKey,
                    manifestFile,
                    filter,
                    deletedIdentifiers,
                    deletedRowIds,
                    newFilesForAbort);
        } finally {
            deletedIdentifiers.release();
        }
    }

    @Nullable
    private static ManifestEntryRunMergePlan discoverRuns(
            List<ManifestFileMeta> section,
            ManifestFileSorter.RowIdEntrySortKey sortKey,
            RowType partitionType,
            ManifestFile manifestFile,
            ManifestEntryRunMergeEntry.Filter filter,
            int maxNumFileHandles,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        PartitionDictionary partitions = new PartitionDictionary(sortKey::comparePartitions);
        List<ManifestEntryRunMergePlan.Source.Spec> sources = new ArrayList<>();
        int streamCursorCount = 0;
        long inMemoryEntries = 0;
        List<Discovery.DiscoveredManifest> discovered = new ArrayList<>(section.size());
        if (section.size() <= 1
                || (manifestReadParallelism != null && manifestReadParallelism <= 1)) {
            for (ManifestFileMeta meta : section) {
                ManifestEntryRunMergeEntry.Filter discoveryFilter = filter.forDiscovery();
                Discovery.DiscoveredManifest manifest =
                        discoverManifestRuns(
                                meta, manifestFile, partitionType, partitions, discoveryFilter);
                if (manifest.requiresExternalSort) {
                    return null;
                }
                filter.combine(discoveryFilter);
                discovered.add(manifest);
            }
        } else {
            Function<
                            ManifestFileMeta,
                            List<
                                    Pair<
                                            Discovery.DiscoveredManifest,
                                            ManifestEntryRunMergeEntry.Filter>>>
                    reader =
                            meta -> {
                                try {
                                    ManifestEntryRunMergeEntry.Filter discoveryFilter =
                                            filter.forDiscovery();
                                    return Collections.singletonList(
                                            Pair.of(
                                                    discoverManifestRuns(
                                                            meta,
                                                            manifestFile,
                                                            partitionType,
                                                            partitions,
                                                            discoveryFilter),
                                                    discoveryFilter));
                                } catch (Exception e) {
                                    throw new RuntimeException(
                                            "Failed to discover sorted Avro runs in "
                                                    + meta.fileName(),
                                            e);
                                }
                            };
            for (Pair<Discovery.DiscoveredManifest, ManifestEntryRunMergeEntry.Filter> scan :
                    sequentialBatchedExecute(reader, section, manifestReadParallelism)) {
                if (scan.getLeft().requiresExternalSort) {
                    return null;
                }
                filter.combine(scan.getRight());
                discovered.add(scan.getLeft());
            }
        }
        for (int manifestIndex = 0; manifestIndex < section.size(); manifestIndex++) {
            ManifestFileMeta meta = section.get(manifestIndex);
            Discovery.DiscoveredManifest manifest = discovered.get(manifestIndex);
            if (manifest.fragmented) {
                long entryCount = meta.numAddedFiles() + meta.numDeletedFiles();
                inMemoryEntries += entryCount;
                if (inMemoryEntries > MAX_IN_MEMORY_FRAGMENTED_ENTRIES) {
                    return null;
                }
                sources.add(new ManifestEntryRunMergePlan.Source.FragmentedManifestSpec(meta));
                streamCursorCount++;
            } else {
                sources.addAll(manifest.runs);
                streamCursorCount += manifest.runs.size();
            }
            if (streamCursorCount > Math.min(MAX_STREAM_CURSORS, maxNumFileHandles)) {
                return null;
            }
        }
        partitions.finish();
        for (Discovery.DiscoveredManifest manifest : discovered) {
            manifest.finishFiltering(filter);
            manifest.updatePartitionRanks(partitions);
        }
        return new ManifestEntryRunMergePlan(sources, partitions);
    }

    private static Discovery.DiscoveredManifest discoverManifestRuns(
            ManifestFileMeta meta,
            ManifestFile manifestFile,
            RowType partitionType,
            PartitionDictionary partitions,
            ManifestEntryRunMergeEntry.Filter filter)
            throws Exception {
        try (ManifestAvroReader reader =
                manifestFile.scanAvroBlocks(meta.fileName(), meta.fileSize())) {
            return discoverManifestRuns(meta, reader, partitionType, partitions, filter);
        } catch (UnsupportedOperationException unsupported) {
            return Discovery.DiscoveredManifest.requiresExternalSort();
        } finally {
            filter.releaseIdentifier();
        }
    }

    private static Discovery.DiscoveredManifest discoverManifestRuns(
            ManifestFileMeta meta,
            ManifestAvroReader reader,
            RowType partitionType,
            PartitionDictionary partitions,
            ManifestEntryRunMergeEntry.Filter filter)
            throws Exception {
        SimpleStatsConverter partitionStatsConverter = new SimpleStatsConverter(partitionType);
        List<ManifestEntryRunMergePlan.Source.ManifestRunSpec> runs = new ArrayList<>();
        List<Discovery.BlockInfo> blocks = new ArrayList<>();
        ManifestEntryRunMergeEntry.Key previous = new ManifestEntryRunMergeEntry.Key();
        ManifestEntryRunMergeEntry.Key current = new ManifestEntryRunMergeEntry.Key();
        boolean hasPrevious = false;
        long runStart = 0;
        long position = 0;
        long entryCount = meta.numAddedFiles() + meta.numDeletedFiles();
        boolean fragmented = false;
        ProjectedManifestEntry entry = ProjectedManifestEntry.ENTRY_LAYOUT_PROJECTION.createEntry();
        while (reader.hasNext()) {
            RawBlock rawBlock = reader.next();
            RowIterator rows =
                    rawBlock.toRows(ProjectedManifestEntry.ENTRY_LAYOUT_PROJECTION.projectedType());
            while (rows.hasNext()) {
                entry.replace(rows.next());
                current.replace(entry, partitions);
                filter.observe(entry, current);
                if (fragmented) {
                    position++;
                    continue;
                }
                if (rows.recordIndex() == 0) {
                    blocks.add(
                            new Discovery.BlockInfo(
                                    rawBlock.blockOrdinal(),
                                    position,
                                    rawBlock.rawBlockCopySupported(),
                                    current.stableCopy(),
                                    partitionType));
                }
                Discovery.BlockInfo block = blocks.get(blocks.size() - 1);
                block.collectForSort(entry, current, partitions, filter);
                boolean inversion =
                        hasPrevious && compareDiscoveryKeys(previous, current, partitions) > 0;
                if (inversion) {
                    if (rows.recordIndex() > 0) {
                        block.sorted = false;
                    }
                    runs.add(
                            new ManifestEntryRunMergePlan.Source.ManifestRunSpec(
                                    meta, runStart, position, blocks));
                    runStart = position;
                    if (runs.size() >= FRAGMENTED_RUN_THRESHOLD) {
                        if (entryCount > MAX_IN_MEMORY_FRAGMENTED_ENTRIES) {
                            return Discovery.DiscoveredManifest.requiresExternalSort();
                        }
                        fragmented = true;
                        runs.clear();
                        blocks.clear();
                        position++;
                        continue;
                    }
                }
                position++;
                if (rows.recordIndex() + 1 == rawBlock.recordCount()) {
                    ManifestEntryRunMergeEntry.Key stableLastKey = current.stableCopy();
                    block.finishSort(position, stableLastKey, partitionStatsConverter);
                    previous.copyFrom(stableLastKey);
                } else {
                    previous.copyFrom(current);
                }
                hasPrevious = true;
            }
        }
        if (fragmented) {
            return Discovery.DiscoveredManifest.fragmented();
        }
        if (position > runStart) {
            runs.add(
                    new ManifestEntryRunMergePlan.Source.ManifestRunSpec(
                            meta, runStart, position, blocks));
        }
        if (exceedsStreamingReadAmplification(runs, blocks.size())) {
            return entryCount > MAX_IN_MEMORY_FRAGMENTED_ENTRIES
                    ? Discovery.DiscoveredManifest.requiresExternalSort()
                    : Discovery.DiscoveredManifest.fragmented();
        }
        return Discovery.DiscoveredManifest.runs(runs, blocks);
    }

    private static boolean exceedsStreamingReadAmplification(
            List<ManifestEntryRunMergePlan.Source.ManifestRunSpec> runs, int blockCount) {
        if (runs.size() <= 1 || blockCount == 0) {
            return false;
        }

        long prefixBlocksRead = 0;
        for (ManifestEntryRunMergePlan.Source.ManifestRunSpec run : runs) {
            prefixBlocksRead += run.prefixBlockCount();
        }
        return prefixBlocksRead > (long) blockCount * MAX_STREAM_READ_AMPLIFICATION;
    }

    private static int compareDiscoveryKeys(
            ManifestEntryRunMergeEntry.Key left,
            ManifestEntryRunMergeEntry.Key right,
            PartitionDictionary partitions) {
        return compareRemainingKeys(
                left, right, partitions.compareIds(left.partitionId, right.partitionId));
    }

    static int compareMergeKeys(
            ManifestEntryRunMergeEntry.Key left, ManifestEntryRunMergeEntry.Key right) {
        return compareRemainingKeys(
                left, right, Integer.compare(left.partitionRank, right.partitionRank));
    }

    private static int compareRemainingKeys(
            ManifestEntryRunMergeEntry.Key left,
            ManifestEntryRunMergeEntry.Key right,
            int comparison) {
        if (comparison == 0) {
            comparison = Byte.compare(left.kind, right.kind);
        }
        if (comparison == 0) {
            comparison = Long.compare(left.firstRowId, right.firstRowId);
        }
        if (comparison == 0) {
            comparison = Long.compare(left.rangeEnd, right.rangeEnd);
        }
        if (comparison == 0) {
            comparison = Long.compare(left.reverseSequence, right.reverseSequence);
        }
        if (comparison == 0) {
            comparison = compareBytes(left, right);
        }
        return comparison;
    }

    private static int compareBytes(
            ManifestEntryRunMergeEntry.Key left, ManifestEntryRunMergeEntry.Key right) {
        int minLength = Math.min(left.fileNameLength, right.fileNameLength);
        for (int i = 0; i < minLength; i++) {
            int leftByte =
                    MemorySegmentUtils.getByte(left.fileNameSegments, left.fileNameOffset + i)
                            & 0xFF;
            int rightByte =
                    MemorySegmentUtils.getByte(right.fileNameSegments, right.fileNameOffset + i)
                            & 0xFF;
            if (leftByte != rightByte) {
                return leftByte - rightByte;
            }
        }
        return left.fileNameLength - right.fileNameLength;
    }

    /** Results and Avro block metadata collected while discovering natural manifest runs. */
    static final class Discovery {

        private Discovery() {}

        static final class DiscoveredManifest {

            final List<ManifestEntryRunMergePlan.Source.ManifestRunSpec> runs;
            final List<BlockInfo> blocks;
            final boolean fragmented;
            final boolean requiresExternalSort;

            DiscoveredManifest(
                    List<ManifestEntryRunMergePlan.Source.ManifestRunSpec> runs,
                    List<BlockInfo> blocks,
                    boolean fragmented,
                    boolean requiresExternalSort) {
                this.runs = runs;
                this.blocks = blocks;
                this.fragmented = fragmented;
                this.requiresExternalSort = requiresExternalSort;
            }

            static DiscoveredManifest runs(
                    List<ManifestEntryRunMergePlan.Source.ManifestRunSpec> runs,
                    List<BlockInfo> blocks) {
                return new DiscoveredManifest(runs, blocks, false, false);
            }

            static DiscoveredManifest fragmented() {
                return new DiscoveredManifest(
                        Collections.emptyList(), Collections.emptyList(), true, false);
            }

            static DiscoveredManifest requiresExternalSort() {
                return new DiscoveredManifest(
                        Collections.emptyList(), Collections.emptyList(), false, true);
            }

            void updatePartitionRanks(PartitionDictionary partitions) {
                for (BlockInfo block : blocks) {
                    block.updatePartitionRanks(partitions);
                }
            }

            void finishFiltering(ManifestEntryRunMergeEntry.Filter filter) {
                for (BlockInfo block : blocks) {
                    block.finishFiltering(filter);
                }
            }
        }

        static final class BlockInfo {

            final long ordinal;
            final long start;
            final @Nullable ManifestEntryRunMergeEntry.Key firstKey;
            boolean eligible;
            boolean sorted = true;
            long end;
            @Nullable ManifestEntryRunMergeEntry.Key lastKey;
            long addedFiles;
            long deletedFiles;
            long schemaId = Long.MIN_VALUE;
            int minBucket = Integer.MAX_VALUE;
            int maxBucket = Integer.MIN_VALUE;
            int minLevel = Integer.MAX_VALUE;
            int maxLevel = Integer.MIN_VALUE;
            long minRowId = Long.MAX_VALUE;
            long maxRowId = Long.MIN_VALUE;
            final boolean singleFieldSortedPartitionStats;
            @Nullable SimpleStatsCollector partitionStats;
            final RowType partitionType;
            @Nullable BinaryRow nullPartition;
            long nullPartitionCount;
            @Nullable BinaryRow minNonNullPartition;
            @Nullable BinaryRow maxNonNullPartition;
            EncodedBlockMeta metadata;

            BlockInfo(
                    long ordinal,
                    long start,
                    boolean eligible,
                    ManifestEntryRunMergeEntry.Key firstKey,
                    RowType partitionType) {
                this.ordinal = ordinal;
                this.start = start;
                this.eligible = eligible;
                this.firstKey = firstKey;
                this.partitionType = partitionType;
                this.singleFieldSortedPartitionStats =
                        eligible && firstKey != null && partitionType.getFieldCount() == 1;
                this.partitionStats =
                        eligible && firstKey != null && !singleFieldSortedPartitionStats
                                ? new SimpleStatsCollector(partitionType)
                                : null;
            }

            void collectForSort(
                    ProjectedManifestEntry entry,
                    ManifestEntryRunMergeEntry.Key key,
                    PartitionDictionary partitions,
                    ManifestEntryRunMergeEntry.Filter filter) {
                if (!eligible) {
                    return;
                }
                if (!filter.copyable(entry, key)) {
                    eligible = false;
                    releasePartitionStats();
                    return;
                }
                collectEntryStats(entry, key);
                BinaryRow partition = partitions.partition(key.partitionId);
                if (singleFieldSortedPartitionStats) {
                    if (partition.isNullAt(0)) {
                        nullPartition = partition;
                        nullPartitionCount++;
                    } else {
                        if (minNonNullPartition == null) {
                            minNonNullPartition = partition;
                        }
                        maxNonNullPartition = partition;
                    }
                } else {
                    partitionStats.collect(partition);
                }
            }

            private void collectEntryStats(
                    ProjectedManifestEntry entry, ManifestEntryRunMergeEntry.Key key) {
                if (key.kind == FileKind.ADD.toByteValue()) {
                    addedFiles++;
                } else {
                    deletedFiles++;
                }
                schemaId = Math.max(schemaId, entry.file().schemaId());
                int bucket = entry.bucket();
                minBucket = Math.min(minBucket, bucket);
                maxBucket = Math.max(maxBucket, bucket);
                int level = entry.file().level();
                minLevel = Math.min(minLevel, level);
                maxLevel = Math.max(maxLevel, level);
                minRowId = Math.min(minRowId, key.firstRowId);
                maxRowId = Math.max(maxRowId, key.rangeEnd);
            }

            void finishSort(
                    long end,
                    ManifestEntryRunMergeEntry.Key lastKey,
                    SimpleStatsConverter partitionStatsConverter) {
                this.end = end;
                this.lastKey = lastKey;
                if (eligible && sorted) {
                    SimpleStats encodedPartitionStats;
                    if (singleFieldSortedPartitionStats) {
                        BinaryRow min =
                                minNonNullPartition == null ? nullPartition : minNonNullPartition;
                        BinaryRow max =
                                maxNonNullPartition == null ? nullPartition : maxNonNullPartition;
                        checkState(min != null && max != null, "Manifest block has no partition.");
                        encodedPartitionStats =
                                new SimpleStats(
                                        min,
                                        max,
                                        BinaryArray.fromLongArray(new Long[] {nullPartitionCount}));
                    } else {
                        checkState(
                                partitionStats != null, "Manifest block has no partition stats.");
                        encodedPartitionStats =
                                partitionStatsConverter.toBinaryAllMode(partitionStats.extract());
                    }
                    metadata =
                            new EncodedBlockMeta(
                                    addedFiles,
                                    deletedFiles,
                                    schemaId,
                                    minBucket,
                                    maxBucket,
                                    minLevel,
                                    maxLevel,
                                    minRowId,
                                    maxRowId,
                                    encodedPartitionStats);
                }
                releasePartitionStats();
            }

            private void releasePartitionStats() {
                partitionStats = null;
                nullPartition = null;
                minNonNullPartition = null;
                maxNonNullPartition = null;
            }

            boolean copyable(long runStart, long runEnd) {
                return metadata != null && start >= runStart && end <= runEnd;
            }

            void finishFiltering(ManifestEntryRunMergeEntry.Filter filter) {
                if (metadata != null && !filter.copyableAfterDiscovery(minRowId, maxRowId)) {
                    metadata = null;
                }
            }

            void updatePartitionRanks(PartitionDictionary partitions) {
                checkState(firstKey != null && lastKey != null, "Manifest block has no sort keys.");
                firstKey.partitionRank = partitions.rank(firstKey.partitionId);
                lastKey.partitionRank = partitions.rank(lastKey.partitionId);
            }
        }
    }
}
