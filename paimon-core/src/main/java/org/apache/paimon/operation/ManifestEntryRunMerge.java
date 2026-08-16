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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.manifest.CollectedDeletes;
import org.apache.paimon.manifest.FileEntry.ReusableIdentifier;
import org.apache.paimon.manifest.ManifestAvroReader;
import org.apache.paimon.manifest.ManifestAvroReader.RawBlock;
import org.apache.paimon.manifest.ManifestAvroReader.RowIterator;
import org.apache.paimon.manifest.ManifestAvroWriter.EncodedBlockMeta;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionDictionary;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ByteArrayKey;
import org.apache.paimon.utils.ByteArrayLookupKey;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
            CollectedDeletes deletes,
            int maxNumFileHandles,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        ManifestEntryRunMergePlan plan =
                discoverRuns(
                        section,
                        sortKey,
                        partitionType,
                        manifestFile,
                        deletes,
                        false,
                        maxNumFileHandles,
                        manifestReadParallelism);
        if (plan == null) {
            return null;
        }
        return plan.mergeToManifest(sortKey, manifestFile, newFilesForAbort);
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
        CollectedDeletes deletes = new CollectedDeletes(true);
        try {
            ManifestEntryRunMergePlan plan =
                    discoverRuns(
                            section,
                            sortKey,
                            partitionType,
                            manifestFile,
                            deletes,
                            true,
                            maxNumFileHandles,
                            manifestReadParallelism);
            if (plan == null) {
                return null;
            }
            return plan.mergeMinorToManifest(sortKey, manifestFile, newFilesForAbort);
        } finally {
            deletes.release();
        }
    }

    @Nullable
    private static ManifestEntryRunMergePlan discoverRuns(
            List<ManifestFileMeta> section,
            ManifestFileSorter.RowIdEntrySortKey sortKey,
            RowType partitionType,
            ManifestFile manifestFile,
            CollectedDeletes deletes,
            boolean minor,
            int maxNumFileHandles,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        SortPartitionDictionary partitions =
                new SortPartitionDictionary(sortKey::comparePartitions);
        List<ManifestEntryRunMergePlan.Source.Spec> sources = new ArrayList<>();
        int streamCursorCount = 0;
        long inMemoryEntries = 0;
        List<Discovery.DiscoveredManifest> discovered = new ArrayList<>(section.size());
        if (section.size() <= 1
                || (manifestReadParallelism != null && manifestReadParallelism <= 1)) {
            for (ManifestFileMeta meta : section) {
                CollectedDeletes discoveryDeletes =
                        minor ? new CollectedDeletes(deletes.useRowIdFilter()) : deletes;
                Discovery.DiscoveredManifest manifest;
                try {
                    manifest =
                            discoverManifestRuns(
                                    meta,
                                    manifestFile,
                                    partitionType,
                                    partitions,
                                    discoveryDeletes,
                                    minor);
                } catch (Exception e) {
                    if (minor) {
                        discoveryDeletes.release();
                    }
                    throw e;
                }
                if (minor) {
                    try {
                        deletes.combine(discoveryDeletes);
                    } finally {
                        discoveryDeletes.release();
                    }
                }
                if (manifest.requiresExternalSort) {
                    return null;
                }
                discovered.add(manifest);
            }
        } else {
            boolean requiresExternalSort = false;
            Function<ManifestFileMeta, List<Pair<Discovery.DiscoveredManifest, CollectedDeletes>>>
                    reader =
                            meta -> {
                                CollectedDeletes discoveryDeletes =
                                        minor
                                                ? new CollectedDeletes(deletes.useRowIdFilter())
                                                : deletes;
                                try {
                                    return Collections.singletonList(
                                            Pair.of(
                                                    discoverManifestRuns(
                                                            meta,
                                                            manifestFile,
                                                            partitionType,
                                                            partitions,
                                                            discoveryDeletes,
                                                            minor),
                                                    discoveryDeletes));
                                } catch (Exception e) {
                                    if (minor) {
                                        discoveryDeletes.release();
                                    }
                                    throw new RuntimeException(
                                            "Failed to discover sorted Avro runs in "
                                                    + meta.fileName(),
                                            e);
                                }
                            };
            for (Pair<Discovery.DiscoveredManifest, CollectedDeletes> scan :
                    sequentialBatchedExecute(reader, section, manifestReadParallelism)) {
                requiresExternalSort |= scan.getLeft().requiresExternalSort;
                if (minor) {
                    try {
                        deletes.combine(scan.getRight());
                    } finally {
                        scan.getRight().release();
                    }
                }
                discovered.add(scan.getLeft());
            }
            // Drain every task in the bounded discovery batch before falling back. Returning from
            // the lazy iterator early would leave already submitted manifest scans running beside
            // the external sorter and duplicate their I/O and retained memory.
            if (requiresExternalSort) {
                return null;
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
        if (minor) {
            deletes.toImmutable();
        }
        partitions.finish();
        for (Discovery.DiscoveredManifest manifest : discovered) {
            manifest.finishFiltering(deletes, minor);
            manifest.updatePartitionRanks(partitions);
        }
        return new ManifestEntryRunMergePlan(sources, partitions, deletes, minor);
    }

    private static Discovery.DiscoveredManifest discoverManifestRuns(
            ManifestFileMeta meta,
            ManifestFile manifestFile,
            RowType partitionType,
            SortPartitionDictionary partitions,
            CollectedDeletes deletes,
            boolean minor)
            throws Exception {
        ReusableIdentifier identifier = new ReusableIdentifier();
        try (ManifestAvroReader reader =
                manifestFile.scanAvroBlocks(meta.fileName(), meta.fileSize())) {
            return discoverManifestRuns(
                    meta, reader, partitionType, partitions, deletes, minor, identifier);
        } catch (UnsupportedOperationException unsupported) {
            return Discovery.DiscoveredManifest.requiresExternalSort();
        } finally {
            identifier.release();
        }
    }

    private static Discovery.DiscoveredManifest discoverManifestRuns(
            ManifestFileMeta meta,
            ManifestAvroReader reader,
            RowType partitionType,
            SortPartitionDictionary partitions,
            CollectedDeletes deletes,
            boolean minor,
            ReusableIdentifier identifier)
            throws Exception {
        SimpleStatsConverter partitionStatsConverter = new SimpleStatsConverter(partitionType);
        List<ManifestEntryRunMergePlan.Source.ManifestRunSpec> runs = new ArrayList<>();
        List<Discovery.BlockInfo> blocks = new ArrayList<>();
        SortKey previous = new SortKey();
        SortKey current = new SortKey();
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
                if (minor && entry.isDelete()) {
                    deletes.add(entry, deletes.useRowIdFilter(), false);
                }
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
                block.collectForSort(entry, current, partitions, deletes, identifier, minor);
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
                    SortKey stableLastKey = current.stableCopy();
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
            SortKey left, SortKey right, SortPartitionDictionary partitions) {
        return compareRemainingKeys(
                left, right, partitions.compareIds(left.partitionId, right.partitionId));
    }

    static int compareMergeKeys(SortKey left, SortKey right) {
        return compareRemainingKeys(
                left, right, Integer.compare(left.partitionRank, right.partitionRank));
    }

    private static int compareRemainingKeys(SortKey left, SortKey right, int comparison) {
        if (comparison == 0) {
            comparison = Byte.compare(left.kind, right.kind);
        }
        if (comparison == 0) {
            comparison = Long.compare(left.firstRowId, right.firstRowId);
        }
        if (comparison == 0) {
            comparison = Long.compare(left.lastRowId, right.lastRowId);
        }
        if (comparison == 0) {
            comparison = Long.compare(left.descendingSequenceKey, right.descendingSequenceKey);
        }
        if (comparison == 0) {
            comparison = compareBytes(left, right);
        }
        return comparison;
    }

    private static int compareBytes(SortKey left, SortKey right) {
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

    static final class SortKey {

        int partitionId;
        int partitionRank;
        byte kind;
        long firstRowId;
        long lastRowId;
        long descendingSequenceKey;
        MemorySegment[] fileNameSegments;
        int fileNameOffset;
        int fileNameLength;
        byte[] ownedFileNameBytes;
        MemorySegment[] ownedFileNameSegments;

        static SortKey viewOf(ProjectedManifestEntry entry, SortPartitionDictionary partitions) {
            SortKey key = new SortKey();
            key.replace(entry, partitions);
            return key;
        }

        void replace(ProjectedManifestEntry entry, SortPartitionDictionary partitions) {
            long firstRowId = entry.file().nonNullFirstRowId();
            this.partitionId = partitions.id(entry.partitionBytes());
            this.partitionRank = partitions.rank(partitionId);
            this.kind = entry.kind().toByteValue();
            this.firstRowId = firstRowId;
            this.lastRowId = firstRowId + entry.file().rowCount() - 1L;
            this.descendingSequenceKey = Long.MAX_VALUE - entry.file().maxSequenceNumber();
            BinaryString fileName = entry.file().fileNameBinary();
            this.fileNameSegments = fileName.getSegments();
            this.fileNameOffset = fileName.getOffset();
            this.fileNameLength = fileName.getSizeInBytes();
        }

        void copyFrom(SortKey key) {
            this.partitionId = key.partitionId;
            this.partitionRank = key.partitionRank;
            this.kind = key.kind;
            this.firstRowId = key.firstRowId;
            this.lastRowId = key.lastRowId;
            this.descendingSequenceKey = key.descendingSequenceKey;
            ensureFileNameCapacity(key.fileNameLength);
            MemorySegmentUtils.copyToBytes(
                    key.fileNameSegments,
                    key.fileNameOffset,
                    ownedFileNameBytes,
                    0,
                    key.fileNameLength);
            this.fileNameSegments = ownedFileNameSegments;
            this.fileNameOffset = 0;
            this.fileNameLength = key.fileNameLength;
        }

        SortKey stableCopy() {
            SortKey copy = new SortKey();
            copy.copyFrom(this);
            return copy;
        }

        private void ensureFileNameCapacity(int length) {
            if (ownedFileNameBytes == null || ownedFileNameBytes.length < length) {
                ownedFileNameBytes = new byte[length];
                ownedFileNameSegments =
                        new MemorySegment[] {MemorySegment.wrap(ownedFileNameBytes)};
            }
        }

        void clear() {
            fileNameSegments = null;
            ownedFileNameBytes = null;
            ownedFileNameSegments = null;
        }
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

            void updatePartitionRanks(SortPartitionDictionary partitions) {
                for (BlockInfo block : blocks) {
                    block.updatePartitionRanks(partitions);
                }
            }

            void finishFiltering(CollectedDeletes deletes, boolean minor) {
                for (BlockInfo block : blocks) {
                    block.finishFiltering(deletes, minor);
                }
            }
        }

        static final class BlockInfo {

            final long ordinal;
            final long start;
            final @Nullable SortKey firstKey;
            boolean sorted = true;
            long end;
            @Nullable SortKey lastKey;
            long minRowId;
            long maxRowId;
            @Nullable BlockMetadataAccumulator metadataAccumulator;
            @Nullable EncodedBlockMeta metadata;

            BlockInfo(
                    long ordinal,
                    long start,
                    boolean rawBlockCopySupported,
                    SortKey firstKey,
                    RowType partitionType) {
                this.ordinal = ordinal;
                this.start = start;
                this.firstKey = firstKey;
                this.metadataAccumulator =
                        rawBlockCopySupported && firstKey != null
                                ? new BlockMetadataAccumulator(partitionType)
                                : null;
            }

            void collectForSort(
                    ProjectedManifestEntry entry,
                    SortKey key,
                    SortPartitionDictionary partitions,
                    CollectedDeletes deletes,
                    ReusableIdentifier identifier,
                    boolean minor) {
                if (metadataAccumulator == null) {
                    return;
                }
                if (!deletes.copyable(entry, identifier, minor)) {
                    metadataAccumulator = null;
                    return;
                }
                metadataAccumulator.collect(entry, key, partitions.partition(key.partitionId));
            }

            void finishSort(
                    long end, SortKey lastKey, SimpleStatsConverter partitionStatsConverter) {
                this.end = end;
                this.lastKey = lastKey;
                if (metadataAccumulator != null && sorted) {
                    minRowId = metadataAccumulator.minRowId;
                    maxRowId = metadataAccumulator.maxRowId;
                    metadata = metadataAccumulator.finish(partitionStatsConverter);
                }
                metadataAccumulator = null;
            }

            boolean copyable(long runStart, long runEnd) {
                return metadata != null && start >= runStart && end <= runEnd;
            }

            void finishFiltering(CollectedDeletes deletes, boolean minor) {
                if (metadata != null
                        && minor
                        && (!deletes.useRowIdFilter()
                                || deletes.intersectsRowIds(minRowId, maxRowId))) {
                    metadata = null;
                }
            }

            void updatePartitionRanks(SortPartitionDictionary partitions) {
                checkState(firstKey != null && lastKey != null, "Manifest block has no sort keys.");
                firstKey.partitionRank = partitions.rank(firstKey.partitionId);
                lastKey.partitionRank = partitions.rank(lastKey.partitionId);
            }
        }

        /** Mutable statistics retained only while the current Avro block is being inspected. */
        private static final class BlockMetadataAccumulator {

            private long addedFiles;
            private long deletedFiles;
            private long schemaId = Long.MIN_VALUE;
            private int minBucket = Integer.MAX_VALUE;
            private int maxBucket = Integer.MIN_VALUE;
            private int minLevel = Integer.MAX_VALUE;
            private int maxLevel = Integer.MIN_VALUE;
            private long minRowId = Long.MAX_VALUE;
            private long maxRowId = Long.MIN_VALUE;
            private final BlockPartitionStats partitionStats;

            private BlockMetadataAccumulator(RowType partitionType) {
                this.partitionStats = new BlockPartitionStats(partitionType);
            }

            private void collect(ProjectedManifestEntry entry, SortKey key, BinaryRow partition) {
                if (entry.isAdd()) {
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
                maxRowId = Math.max(maxRowId, key.lastRowId);
                partitionStats.collect(partition);
            }

            private EncodedBlockMeta finish(SimpleStatsConverter partitionStatsConverter) {
                return new EncodedBlockMeta(
                        addedFiles,
                        deletedFiles,
                        schemaId,
                        minBucket,
                        maxBucket,
                        minLevel,
                        maxLevel,
                        minRowId,
                        maxRowId,
                        partitionStats.finish(partitionStatsConverter));
            }
        }

        /** Partition statistics for one sorted Avro block. */
        private static final class BlockPartitionStats {

            private final boolean singleField;
            private final @Nullable SimpleStatsCollector collector;
            private @Nullable BinaryRow nullPartition;
            private @Nullable BinaryRow minNonNullPartition;
            private @Nullable BinaryRow maxNonNullPartition;
            private long nullCount;

            private BlockPartitionStats(RowType partitionType) {
                this.singleField = partitionType.getFieldCount() == 1;
                this.collector = singleField ? null : new SimpleStatsCollector(partitionType);
            }

            private void collect(BinaryRow partition) {
                if (!singleField) {
                    checkState(collector != null, "Manifest block has no partition collector.");
                    collector.collect(partition);
                    return;
                }
                if (partition.isNullAt(0)) {
                    nullPartition = partition;
                    nullCount++;
                } else {
                    if (minNonNullPartition == null) {
                        minNonNullPartition = partition;
                    }
                    maxNonNullPartition = partition;
                }
            }

            private SimpleStats finish(SimpleStatsConverter converter) {
                if (!singleField) {
                    checkState(collector != null, "Manifest block has no partition collector.");
                    return converter.toBinaryAllMode(collector.extract());
                }
                BinaryRow min = minNonNullPartition == null ? nullPartition : minNonNullPartition;
                BinaryRow max = maxNonNullPartition == null ? nullPartition : maxNonNullPartition;
                checkState(min != null && max != null, "Manifest block has no partition.");
                return new SimpleStats(min, max, BinaryArray.fromLongArray(new Long[] {nullCount}));
            }
        }
    }

    /** Concurrent partition dictionary and ordering used only by manifest run merge. */
    static final class SortPartitionDictionary {

        private final Comparator<BinaryRow> comparator;
        private final PartitionDictionary partitions = new PartitionDictionary();
        private final Map<ByteArrayKey, Integer> ids = new ConcurrentHashMap<>();
        private final ThreadLocal<ByteArrayLookupKey> lookup =
                ThreadLocal.withInitial(ByteArrayLookupKey::new);
        private int[] ranks;

        SortPartitionDictionary(Comparator<BinaryRow> comparator) {
            this.comparator = comparator;
        }

        int id(byte[] bytes) {
            ByteArrayLookupKey lookupKey = lookup.get();
            lookupKey.reset(bytes);
            try {
                Integer existing = ids.get(lookupKey);
                if (existing != null) {
                    return existing;
                }
                synchronized (this) {
                    existing = ids.get(lookupKey);
                    if (existing != null) {
                        return existing;
                    }
                    checkState(ranks == null, "Manifest scan found an unknown partition.");
                    byte[] canonical = Arrays.copyOf(bytes, bytes.length);
                    int id = partitions.id(canonical);
                    ids.put(new ByteArrayKey(canonical), id);
                    return id;
                }
            } finally {
                lookupKey.clear();
            }
        }

        void finish() {
            int partitionCount = ids.size();
            List<Integer> order = new ArrayList<>(partitionCount);
            for (int id = 0; id < partitionCount; id++) {
                order.add(id);
            }
            order.sort((left, right) -> compareIds(left, right));
            ranks = new int[partitionCount];
            int rank = 0;
            for (int position = 0; position < order.size(); position++) {
                if (position > 0 && compareIds(order.get(position - 1), order.get(position)) != 0) {
                    rank++;
                }
                ranks[order.get(position)] = rank;
            }
        }

        int compareIds(int left, int right) {
            return comparator.compare(partitions.partition(left), partitions.partition(right));
        }

        int rank(int id) {
            return ranks == null ? 0 : ranks[id];
        }

        BinaryRow partition(int id) {
            return partitions.partition(id);
        }
    }
}
