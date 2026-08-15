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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.CompactFileIdentifierSet;
import org.apache.paimon.manifest.DeletedRowIdSet;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestAvroReader;
import org.apache.paimon.manifest.ManifestAvroReader.RawBlock;
import org.apache.paimon.manifest.ManifestAvroReader.RowIterator;
import org.apache.paimon.manifest.ManifestAvroWriter.EncodedBlockMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.DataField;
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
    static final int KIND = 0;
    static final int PARTITION = 1;
    static final int BUCKET = 2;
    static final int FILE = 3;
    static final int FILE_NAME = 0;
    static final int ROW_COUNT = 1;
    static final int LEVEL = 2;
    static final int SCHEMA_ID = 3;
    static final int FIRST_ROW_ID = 4;
    static final int MAX_SEQUENCE_NUMBER = 5;
    static final int EXTRA_FILES = 6;
    static final int EMBEDDED_FILE_INDEX = 7;
    static final int EXTERNAL_PATH = 8;
    static final int FILE_FIELD_COUNT = 9;
    private static final String[] ENTRY_FILE_FIELD_NAMES = {
        DataFileMeta.FILE_NAME,
        DataFileMeta.ROW_COUNT,
        DataFileMeta.LEVEL,
        DataFileMeta.SCHEMA_ID,
        DataFileMeta.FIRST_ROW_ID,
        DataFileMeta.MAX_SEQUENCE_NUMBER,
        DataFileMeta.EXTRA_FILES,
        DataFileMeta.EMBEDDED_FILE_INDEX,
        DataFileMeta.EXTERNAL_PATH
    };
    private static final InternalRow.FieldGetter[] ENTRY_FILE_GETTERS = entryFileGetters();
    static final RowType ENTRY_LAYOUT = entryLayout();
    private static final int FULL_KIND =
            ManifestEntry.MANIFEST_ROW_TYPE.getFieldIndex(ManifestEntry.KIND);
    private static final int FULL_PARTITION =
            ManifestEntry.MANIFEST_ROW_TYPE.getFieldIndex(ManifestEntry.PARTITION);
    private static final int FULL_BUCKET =
            ManifestEntry.MANIFEST_ROW_TYPE.getFieldIndex(ManifestEntry.BUCKET);
    private static final int FULL_FILE =
            ManifestEntry.MANIFEST_ROW_TYPE.getFieldIndex(ManifestEntry.FILE);

    private ManifestEntryRunMerge() {}

    private static InternalRow.FieldGetter[] entryFileGetters() {
        InternalRow.FieldGetter[] getters =
                new InternalRow.FieldGetter[ENTRY_FILE_FIELD_NAMES.length];
        for (int field = 0; field < getters.length; field++) {
            int position = DataFileMeta.SCHEMA.getFieldIndex(ENTRY_FILE_FIELD_NAMES[field]);
            getters[field] =
                    InternalRow.createFieldGetter(
                            DataFileMeta.SCHEMA.getTypeAt(position), position);
        }
        return getters;
    }

    private static RowType entryLayout() {
        List<DataField> fields = new ArrayList<>();
        fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.KIND));
        fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.PARTITION));
        fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.BUCKET));
        fields.add(
                ManifestEntry.MANIFEST_ROW_TYPE
                        .getField(ManifestEntry.FILE)
                        .newType(
                                DataFileMeta.SCHEMA.project(
                                        DataFileMeta.FILE_NAME,
                                        DataFileMeta.ROW_COUNT,
                                        DataFileMeta.LEVEL,
                                        DataFileMeta.SCHEMA_ID,
                                        DataFileMeta.FIRST_ROW_ID,
                                        DataFileMeta.MAX_SEQUENCE_NUMBER,
                                        DataFileMeta.EXTRA_FILES,
                                        DataFileMeta.EMBEDDED_FILE_INDEX,
                                        DataFileMeta.EXTERNAL_PATH)));
        return new RowType(false, fields);
    }

    static GenericRow projectEntryLayout(
            GenericRow fullRow, GenericRow reuse, GenericRow reuseFile) {
        reuse.setField(KIND, fullRow.getByte(FULL_KIND));
        reuse.setField(PARTITION, fullRow.getBinary(FULL_PARTITION));
        reuse.setField(BUCKET, fullRow.getInt(FULL_BUCKET));
        InternalRow fullFile = fullRow.getRow(FULL_FILE, DataFileMeta.SCHEMA.getFieldCount());
        for (int field = 0; field < ENTRY_FILE_GETTERS.length; field++) {
            reuseFile.setField(field, ENTRY_FILE_GETTERS[field].getFieldOrNull(fullFile));
        }
        reuse.setField(FILE, reuseFile);
        return reuse;
    }

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
        ManifestEntryRunMergeEntry.PartitionDictionary partitions =
                new ManifestEntryRunMergeEntry.PartitionDictionary(sortKey);
        List<ManifestEntryRunMergePlan.Source.Spec> sources = new ArrayList<>();
        int streamCursorCount = 0;
        long inMemoryEntries = 0;
        List<Discovery.DiscoveredManifest> discovered = new ArrayList<>(section.size());
        if (section.size() <= 1
                || (manifestReadParallelism != null && manifestReadParallelism <= 1)) {
            for (ManifestFileMeta meta : section) {
                Discovery.DiscoveredManifest manifest =
                        discoverManifestRuns(meta, manifestFile, partitionType, partitions, filter);
                if (manifest.requiresExternalSort) {
                    return null;
                }
                discovered.add(manifest);
            }
        } else {
            Function<ManifestFileMeta, List<Discovery.DiscoveredManifest>> reader =
                    meta -> {
                        try {
                            return Collections.singletonList(
                                    discoverManifestRuns(
                                            meta, manifestFile, partitionType, partitions, filter));
                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "Failed to discover sorted Avro runs in " + meta.fileName(), e);
                        }
                    };
            for (Discovery.DiscoveredManifest manifest :
                    sequentialBatchedExecute(reader, section, manifestReadParallelism)) {
                discovered.add(manifest);
            }
        }
        for (int manifestIndex = 0; manifestIndex < section.size(); manifestIndex++) {
            ManifestFileMeta meta = section.get(manifestIndex);
            Discovery.DiscoveredManifest manifest = discovered.get(manifestIndex);
            if (manifest.requiresExternalSort) {
                return null;
            }
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
            ManifestEntryRunMergeEntry.PartitionDictionary partitions,
            ManifestEntryRunMergeEntry.Filter filter)
            throws Exception {
        try (ManifestAvroReader reader =
                manifestFile.scanAvroBlocks(meta.fileName(), meta.fileSize())) {
            return discoverManifestRuns(meta, reader, partitionType, partitions, filter);
        } catch (UnsupportedOperationException unsupported) {
            return Discovery.DiscoveredManifest.requiresExternalSort();
        }
    }

    private static Discovery.DiscoveredManifest discoverManifestRuns(
            ManifestFileMeta meta,
            ManifestAvroReader reader,
            RowType partitionType,
            ManifestEntryRunMergeEntry.PartitionDictionary partitions,
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
        while (reader.hasNext()) {
            RawBlock rawBlock = reader.next();
            RowIterator rows = rawBlock.toRows(ENTRY_LAYOUT);
            while (rows.hasNext()) {
                GenericRow row = rows.next();
                current.replace(row, partitions);
                filter.observe(row, current);
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
                block.collectForSort(row, current, partitions, filter);
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
            ManifestEntryRunMergeEntry.PartitionDictionary partitions) {
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
            int leftByte = left.fileNameBytes[left.fileNameOffset + i] & 0xFF;
            int rightByte = right.fileNameBytes[right.fileNameOffset + i] & 0xFF;
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

            void updatePartitionRanks(ManifestEntryRunMergeEntry.PartitionDictionary partitions) {
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
                    GenericRow record,
                    ManifestEntryRunMergeEntry.Key key,
                    ManifestEntryRunMergeEntry.PartitionDictionary partitions,
                    ManifestEntryRunMergeEntry.Filter filter) {
                if (!eligible) {
                    return;
                }
                if (!filter.copyable(record, key)) {
                    eligible = false;
                    releasePartitionStats();
                    return;
                }
                collectEntryStats(record, key);
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

            private void collectEntryStats(GenericRow record, ManifestEntryRunMergeEntry.Key key) {
                InternalRow file = ManifestEntryRunMergeEntry.file(record);
                if (key.kind == FileKind.ADD.toByteValue()) {
                    addedFiles++;
                } else {
                    deletedFiles++;
                }
                schemaId = Math.max(schemaId, file.getLong(SCHEMA_ID));
                int bucket = record.getInt(BUCKET);
                minBucket = Math.min(minBucket, bucket);
                maxBucket = Math.max(maxBucket, bucket);
                int level = file.getInt(LEVEL);
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

            void updatePartitionRanks(ManifestEntryRunMergeEntry.PartitionDictionary partitions) {
                checkState(firstKey != null && lastKey != null, "Manifest block has no sort keys.");
                firstKey.partitionRank = partitions.rank(firstKey.partitionId);
                lastKey.partitionRank = partitions.rank(lastKey.partitionId);
            }
        }
    }
}
