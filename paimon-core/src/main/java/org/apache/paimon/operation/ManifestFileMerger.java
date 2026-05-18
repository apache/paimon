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
import org.apache.paimon.codegen.NormalizedKeyComputer;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.sort.BinaryInMemorySortBuffer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.SerializationUtils;

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

import static org.apache.paimon.codegen.CodeGenUtils.newRecordComparator;
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
            @Nullable Integer manifestReadParallelism,
            boolean manifestMergeSorted) {
        return merge(
                input,
                manifestFile,
                suggestedMetaSize,
                suggestedMinMetaCount,
                manifestFullCompactionSize,
                partitionType,
                manifestReadParallelism,
                manifestMergeSorted,
                CoreOptions.MANIFEST_MERGE_SORT_BUFFER.defaultValue().getBytes());
    }

    public static List<ManifestFileMeta> merge(
            List<ManifestFileMeta> input,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            long manifestFullCompactionSize,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism,
            boolean manifestMergeSorted,
            long manifestMergeSortBufferSize) {
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
                            manifestReadParallelism,
                            manifestMergeSorted,
                            manifestMergeSortBufferSize);
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

    public static Optional<List<ManifestFileMeta>> tryFullCompaction(
            List<ManifestFileMeta> inputs,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            long sizeTrigger,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism,
            boolean manifestMergeSorted)
            throws Exception {
        return tryFullCompaction(
                inputs,
                newFilesForAbort,
                manifestFile,
                suggestedMetaSize,
                sizeTrigger,
                partitionType,
                manifestReadParallelism,
                manifestMergeSorted,
                CoreOptions.MANIFEST_MERGE_SORT_BUFFER.defaultValue().getBytes());
    }

    public static Optional<List<ManifestFileMeta>> tryFullCompaction(
            List<ManifestFileMeta> inputs,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            long sizeTrigger,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism,
            boolean manifestMergeSorted,
            long manifestMergeSortBufferSize)
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
        Exception exception = null;
        int actualRewriteCount;
        try {
            if (manifestMergeSorted) {
                actualRewriteCount =
                        mergeSortedByPartition(
                                toBeMerged,
                                mustChange,
                                deleteEntries,
                                manifestFile,
                                partitionType,
                                manifestMergeSortBufferSize,
                                writer,
                                result);
            } else {
                actualRewriteCount =
                        mergeUnsorted(
                                toBeMerged,
                                mustChange,
                                deleteEntries,
                                manifestFile,
                                writer,
                                result);
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

    private static final NormalizedKeyComputer NO_NORMALIZED_KEY_COMPUTER =
            new NormalizedKeyComputer() {
                @Override
                public void putKey(InternalRow record, MemorySegment target, int offset) {
                    // no-op
                }

                @Override
                public int compareKey(
                        MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ) {
                    return 0;
                }

                @Override
                public void swapKey(
                        MemorySegment segI, int offsetI, MemorySegment segJ, int offsetJ) {
                    // no-op
                }

                @Override
                public int getNumKeyBytes() {
                    return 0;
                }

                @Override
                public boolean isKeyFullyDetermines() {
                    return false;
                }

                @Override
                public boolean invertKey() {
                    return false;
                }
            };

    private static int mergeUnsorted(
            List<ManifestFileMeta> toBeMerged,
            Filter<ManifestFileMeta> mustChange,
            Set<FileEntry.Identifier> deleteEntries,
            ManifestFile manifestFile,
            RollingFileWriter<ManifestEntry, ManifestFileMeta> writer,
            List<ManifestFileMeta> result)
            throws Exception {
        int actualRewriteCount = 0;
        for (ManifestFileMeta file : toBeMerged) {
            List<ManifestEntry> entries = new ArrayList<>();
            boolean requireChange = mustChange.test(file);
            for (ManifestEntry entry : manifestFile.read(file.fileName(), file.fileSize())) {
                if (entry.kind() == FileKind.DELETE) {
                    continue;
                }

                if (deleteEntries.contains(entry.identifier())) {
                    requireChange = true;
                } else {
                    entries.add(entry);
                }
            }

            if (requireChange) {
                writer.write(entries);
                actualRewriteCount++;
            } else {
                result.add(file);
            }
        }
        return actualRewriteCount;
    }

    private static int mergeSortedByPartition(
            List<ManifestFileMeta> toBeMerged,
            Filter<ManifestFileMeta> mustChange,
            Set<FileEntry.Identifier> deleteEntries,
            ManifestFile manifestFile,
            RowType partitionType,
            long manifestMergeSortBufferSize,
            RollingFileWriter<ManifestEntry, ManifestFileMeta> writer,
            List<ManifestFileMeta> result)
            throws Exception {
        IOManager ioManager = null;
        BinaryExternalSortBuffer sortBuffer = null;
        RowType sortRowType = null;
        ManifestEntrySerializer entrySerializer = new ManifestEntrySerializer();
        int actualRewriteCount = 0;

        try {
            for (ManifestFileMeta file : toBeMerged) {
                List<ManifestEntry> entries = new ArrayList<>();
                boolean requireChange = mustChange.test(file);
                for (ManifestEntry entry : manifestFile.read(file.fileName(), file.fileSize())) {
                    if (entry.kind() == FileKind.DELETE) {
                        continue;
                    }

                    if (deleteEntries.contains(entry.identifier())) {
                        requireChange = true;
                    } else {
                        entries.add(entry);
                    }
                }

                if (requireChange) {
                    if (sortBuffer == null) {
                        sortRowType = manifestEntrySortRowType(partitionType);
                        ioManager = IOManager.create(System.getProperty("java.io.tmpdir"));
                        sortBuffer =
                                createManifestEntrySortBuffer(
                                        ioManager,
                                        sortRowType,
                                        partitionType,
                                        manifestMergeSortBufferSize);
                    }
                    for (ManifestEntry entry : entries) {
                        GenericRow row = new GenericRow(4);
                        row.setField(0, entry.partition());
                        row.setField(1, entry.bucket());
                        row.setField(2, entry.level());
                        row.setField(3, entrySerializer.serializeToBytes(entry));
                        sortBuffer.write(row);
                    }
                    actualRewriteCount++;
                } else {
                    result.add(file);
                }
            }

            if (sortBuffer != null) {
                MutableObjectIterator<BinaryRow> iterator = sortBuffer.sortedIterator();
                BinaryRow reuse = new BinaryRow(sortRowType.getFieldCount());
                BinaryRow next;
                while ((next = iterator.next(reuse)) != null) {
                    ManifestEntry entry = entrySerializer.deserializeFromBytes(next.getBinary(3));
                    writer.write(entry);
                }
            }

            return actualRewriteCount;
        } finally {
            if (sortBuffer != null) {
                sortBuffer.clear();
            }
            if (ioManager != null) {
                ioManager.close();
            }
        }
    }

    private static BinaryExternalSortBuffer createManifestEntrySortBuffer(
            IOManager ioManager,
            RowType sortRowType,
            RowType partitionType,
            long manifestMergeSortBufferSize) {
        int pageSize = (int) CoreOptions.PAGE_SIZE.defaultValue().getBytes();
        long minBufferSize = 3L * pageSize;
        checkArgument(
                manifestMergeSortBufferSize >= minBufferSize,
                "Manifest merge sort buffer must be at least three pages (" + minBufferSize + ")");

        RecordComparator partitionRmpr = null;
        if (partitionType.getFieldCount() > 0) {
            partitionRmpr = createPartitionRecordComparator(partitionType);
        }
        RecordComparator partitionComparator = partitionRmpr;

        RecordComparator comparator =
                (a, b) -> {
                    if (partitionComparator != null) {
                        int cmp =
                                partitionComparator.compare(
                                        a.getRow(0, partitionType.getFieldCount()),
                                        b.getRow(0, partitionType.getFieldCount()));
                        if (cmp != 0) {
                            return cmp;
                        }
                    }

                    int cmp = Integer.compare(a.getInt(1), b.getInt(1));
                    if (cmp != 0) {
                        return cmp;
                    }

                    return Integer.compare(a.getInt(2), b.getInt(2));
                };

        MemorySegmentPool memoryPool =
                new HeapMemorySegmentPool(manifestMergeSortBufferSize, pageSize);
        InternalRowSerializer serializer = new InternalRowSerializer(sortRowType);
        BinaryInMemorySortBuffer inMemorySortBuffer =
                BinaryInMemorySortBuffer.createBuffer(
                        NO_NORMALIZED_KEY_COMPUTER, serializer, comparator, memoryPool);

        return new BinaryExternalSortBuffer(
                new BinaryRowSerializer(sortRowType.getFieldCount()),
                comparator,
                memoryPool.pageSize(),
                inMemorySortBuffer,
                ioManager,
                CoreOptions.LOCAL_SORT_MAX_NUM_FILE_HANDLES.defaultValue(),
                CompressOptions.defaultOptions(),
                CoreOptions.WRITE_BUFFER_MAX_DISK_SIZE.defaultValue());
    }

    private static RowType manifestEntrySortRowType(RowType partitionType) {
        return RowType.of(
                partitionType,
                new IntType(false),
                new IntType(false),
                SerializationUtils.newBytesType(false));
    }

    private static Comparator<BinaryRow> createPartitionRecordComparator(RowType partitionType) {
        try {
            int[] sortFields = new int[partitionType.getFieldCount()];
            boolean[] ascendingOrders = new boolean[sortFields.length];
            for (int i = 0; i < sortFields.length; i++) {
                sortFields[i] = i;
                ascendingOrders[i] = true;
            }
            RecordComparator codegenComparator =
                    newRecordComparator(
                            partitionType.getFieldTypes(), sortFields, ascendingOrders);
            return (a, b) -> codegenComparator.compare(a, b);
        } catch (Throwable t) {
            // Fallback to pure-java comparison for environments where codegen is unavailable.
            List<DataType> fieldTypes = partitionType.getFieldTypes();
            InternalRow.FieldGetter[] getters = new InternalRow.FieldGetter[fieldTypes.size()];
            for (int i = 0; i < getters.length; i++) {
                getters[i] = InternalRow.createFieldGetter(fieldTypes.get(i), i);
            }
            return (a, b) -> {
                for (int i = 0; i < getters.length; i++) {
                    int cmp =
                            InternalRowUtils.compare(
                                    getters[i].getFieldOrNull(a),
                                    getters[i].getFieldOrNull(b),
                                    fieldTypes.get(i).getTypeRoot());
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                return 0;
            };
        }
    }

    static Comparator<ManifestEntry> createManifestEntryComparator(RowType partitionType) {
        Comparator<BinaryRow> partitionComparator = null;
        if (partitionType.getFieldCount() > 0) {
            partitionComparator = createPartitionRecordComparator(partitionType);
        }

        Comparator<BinaryRow> finalPartitionComparator = partitionComparator;
        return (a, b) -> {
            int cmp = 0;
            if (finalPartitionComparator != null) {
                cmp = finalPartitionComparator.compare(a.partition(), b.partition());
                if (cmp != 0) {
                    return cmp;
                }
            }

            cmp = Integer.compare(a.bucket(), b.bucket());
            if (cmp != 0) {
                return cmp;
            }

            cmp = Integer.compare(a.level(), b.level());
            if (cmp != 0) {
                return cmp;
            }

            return a.fileName().compareTo(b.fileName());
        };
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
}
