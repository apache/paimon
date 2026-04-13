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

package org.apache.paimon.mergetree.compact.clustering;

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.disk.ChannelReaderInputView;
import org.apache.paimon.disk.ChannelReaderInputViewIterator;
import org.apache.paimon.disk.ChannelWithMeta;
import org.apache.paimon.disk.ChannelWriterOutputView;
import org.apache.paimon.disk.FileChannelUtil;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.KeyValueWithLevelNoReusingSerializer;
import org.apache.paimon.utils.MutableObjectIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Handles file rewriting for clustering compaction, including sorting unsorted files (Phase 1) and
 * merging sorted files via multi-way merge (Phase 2).
 */
public class ClusteringFileRewriter {

    private final RowType keyType;
    private final RowType valueType;
    private final int[] clusteringColumns;
    private final RecordComparator clusteringComparatorAlone;
    private final RecordComparator clusteringComparatorInValue;
    private final IOManager ioManager;
    private final KeyValueFileReaderFactory valueReaderFactory;
    private final KeyValueFileWriterFactory writerFactory;
    private final ClusteringFiles fileLevels;
    private final long targetFileSize;
    private final long sortSpillBufferSize;
    private final int pageSize;
    private final int maxNumFileHandles;
    private final int spillThreshold;
    private final CompressOptions compression;

    public ClusteringFileRewriter(
            RowType keyType,
            RowType valueType,
            int[] clusteringColumns,
            RecordComparator clusteringComparatorAlone,
            RecordComparator clusteringComparatorInValue,
            IOManager ioManager,
            KeyValueFileReaderFactory valueReaderFactory,
            KeyValueFileWriterFactory writerFactory,
            ClusteringFiles fileLevels,
            long targetFileSize,
            long sortSpillBufferSize,
            int pageSize,
            int maxNumFileHandles,
            int spillThreshold,
            CompressOptions compression) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.clusteringColumns = clusteringColumns;
        this.clusteringComparatorAlone = clusteringComparatorAlone;
        this.clusteringComparatorInValue = clusteringComparatorInValue;
        this.ioManager = ioManager;
        this.valueReaderFactory = valueReaderFactory;
        this.writerFactory = writerFactory;
        this.fileLevels = fileLevels;
        this.targetFileSize = targetFileSize;
        this.sortSpillBufferSize = sortSpillBufferSize;
        this.pageSize = pageSize;
        this.maxNumFileHandles = maxNumFileHandles;
        this.spillThreshold = spillThreshold;
        this.compression = compression;
    }

    /**
     * Sort and rewrite unsorted files by clustering columns. Reads all KeyValue records, sorts them
     * using an external sort buffer, and writes to new level-1 files. Checks the key index inline
     * during writing to handle deduplication (FIRST_ROW skips duplicates, DEDUPLICATE marks old
     * positions in DV) and updates the index without re-reading the output files.
     *
     * @param keyIndex the key index for inline checking and batch update, or null to skip
     * @param originalFileNames file names of the original files being replaced (for index check)
     */
    public List<DataFileMeta> sortAndRewriteFiles(
            List<DataFileMeta> inputFiles,
            KeyValueSerializer kvSerializer,
            RowType kvSchemaType,
            @Nullable ClusteringKeyIndex keyIndex,
            Set<String> originalFileNames)
            throws Exception {
        int[] sortFieldsInKeyValue =
                Arrays.stream(clusteringColumns)
                        .map(i -> i + keyType.getFieldCount() + 2)
                        .toArray();
        BinaryExternalSortBuffer sortBuffer =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        kvSchemaType,
                        sortFieldsInKeyValue,
                        sortSpillBufferSize,
                        pageSize,
                        maxNumFileHandles,
                        compression,
                        MemorySize.MAX_VALUE,
                        false);

        for (DataFileMeta file : inputFiles) {
            try (RecordReader<KeyValue> reader = valueReaderFactory.createRecordReader(file)) {
                try (CloseableIterator<KeyValue> iterator = reader.toCloseableIterator()) {
                    while (iterator.hasNext()) {
                        KeyValue kv = iterator.next();
                        InternalRow serializedRow = kvSerializer.toRow(kv);
                        sortBuffer.write(serializedRow);
                    }
                }
            }
        }

        RowCompactedSerializer keySerializer =
                keyIndex != null ? new RowCompactedSerializer(keyType) : null;
        List<byte[]> collectedKeys = keyIndex != null ? new ArrayList<>() : null;

        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingClusteringFileWriter();
        try {
            MutableObjectIterator<BinaryRow> sortedIterator = sortBuffer.sortedIterator();
            BinaryRow binaryRow = new BinaryRow(kvSchemaType.getFieldCount());
            while ((binaryRow = sortedIterator.next(binaryRow)) != null) {
                KeyValue kv = kvSerializer.fromRow(binaryRow);
                KeyValue copied =
                        kv.copy(
                                new InternalRowSerializer(keyType),
                                new InternalRowSerializer(valueType));
                if (keyIndex != null) {
                    byte[] keyBytes = keySerializer.serializeToBytes(copied.key());
                    if (!keyIndex.checkKey(keyBytes, originalFileNames)) {
                        continue;
                    }
                    collectedKeys.add(keyBytes);
                }
                writer.write(copied);
            }
        } finally {
            sortBuffer.clear();
            writer.close();
        }

        List<DataFileMeta> newFiles = writer.result();
        for (DataFileMeta file : inputFiles) {
            fileLevels.removeFile(file);
        }
        for (DataFileMeta newFile : newFiles) {
            fileLevels.addNewFile(newFile);
        }

        // Batch update index using collected keys, split by file rowCount
        if (keyIndex != null) {
            int offset = 0;
            for (DataFileMeta newFile : newFiles) {
                int count = (int) newFile.rowCount();
                keyIndex.batchPutIndex(newFile, collectedKeys.subList(offset, offset + count));
                offset += count;
            }
        }

        return newFiles;
    }

    /**
     * Pick merge candidate groups based on clustering column range overlap and file sizes.
     *
     * @param sortedFiles all sorted files
     * @return list of merge groups; each group contains files to merge together
     */
    public List<List<DataFileMeta>> pickMergeCandidates(List<DataFileMeta> sortedFiles) {
        if (sortedFiles.size() < 2) {
            return Collections.emptyList();
        }

        List<List<DataFileMeta>> sections = groupIntoSections(sortedFiles);

        long smallSectionThreshold = targetFileSize / 2;
        List<List<DataFileMeta>> mergeGroups = new ArrayList<>();
        List<DataFileMeta> pending = new ArrayList<>();

        for (List<DataFileMeta> section : sections) {
            boolean needsMerge = section.size() >= 2;
            boolean isSmall = sectionSize(section) < smallSectionThreshold;

            if (needsMerge || isSmall) {
                pending.addAll(section);
            } else {
                if (pending.size() >= 2) {
                    mergeGroups.add(new ArrayList<>(pending));
                }
                pending.clear();
            }
        }

        if (pending.size() >= 2) {
            mergeGroups.add(pending);
        }

        return mergeGroups;
    }

    /**
     * Merge sorted files using min-heap based multi-way merge. Key index entries are deleted before
     * reading and rebuilt after writing by the caller.
     */
    public List<DataFileMeta> mergeAndRewriteFiles(List<DataFileMeta> inputFiles) throws Exception {
        InternalRowSerializer keyRowSerializer = new InternalRowSerializer(keyType);
        InternalRowSerializer valueRowSerializer = new InternalRowSerializer(valueType);

        // Determine which files to spill to row-based temp files
        List<DataFileMeta> filesToSpill = new ArrayList<>();
        List<DataFileMeta> filesToKeep;
        if (inputFiles.size() > spillThreshold) {
            List<DataFileMeta> sortedBySize = new ArrayList<>(inputFiles);
            sortedBySize.sort(Comparator.comparingLong(DataFileMeta::fileSize));
            int spillCount = inputFiles.size() - spillThreshold;
            filesToSpill = new ArrayList<>(sortedBySize.subList(0, spillCount));
            filesToKeep = new ArrayList<>(sortedBySize.subList(spillCount, sortedBySize.size()));
        } else {
            filesToKeep = inputFiles;
        }

        // Spill smaller files to row-based temp files
        List<SpilledChannel> spilledChannels = new ArrayList<>();
        for (DataFileMeta file : filesToSpill) {
            spilledChannels.add(spillToRowBasedFile(file));
        }

        // Open iterators and initialize the min-heap
        List<CloseableIterator<KeyValue>> openIterators = new ArrayList<>();
        PriorityQueue<MergeEntry> minHeap =
                new PriorityQueue<>(
                        (a, b) ->
                                clusteringComparatorInValue.compare(
                                        a.currentKeyValue.value(), b.currentKeyValue.value()));

        try {
            for (DataFileMeta file : filesToKeep) {
                @SuppressWarnings("resource")
                CloseableIterator<KeyValue> iterator =
                        valueReaderFactory.createRecordReader(file).toCloseableIterator();
                openIterators.add(iterator);
                if (iterator.hasNext()) {
                    KeyValue firstKv = iterator.next().copy(keyRowSerializer, valueRowSerializer);
                    minHeap.add(new MergeEntry(firstKv, iterator));
                }
            }

            for (SpilledChannel spilled : spilledChannels) {
                CloseableIterator<KeyValue> iterator = spilled.createIterator();
                openIterators.add(iterator);
                if (iterator.hasNext()) {
                    KeyValue firstKv = iterator.next().copy(keyRowSerializer, valueRowSerializer);
                    minHeap.add(new MergeEntry(firstKv, iterator));
                }
            }

            RollingFileWriter<KeyValue, DataFileMeta> writer =
                    writerFactory.createRollingClusteringFileWriter();
            try {
                while (!minHeap.isEmpty()) {
                    MergeEntry entry = minHeap.poll();
                    writer.write(entry.currentKeyValue);
                    if (entry.iterator.hasNext()) {
                        entry.currentKeyValue =
                                entry.iterator.next().copy(keyRowSerializer, valueRowSerializer);
                        minHeap.add(entry);
                    }
                }
            } finally {
                writer.close();
            }

            List<DataFileMeta> newFiles = writer.result();
            for (DataFileMeta file : inputFiles) {
                fileLevels.removeFile(file);
            }
            for (DataFileMeta newFile : newFiles) {
                fileLevels.addNewFile(newFile);
            }

            return newFiles;
        } finally {
            for (CloseableIterator<KeyValue> iterator : openIterators) {
                try {
                    iterator.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    private List<List<DataFileMeta>> groupIntoSections(List<DataFileMeta> files) {
        List<DataFileMeta> sorted = new ArrayList<>(files);
        sorted.sort((a, b) -> clusteringComparatorAlone.compare(a.minKey(), b.minKey()));

        List<List<DataFileMeta>> sections = new ArrayList<>();
        List<DataFileMeta> currentSection = new ArrayList<>();
        currentSection.add(sorted.get(0));
        BinaryRow currentMax = sorted.get(0).maxKey();

        for (int i = 1; i < sorted.size(); i++) {
            DataFileMeta file = sorted.get(i);
            if (clusteringComparatorAlone.compare(currentMax, file.minKey()) >= 0) {
                currentSection.add(file);
                if (clusteringComparatorAlone.compare(file.maxKey(), currentMax) > 0) {
                    currentMax = file.maxKey();
                }
            } else {
                sections.add(currentSection);
                currentSection = new ArrayList<>();
                currentSection.add(file);
                currentMax = file.maxKey();
            }
        }
        sections.add(currentSection);
        return sections;
    }

    private long sectionSize(List<DataFileMeta> section) {
        long total = 0;
        for (DataFileMeta file : section) {
            total += file.fileSize();
        }
        return total;
    }

    private SpilledChannel spillToRowBasedFile(DataFileMeta file) throws Exception {
        FileIOChannel.ID channel = ioManager.createChannel();
        KeyValueWithLevelNoReusingSerializer serializer =
                new KeyValueWithLevelNoReusingSerializer(keyType, valueType);
        BlockCompressionFactory compressFactory = BlockCompressionFactory.create(compression);
        int compressBlock = (int) MemorySize.parse("64 kb").getBytes();

        ChannelWithMeta channelWithMeta;
        ChannelWriterOutputView out =
                FileChannelUtil.createOutputView(
                        ioManager, channel, compressFactory, compressBlock);
        try (RecordReader<KeyValue> reader = valueReaderFactory.createRecordReader(file)) {
            RecordIterator<KeyValue> batch;
            KeyValue record;
            while ((batch = reader.readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    serializer.serialize(record, out);
                }
                batch.releaseBatch();
            }
        } finally {
            out.close();
            channelWithMeta =
                    new ChannelWithMeta(channel, out.getBlockCount(), out.getWriteBytes());
        }

        return new SpilledChannel(channelWithMeta, compressFactory, compressBlock, serializer);
    }

    /** Holds metadata for a spilled row-based temp file. */
    private class SpilledChannel {
        private final ChannelWithMeta channel;
        private final BlockCompressionFactory compressFactory;
        private final int compressBlock;
        private final KeyValueWithLevelNoReusingSerializer serializer;

        SpilledChannel(
                ChannelWithMeta channel,
                BlockCompressionFactory compressFactory,
                int compressBlock,
                KeyValueWithLevelNoReusingSerializer serializer) {
            this.channel = channel;
            this.compressFactory = compressFactory;
            this.compressBlock = compressBlock;
            this.serializer = serializer;
        }

        CloseableIterator<KeyValue> createIterator() throws IOException {
            ChannelReaderInputView view =
                    FileChannelUtil.createInputView(
                            ioManager, channel, new ArrayList<>(), compressFactory, compressBlock);
            BinaryRowSerializer rowSerializer = new BinaryRowSerializer(serializer.numFields());
            ChannelReaderInputViewIterator iterator =
                    new ChannelReaderInputViewIterator(view, null, rowSerializer);
            return new SpilledChannelIterator(view, iterator, serializer);
        }
    }

    /** Iterator that reads KeyValue records from a spilled row-based temp file. */
    private static class SpilledChannelIterator implements CloseableIterator<KeyValue> {
        private final ChannelReaderInputView view;
        private final ChannelReaderInputViewIterator iterator;
        private final KeyValueWithLevelNoReusingSerializer serializer;
        private KeyValue next;

        SpilledChannelIterator(
                ChannelReaderInputView view,
                ChannelReaderInputViewIterator iterator,
                KeyValueWithLevelNoReusingSerializer serializer) {
            this.view = view;
            this.iterator = iterator;
            this.serializer = serializer;
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            try {
                BinaryRow row = iterator.next();
                if (row == null) {
                    return false;
                }
                next = serializer.fromRow(row);
                return true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public KeyValue next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            KeyValue result = next;
            next = null;
            return result;
        }

        @Override
        public void close() throws Exception {
            view.getChannel().closeAndDelete();
        }
    }

    /** Entry in the min-heap for multi-way merge. */
    private static class MergeEntry {
        KeyValue currentKeyValue;
        final CloseableIterator<KeyValue> iterator;

        MergeEntry(KeyValue currentKeyValue, CloseableIterator<KeyValue> iterator) {
            this.currentKeyValue = currentKeyValue;
            this.iterator = iterator;
        }
    }
}
