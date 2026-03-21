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
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
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
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.sort.db.SimpleLsmKvDb;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.KeyValueWithLevelNoReusingSerializer;
import org.apache.paimon.utils.MutableObjectIterator;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.paimon.utils.VarLengthIntUtils.decodeInt;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;

/**
 * Key Value clustering compact manager for {@link KeyValueFileStore}.
 *
 * <p>Compaction is triggered when unsorted files exist. The compaction process has two phases:
 *
 * <ol>
 *   <li><b>Phase 1</b>: Sort and rewrite all unsorted (level 0) files by clustering columns.
 *   <li><b>Phase 2</b>: Merge sorted files based on clustering column key range overlap. Files are
 *       grouped into sections where each section contains overlapping files. Adjacent sections are
 *       merged when beneficial (overlapping files or small sections) to reduce IO amplification
 *       while consolidating small files.
 * </ol>
 */
public class ClusteringCompactManager extends CompactFutureManager {

    private final RowType keyType;
    private final RowType valueType;
    private final long sortSpillBufferSize;
    private final int pageSize;
    private final int maxNumFileHandles;
    private final int spillThreshold;
    private final CompressOptions compression;
    private final int[] clusteringColumns;
    private final RecordComparator clusteringComparatorAlone;
    private final RecordComparator clusteringComparatorInValue;
    private final IOManager ioManager;
    private final KeyValueFileReaderFactory keyReaderFactory;
    private final KeyValueFileReaderFactory valueReaderFactory;
    private final KeyValueFileWriterFactory writerFactory;
    private final ExecutorService executor;
    private final BucketedDvMaintainer dvMaintainer;
    private final SimpleLsmKvDb kvDb;
    private final boolean lazyGenDeletionFile;
    private final boolean firstRow;
    @Nullable private final CompactionMetrics.Reporter metricsReporter;

    private final ClusteringFiles fileLevels;
    private final long targetFileSize;

    public ClusteringCompactManager(
            RowType keyType,
            RowType valueType,
            List<String> clusteringColumns,
            IOManager ioManager,
            CacheManager cacheManager,
            KeyValueFileReaderFactory keyReaderFactory,
            KeyValueFileReaderFactory valueReaderFactory,
            KeyValueFileWriterFactory writerFactory,
            ExecutorService executor,
            BucketedDvMaintainer dvMaintainer,
            boolean lazyGenDeletionFile,
            List<DataFileMeta> restoreFiles,
            long targetFileSize,
            long sortSpillBufferSize,
            int pageSize,
            int maxNumFileHandles,
            int spillThreshold,
            CompressOptions compression,
            boolean firstRow,
            @Nullable CompactionMetrics.Reporter metricsReporter) {
        this.targetFileSize = targetFileSize;
        this.keyType = keyType;
        this.valueType = valueType;
        this.sortSpillBufferSize = sortSpillBufferSize;
        this.pageSize = pageSize;
        this.maxNumFileHandles = maxNumFileHandles;
        this.spillThreshold = spillThreshold;
        this.compression = compression;
        this.firstRow = firstRow;
        this.clusteringColumns = valueType.projectIndexes(clusteringColumns);
        this.clusteringComparatorAlone =
                CodeGenUtils.newRecordComparator(
                        valueType.project(clusteringColumns).getFieldTypes(),
                        IntStream.range(0, clusteringColumns.size()).toArray(),
                        true);
        this.clusteringComparatorInValue =
                CodeGenUtils.newRecordComparator(
                        valueType.getFieldTypes(), this.clusteringColumns, true);
        this.ioManager = ioManager;
        this.keyReaderFactory = keyReaderFactory;
        this.valueReaderFactory = valueReaderFactory;
        this.writerFactory = writerFactory;
        this.executor = executor;
        this.dvMaintainer = dvMaintainer;
        this.lazyGenDeletionFile = lazyGenDeletionFile;
        this.metricsReporter = metricsReporter;
        this.fileLevels = new ClusteringFiles();
        restoreFiles.forEach(this::addNewFile);

        this.kvDb =
                SimpleLsmKvDb.builder(new File(ioManager.pickRandomTempDir()))
                        .cacheManager(cacheManager)
                        .keyComparator(new RowCompactedSerializer(keyType).createSliceComparator())
                        .build();
        bootstrapKeyIndex(restoreFiles);
    }

    private void bootstrapKeyIndex(List<DataFileMeta> restoreFiles) {
        // Build a combined RowType: key fields + one VARBINARY field for the encoded value
        List<DataField> combinedFields = new ArrayList<>();
        List<DataField> keyFields = keyType.getFields();
        for (int i = 0; i < keyFields.size(); i++) {
            DataField kf = keyFields.get(i);
            combinedFields.add(new DataField(i, kf.name(), kf.type()));
        }
        int valueFieldIndex = keyFields.size();
        combinedFields.add(
                new DataField(
                        valueFieldIndex, "_value_bytes", new VarBinaryType(Integer.MAX_VALUE)));
        RowType combinedType = new RowType(combinedFields);

        int[] sortFields = IntStream.range(0, keyType.getFieldCount()).toArray();
        BinaryExternalSortBuffer sortBuffer =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        combinedType,
                        sortFields,
                        sortSpillBufferSize,
                        pageSize,
                        maxNumFileHandles,
                        compression,
                        MemorySize.MAX_VALUE,
                        false);

        RowCompactedSerializer keySerializer = new RowCompactedSerializer(keyType);
        InternalRow.FieldGetter[] keyFieldGetters =
                new InternalRow.FieldGetter[keyType.getFieldCount()];
        for (int i = 0; i < keyType.getFieldCount(); i++) {
            keyFieldGetters[i] = InternalRow.createFieldGetter(keyType.getTypeAt(i), i);
        }
        try {
            // First pass: read all keys and write to sort buffer
            for (DataFileMeta file : restoreFiles) {
                if (file.level() == 0) {
                    continue;
                }
                int fileId = fileLevels.getFileIdByName(file.fileName());
                try (RecordReader<KeyValue> reader = keyReaderFactory.createRecordReader(file)) {
                    FileRecordIterator<KeyValue> batch;
                    while ((batch = (FileRecordIterator<KeyValue>) reader.readBatch()) != null) {
                        KeyValue kv;
                        while ((kv = batch.next()) != null) {
                            int position = (int) batch.returnedPosition();
                            ByteArrayOutputStream valueOut = new ByteArrayOutputStream(8);
                            encodeInt(valueOut, fileId);
                            encodeInt(valueOut, position);
                            byte[] valueBytes = valueOut.toByteArray();

                            GenericRow combinedRow = new GenericRow(combinedType.getFieldCount());
                            for (int i = 0; i < keyType.getFieldCount(); i++) {
                                combinedRow.setField(
                                        i, keyFieldGetters[i].getFieldOrNull(kv.key()));
                            }
                            combinedRow.setField(valueFieldIndex, valueBytes);
                            sortBuffer.write(combinedRow);
                        }
                        batch.releaseBatch();
                    }
                }
            }

            // Second pass: read sorted output and bulk-load into kvDb
            MutableObjectIterator<BinaryRow> sortedIterator = sortBuffer.sortedIterator();
            BinaryRow binaryRow = new BinaryRow(combinedType.getFieldCount());
            InternalRow.FieldGetter valueGetter =
                    InternalRow.createFieldGetter(
                            new VarBinaryType(Integer.MAX_VALUE), valueFieldIndex);

            Iterator<Map.Entry<byte[], byte[]>> entryIterator =
                    new Iterator<Map.Entry<byte[], byte[]>>() {
                        private BinaryRow current = binaryRow;
                        private boolean hasNext;

                        {
                            advance();
                        }

                        private void advance() {
                            try {
                                current = sortedIterator.next(current);
                                hasNext = current != null;
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public boolean hasNext() {
                            return hasNext;
                        }

                        @Override
                        public Map.Entry<byte[], byte[]> next() {
                            byte[] key = keySerializer.serializeToBytes(current);
                            byte[] value = (byte[]) valueGetter.getFieldOrNull(current);
                            advance();
                            return new AbstractMap.SimpleImmutableEntry<>(key, value);
                        }
                    };

            kvDb.bulkLoad(entryIterator);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            sortBuffer.clear();
        }
    }

    private CloseableIterator<InternalRow> readKeyIterator(DataFileMeta file) throws IOException {
        //noinspection resource
        return keyReaderFactory
                .createRecordReader(file)
                .transform(KeyValue::key)
                .toCloseableIterator();
    }

    @Override
    public boolean shouldWaitForLatestCompaction() {
        return false;
    }

    @Override
    public boolean shouldWaitForPreparingCheckpoint() {
        return false;
    }

    @Override
    public void addNewFile(DataFileMeta file) {
        fileLevels.addNewFile(file);
    }

    @Override
    public List<DataFileMeta> allFiles() {
        return fileLevels.allFiles();
    }

    @Override
    public void triggerCompaction(boolean fullCompaction) {
        taskFuture =
                executor.submit(
                        new CompactTask(metricsReporter) {
                            @Override
                            protected CompactResult doCompact() throws Exception {
                                return compact(fullCompaction);
                            }
                        });
    }

    private CompactResult compact(boolean fullCompaction) throws Exception {
        RowCompactedSerializer keySerializer = new RowCompactedSerializer(keyType);
        KeyValueSerializer kvSerializer = new KeyValueSerializer(keyType, valueType);
        RowType kvSchemaType = KeyValue.schema(keyType, valueType);

        CompactResult result = new CompactResult();

        // Phase 1: Sort and rewrite all unsorted (level 0) files
        List<DataFileMeta> unsortedFiles = fileLevels.unsortedFiles();
        // Snapshot sorted files before Phase 1 to avoid including newly created files in Phase 2
        List<DataFileMeta> existingSortedFiles = fileLevels.sortedFiles();
        for (DataFileMeta file : unsortedFiles) {
            List<DataFileMeta> sortedFiles =
                    sortAndRewriteFiles(singletonList(file), kvSerializer, kvSchemaType);
            updateKeyIndex(keySerializer, file, sortedFiles);
            result.before().add(file);
            result.after().addAll(sortedFiles);
        }

        // Phase 2: Universal Compaction on sorted files that existed before Phase 1.
        // Files produced by Phase 1 are excluded to avoid the same file appearing in both
        // result.before() and result.after().
        List<List<DataFileMeta>> mergeGroups;
        if (fullCompaction) {
            mergeGroups = singletonList(existingSortedFiles);
        } else {
            mergeGroups = pickMergeCandidates(existingSortedFiles);
        }

        for (List<DataFileMeta> mergeGroup : mergeGroups) {
            if (mergeGroup.size() >= 2) {
                List<DataFileMeta> mergedFiles = mergeAndRewriteFiles(mergeGroup, keySerializer);
                result.before().addAll(mergeGroup);
                result.after().addAll(mergedFiles);
            }
        }

        CompactDeletionFile deletionFile =
                lazyGenDeletionFile
                        ? CompactDeletionFile.lazyGeneration(dvMaintainer)
                        : CompactDeletionFile.generateFiles(dvMaintainer);
        result.setDeletionFile(deletionFile);
        return result;
    }

    /**
     * Pick merge candidate groups based on clustering column range overlap and file sizes.
     *
     * <ol>
     *   <li><b>Group into sections</b>: Files are sorted by minKey and grouped into sections based
     *       on clustering column key range overlap. Overlapping files belong to the same section.
     *   <li><b>Merge adjacent sections</b>: Sections that have overlapping files (size &gt;= 2) or
     *       are small (total size &lt; targetFileSize/2) are accumulated together. Large
     *       single-file sections act as barriers, flushing accumulated files into a merge group.
     * </ol>
     *
     * @param sortedFiles all sorted files
     * @return list of merge groups; each group contains files to merge together
     */
    private List<List<DataFileMeta>> pickMergeCandidates(List<DataFileMeta> sortedFiles) {
        if (sortedFiles.size() < 2) {
            return java.util.Collections.emptyList();
        }

        // Step 1: Group files into sections based on clustering column range overlap.
        List<List<DataFileMeta>> sections = groupIntoSections(sortedFiles);

        // Step 2: Merge adjacent sections when beneficial to reduce small files.
        // A section should be merged if it has overlapping files (size >= 2) or is small.
        long smallSectionThreshold = targetFileSize / 2;
        List<List<DataFileMeta>> mergeGroups = new ArrayList<>();
        List<DataFileMeta> pending = new ArrayList<>();

        for (List<DataFileMeta> section : sections) {
            boolean needsMerge = section.size() >= 2;
            boolean isSmall = sectionSize(section) < smallSectionThreshold;

            if (needsMerge || isSmall) {
                // This section should be merged, accumulate it
                pending.addAll(section);
            } else {
                // This section is a single large file, flush pending if any
                if (pending.size() >= 2) {
                    mergeGroups.add(new ArrayList<>(pending));
                }
                pending.clear();
            }
        }

        // Flush remaining pending files
        if (pending.size() >= 2) {
            mergeGroups.add(pending);
        }

        return mergeGroups;
    }

    private long sectionSize(List<DataFileMeta> section) {
        long total = 0;
        for (DataFileMeta file : section) {
            total += file.fileSize();
        }
        return total;
    }

    /**
     * Group files into sections based on clustering column key range overlap. Files are first
     * sorted by minKey, then adjacent files with overlapping ranges are grouped into the same
     * section.
     *
     * @param files input files
     * @return list of sections, each section contains overlapping files
     */
    private List<List<DataFileMeta>> groupIntoSections(List<DataFileMeta> files) {
        // Sort files by minKey to properly detect overlapping ranges
        List<DataFileMeta> sorted = new ArrayList<>(files);
        sorted.sort((a, b) -> clusteringComparatorAlone.compare(a.minKey(), b.minKey()));

        List<List<DataFileMeta>> sections = new ArrayList<>();
        List<DataFileMeta> currentSection = new ArrayList<>();
        currentSection.add(sorted.get(0));
        BinaryRow currentMax = sorted.get(0).maxKey();

        for (int i = 1; i < sorted.size(); i++) {
            DataFileMeta file = sorted.get(i);
            if (clusteringComparatorAlone.compare(currentMax, file.minKey()) >= 0) {
                // Overlaps with current section
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

    /**
     * Update the key index for a single original file replaced by new sorted files. Marks old key
     * positions in deletion vectors and registers new positions.
     */
    private void updateKeyIndex(
            RowCompactedSerializer keySerializer,
            DataFileMeta originalFile,
            List<DataFileMeta> newSortedFiles)
            throws Exception {
        updateKeyIndex(keySerializer, singletonList(originalFile), newSortedFiles);
    }

    /**
     * Update the key index for multiple original files replaced by new sorted files.
     *
     * <p>For DEDUPLICATE mode: mark the old position in deletion vectors, keep the new position.
     *
     * <p>For FIRST_ROW mode: if key exists, mark the new position in deletion vectors (keep the
     * first/old one); if key is new, store the new position.
     */
    private void updateKeyIndex(
            RowCompactedSerializer keySerializer,
            List<DataFileMeta> originalFiles,
            List<DataFileMeta> newSortedFiles)
            throws Exception {
        // Collect file names of original files to avoid self-deletion marking
        java.util.Set<String> originalFileNames = new java.util.HashSet<>();
        for (DataFileMeta file : originalFiles) {
            originalFileNames.add(file.fileName());
        }

        for (DataFileMeta sortedFile : newSortedFiles) {
            int fileId = fileLevels.getFileIdByName(sortedFile.fileName());
            int position = 0;
            try (CloseableIterator<InternalRow> iterator = readKeyIterator(sortedFile)) {
                while (iterator.hasNext()) {
                    byte[] key = keySerializer.serializeToBytes(iterator.next());
                    byte[] oldValue = kvDb.get(key);
                    if (oldValue != null) {
                        ByteArrayInputStream valueIn = new ByteArrayInputStream(oldValue);
                        int oldFileId = decodeInt(valueIn);
                        int oldPosition = decodeInt(valueIn);
                        DataFileMeta oldFile = fileLevels.getFileById(oldFileId);
                        if (oldFile != null && !originalFileNames.contains(oldFile.fileName())) {
                            if (firstRow) {
                                // First-row mode: keep the old (first) record, delete the new one
                                dvMaintainer.notifyNewDeletion(sortedFile.fileName(), position);
                                position++;
                                continue;
                            } else {
                                // Deduplicate mode: keep the new record, delete the old one
                                dvMaintainer.notifyNewDeletion(oldFile.fileName(), oldPosition);
                            }
                        }
                    }
                    ByteArrayOutputStream value = new ByteArrayOutputStream(8);
                    encodeInt(value, fileId);
                    encodeInt(value, position);
                    kvDb.put(key, value.toByteArray());
                    position++;
                }
            }
        }
    }

    /**
     * Sort and rewrite one or more unsorted files by clustering columns. Reads all KeyValue records
     * from the input files, sorts them using an external sort buffer, and writes to new level-1
     * files.
     */
    private List<DataFileMeta> sortAndRewriteFiles(
            List<DataFileMeta> inputFiles, KeyValueSerializer kvSerializer, RowType kvSchemaType)
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

        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingClusteringFileWriter();
        try {
            MutableObjectIterator<BinaryRow> sortedIterator = sortBuffer.sortedIterator();
            BinaryRow binaryRow = new BinaryRow(kvSchemaType.getFieldCount());
            while ((binaryRow = sortedIterator.next(binaryRow)) != null) {
                KeyValue kv = kvSerializer.fromRow(binaryRow);
                writer.write(
                        kv.copy(
                                new InternalRowSerializer(keyType),
                                new InternalRowSerializer(valueType)));
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

        return newFiles;
    }

    /**
     * Merge sorted files using min-heap based multi-way merge. Since all input files are already
     * sorted by clustering columns, we use a PriorityQueue to merge them efficiently without
     * re-sorting. Key index entries are deleted during reading and rebuilt after writing.
     *
     * <p>When the number of input files exceeds spillThreshold, smaller files are spilled to
     * row-based temp files first. Row-based iterators consume much less memory than columnar file
     * readers.
     */
    private List<DataFileMeta> mergeAndRewriteFiles(
            List<DataFileMeta> inputFiles, RowCompactedSerializer keySerializer) throws Exception {
        InternalRowSerializer keyRowSerializer = new InternalRowSerializer(keyType);
        InternalRowSerializer valueRowSerializer = new InternalRowSerializer(valueType);

        // Delete key index entries for all input files before reading
        for (DataFileMeta file : inputFiles) {
            deleteKeyIndexForFile(keySerializer, file);
        }

        // Determine which files to spill to row-based temp files
        List<DataFileMeta> filesToSpill = new ArrayList<>();
        List<DataFileMeta> filesToKeep = new ArrayList<>();
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
            // Add iterators for columnar files (kept in memory)
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

            // Add iterators for row-based spilled files (low memory consumption)
            for (SpilledChannel spilled : spilledChannels) {
                CloseableIterator<KeyValue> iterator = spilled.createIterator();
                openIterators.add(iterator);
                if (iterator.hasNext()) {
                    KeyValue firstKv = iterator.next().copy(keyRowSerializer, valueRowSerializer);
                    minHeap.add(new MergeEntry(firstKv, iterator));
                }
            }

            // Multi-way merge: write records in sorted order
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

            // Remove original files and register new sorted files
            List<DataFileMeta> newFiles = writer.result();
            for (DataFileMeta file : inputFiles) {
                fileLevels.removeFile(file);
            }
            for (DataFileMeta newFile : newFiles) {
                fileLevels.addNewFile(newFile);
            }

            // Rebuild key index for the new files
            for (DataFileMeta newFile : newFiles) {
                int fileId = fileLevels.getFileIdByName(newFile.fileName());
                int position = 0;
                try (CloseableIterator<InternalRow> keyIterator = readKeyIterator(newFile)) {
                    while (keyIterator.hasNext()) {
                        byte[] key = keySerializer.serializeToBytes(keyIterator.next());
                        ByteArrayOutputStream value = new ByteArrayOutputStream(8);
                        encodeInt(value, fileId);
                        encodeInt(value, position);
                        kvDb.put(key, value.toByteArray());
                        position++;
                    }
                }
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

    /**
     * Spill a columnar DataFileMeta to a row-based temp file. Row-based files consume much less
     * memory when reading compared to columnar files.
     */
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

    /** Delete key index entries for the given file from kvDb (only if they still point to it). */
    private void deleteKeyIndexForFile(RowCompactedSerializer keySerializer, DataFileMeta file)
            throws Exception {
        int fileId = fileLevels.getFileIdByName(file.fileName());
        try (CloseableIterator<InternalRow> iterator = readKeyIterator(file)) {
            while (iterator.hasNext()) {
                byte[] key = keySerializer.serializeToBytes(iterator.next());
                byte[] value = kvDb.get(key);
                if (value != null) {
                    int storedFileId = decodeInt(new ByteArrayInputStream(value));
                    if (storedFileId == fileId) {
                        kvDb.delete(key);
                    }
                }
            }
        }
    }

    /** Entry in the min-heap for multi-way merge, holding the current KeyValue and its iterator. */
    private static class MergeEntry {
        KeyValue currentKeyValue;
        final CloseableIterator<KeyValue> iterator;

        MergeEntry(KeyValue currentKeyValue, CloseableIterator<KeyValue> iterator) {
            this.currentKeyValue = currentKeyValue;
            this.iterator = iterator;
        }
    }

    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        return innerGetCompactionResult(blocking);
    }

    @Override
    public boolean compactNotCompleted() {
        return super.compactNotCompleted() || fileLevels.compactNotCompleted();
    }

    @Override
    public void close() throws IOException {
        kvDb.close();
    }
}
