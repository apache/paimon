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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFilePathFactory;
import org.apache.flink.table.store.file.data.DataFileReader;
import org.apache.flink.table.store.file.data.DataFileWriter;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.mergetree.Levels;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.mergetree.MergeTreeReader;
import org.apache.flink.table.store.file.mergetree.MergeTreeWriter;
import org.apache.flink.table.store.file.mergetree.SortBufferMemTable;
import org.apache.flink.table.store.file.mergetree.compact.CompactManager;
import org.apache.flink.table.store.file.mergetree.compact.CompactStrategy;
import org.apache.flink.table.store.file.mergetree.compact.CompactUnit;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.UniversalCompaction;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.file.writer.AppendOnlyWriter;
import org.apache.flink.table.store.file.writer.CompactWriter;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/** Default implementation of {@link FileStoreWrite}. */
public class FileStoreWriteImpl implements FileStoreWrite {

    private final WriteMode writeMode;
    private final RowType valueType;
    private final DataFileReader.Factory dataFileReaderFactory;
    private final DataFileWriter.Factory dataFileWriterFactory;
    private final FileFormat fileFormat;
    private final Supplier<Comparator<RowData>> keyComparatorSupplier;
    private final MergeFunction mergeFunction;
    private final FileStorePathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final FileStoreScan scan;
    private final MergeTreeOptions options;

    public FileStoreWriteImpl(
            WriteMode writeMode,
            RowType keyType,
            RowType valueType,
            Supplier<Comparator<RowData>> keyComparatorSupplier,
            MergeFunction mergeFunction,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            MergeTreeOptions options) {
        this.valueType = valueType;
        this.dataFileReaderFactory =
                new DataFileReader.Factory(keyType, valueType, fileFormat, pathFactory);
        this.dataFileWriterFactory =
                new DataFileWriter.Factory(
                        keyType, valueType, fileFormat, pathFactory, options.targetFileSize);
        this.writeMode = writeMode;
        this.fileFormat = fileFormat;
        this.keyComparatorSupplier = keyComparatorSupplier;
        this.mergeFunction = mergeFunction;
        this.pathFactory = pathFactory;
        this.snapshotManager = snapshotManager;
        this.scan = scan;
        this.options = options;
    }

    @Override
    public RecordWriter createWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        List<DataFileMeta> existingFileMetas = Lists.newArrayList();
        if (latestSnapshotId != null) {
            // Concat all the DataFileMeta of existing files into existingFileMetas.
            scan.withSnapshot(latestSnapshotId)
                    .withPartitionFilter(Collections.singletonList(partition)).withBucket(bucket)
                    .plan().files().stream()
                    .map(ManifestEntry::file)
                    .forEach(existingFileMetas::add);
        }

        switch (writeMode) {
            case APPEND_ONLY:
                DataFilePathFactory factory =
                        pathFactory.createDataFilePathFactory(partition, bucket);
                long maxSeqNum =
                        existingFileMetas.stream()
                                .map(DataFileMeta::maxSequenceNumber)
                                .max(Long::compare)
                                .orElse(-1L);

                return new AppendOnlyWriter(
                        fileFormat, options.targetFileSize, valueType, maxSeqNum, factory);

            case CHANGE_LOG:
                if (latestSnapshotId == null) {
                    return createEmptyWriter(partition, bucket, compactExecutor);
                } else {
                    return createMergeTreeWriter(
                            partition, bucket, existingFileMetas, compactExecutor);
                }

            default:
                throw new UnsupportedOperationException("Unknown write mode: " + writeMode);
        }
    }

    @Override
    public RecordWriter createEmptyWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
        return createMergeTreeWriter(partition, bucket, Collections.emptyList(), compactExecutor);
    }

    @Override
    public RecordWriter createCompactWriter(
            BinaryRowData partition,
            int bucket,
            ExecutorService compactExecutor,
            List<DataFileMeta> restoredFiles) {
        Levels levels = new Levels(keyComparatorSupplier.get(), restoredFiles, options.numLevels);
        return new CompactWriter(
                CompactUnit.fromLevelRuns(levels.numberOfLevels() - 1, levels.levelSortedRuns()),
                createCompactManager(
                        partition,
                        bucket,
                        (numLevels, runs) ->
                                Optional.of(CompactUnit.fromLevelRuns(numLevels - 1, runs)),
                        compactExecutor));
    }

    private RecordWriter createMergeTreeWriter(
            BinaryRowData partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            ExecutorService compactExecutor) {
        long maxSequenceNumber =
                restoreFiles.stream()
                        .map(DataFileMeta::maxSequenceNumber)
                        .max(Long::compare)
                        .orElse(-1L);
        DataFileWriter dataFileWriter = dataFileWriterFactory.create(partition, bucket);
        Comparator<RowData> keyComparator = keyComparatorSupplier.get();
        return new MergeTreeWriter(
                new SortBufferMemTable(
                        dataFileWriter.keyType(),
                        dataFileWriter.valueType(),
                        options.writeBufferSize,
                        options.pageSize),
                createCompactManager(
                        partition,
                        bucket,
                        new UniversalCompaction(
                                options.maxSizeAmplificationPercent,
                                options.sizeRatio,
                                options.numSortedRunCompactionTrigger),
                        compactExecutor),
                new Levels(keyComparator, restoreFiles, options.numLevels),
                maxSequenceNumber,
                keyComparator,
                mergeFunction.copy(),
                dataFileWriter,
                options.commitForceCompact,
                options.numSortedRunStopTrigger);
    }

    private CompactManager createCompactManager(
            BinaryRowData partition,
            int bucket,
            CompactStrategy compactStrategy,
            ExecutorService compactExecutor) {
        DataFileWriter dataFileWriter = dataFileWriterFactory.create(partition, bucket);
        Comparator<RowData> keyComparator = keyComparatorSupplier.get();
        CompactManager.Rewriter rewriter =
                (outputLevel, dropDelete, sections) ->
                        dataFileWriter.write(
                                new RecordReaderIterator<>(
                                        new MergeTreeReader(
                                                sections,
                                                dropDelete,
                                                dataFileReaderFactory.create(partition, bucket),
                                                keyComparator,
                                                mergeFunction.copy())),
                                outputLevel);
        return new CompactManager(
                compactExecutor, compactStrategy, keyComparator, options.targetFileSize, rewriter);
    }
}
