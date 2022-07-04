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
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileReader;
import org.apache.flink.table.store.file.data.DataFileWriter;
import org.apache.flink.table.store.file.mergetree.Levels;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.mergetree.MergeTreeReader;
import org.apache.flink.table.store.file.mergetree.MergeTreeWriter;
import org.apache.flink.table.store.file.mergetree.compact.CompactManager;
import org.apache.flink.table.store.file.mergetree.compact.CompactResult;
import org.apache.flink.table.store.file.mergetree.compact.CompactRewriter;
import org.apache.flink.table.store.file.mergetree.compact.CompactStrategy;
import org.apache.flink.table.store.file.mergetree.compact.CompactTask;
import org.apache.flink.table.store.file.mergetree.compact.CompactUnit;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.UniversalCompaction;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/** {@link FileStoreWrite} for {@link org.apache.flink.table.store.file.KeyValueFileStore}. */
public class KeyValueFileStoreWrite extends AbstractFileStoreWrite<KeyValue> {

    private final DataFileReader.Factory dataFileReaderFactory;
    private final DataFileWriter.Factory dataFileWriterFactory;
    private final Supplier<Comparator<RowData>> keyComparatorSupplier;
    private final MergeFunction mergeFunction;
    private final MergeTreeOptions options;

    public KeyValueFileStoreWrite(
            SchemaManager schemaManager,
            long schemaId,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            int expectedNumBucket,
            Supplier<Comparator<RowData>> keyComparatorSupplier,
            MergeFunction mergeFunction,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            MergeTreeOptions options) {
        super(partitionType, snapshotManager, scan, expectedNumBucket);
        this.dataFileReaderFactory =
                new DataFileReader.Factory(
                        schemaManager, schemaId, keyType, valueType, fileFormat, pathFactory);
        this.dataFileWriterFactory =
                new DataFileWriter.Factory(
                        schemaId,
                        keyType,
                        valueType,
                        fileFormat,
                        pathFactory,
                        options.targetFileSize);
        this.keyComparatorSupplier = keyComparatorSupplier;
        this.mergeFunction = mergeFunction;
        this.options = options;
    }

    @Override
    public RecordWriter<KeyValue> createWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
        return createMergeTreeWriter(
                partition, bucket, scanExistingFileMetas(partition, bucket), compactExecutor);
    }

    @Override
    public RecordWriter<KeyValue> createEmptyWriter(
            BinaryRowData partition, int bucket, ExecutorService compactExecutor) {
        return createMergeTreeWriter(partition, bucket, Collections.emptyList(), compactExecutor);
    }

    @Override
    public Callable<CompactResult> createCompactWriter(
            BinaryRowData partition, int bucket, @Nullable List<DataFileMeta> compactFiles) {
        if (compactFiles == null) {
            compactFiles = scanExistingFileMetas(partition, bucket);
        }
        Comparator<RowData> keyComparator = keyComparatorSupplier.get();
        CompactRewriter rewriter = compactRewriter(partition, bucket, keyComparator);
        Levels levels = new Levels(keyComparator, compactFiles, options.numLevels);
        CompactUnit unit =
                CompactUnit.fromLevelRuns(levels.numberOfLevels() - 1, levels.levelSortedRuns());
        return new CompactTask(keyComparator, options.targetFileSize, rewriter, unit, true);
    }

    private MergeTreeWriter createMergeTreeWriter(
            BinaryRowData partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            ExecutorService compactExecutor) {
        DataFileWriter dataFileWriter = dataFileWriterFactory.create(partition, bucket);
        Comparator<RowData> keyComparator = keyComparatorSupplier.get();
        return new MergeTreeWriter(
                dataFileWriter.keyType(),
                dataFileWriter.valueType(),
                createCompactManager(
                        partition,
                        bucket,
                        new UniversalCompaction(
                                options.maxSizeAmplificationPercent,
                                options.sizeRatio,
                                options.numSortedRunCompactionTrigger),
                        compactExecutor),
                new Levels(keyComparator, restoreFiles, options.numLevels),
                getMaxSequenceNumber(restoreFiles),
                keyComparator,
                mergeFunction.copy(),
                dataFileWriter,
                options.commitForceCompact,
                options.numSortedRunStopTrigger,
                options.enableChangelogFile);
    }

    private CompactManager createCompactManager(
            BinaryRowData partition,
            int bucket,
            CompactStrategy compactStrategy,
            ExecutorService compactExecutor) {
        Comparator<RowData> keyComparator = keyComparatorSupplier.get();
        CompactRewriter rewriter = compactRewriter(partition, bucket, keyComparator);
        return new CompactManager(
                compactExecutor, compactStrategy, keyComparator, options.targetFileSize, rewriter);
    }

    private CompactRewriter compactRewriter(
            BinaryRowData partition, int bucket, Comparator<RowData> keyComparator) {
        DataFileWriter dataFileWriter = dataFileWriterFactory.create(partition, bucket);
        return (outputLevel, dropDelete, sections) ->
                dataFileWriter.write(
                        new RecordReaderIterator<>(
                                new MergeTreeReader(
                                        sections,
                                        dropDelete,
                                        dataFileReaderFactory.create(partition, bucket),
                                        keyComparator,
                                        mergeFunction.copy())),
                        outputLevel);
    }
}
