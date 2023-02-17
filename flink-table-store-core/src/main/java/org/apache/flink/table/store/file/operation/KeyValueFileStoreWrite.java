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

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.compact.CompactManager;
import org.apache.flink.table.store.file.compact.NoopCompactManager;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.io.KeyValueFileReaderFactory;
import org.apache.flink.table.store.file.io.KeyValueFileWriterFactory;
import org.apache.flink.table.store.file.mergetree.Levels;
import org.apache.flink.table.store.file.mergetree.MergeTreeWriter;
import org.apache.flink.table.store.file.mergetree.compact.CompactRewriter;
import org.apache.flink.table.store.file.mergetree.compact.CompactStrategy;
import org.apache.flink.table.store.file.mergetree.compact.FullChangelogMergeTreeCompactRewriter;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunctionFactory;
import org.apache.flink.table.store.file.mergetree.compact.MergeTreeCompactManager;
import org.apache.flink.table.store.file.mergetree.compact.MergeTreeCompactRewriter;
import org.apache.flink.table.store.file.mergetree.compact.UniversalCompaction;
import org.apache.flink.table.store.file.schema.KeyValueFieldsExtractor;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.format.FileFormatDiscover;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static org.apache.flink.table.store.file.io.DataFileMeta.getMaxSequenceNumber;

/** {@link FileStoreWrite} for {@link org.apache.flink.table.store.file.KeyValueFileStore}. */
public class KeyValueFileStoreWrite extends MemoryFileStoreWrite<KeyValue> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueFileStoreWrite.class);

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final KeyValueFileWriterFactory.Builder writerFactoryBuilder;
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final CoreOptions options;
    private final FileIO fileIO;

    public KeyValueFileStoreWrite(
            FileIO fileIO,
            SchemaManager schemaManager,
            long schemaId,
            String commitUser,
            RowType keyType,
            RowType valueType,
            Supplier<Comparator<InternalRow>> keyComparatorSupplier,
            MergeFunctionFactory<KeyValue> mfFactory,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options,
            KeyValueFieldsExtractor extractor) {
        super(commitUser, snapshotManager, scan, options);
        this.fileIO = fileIO;
        this.readerFactoryBuilder =
                KeyValueFileReaderFactory.builder(
                        fileIO,
                        schemaManager,
                        schemaId,
                        keyType,
                        valueType,
                        FileFormatDiscover.of(options),
                        pathFactory,
                        extractor);
        this.writerFactoryBuilder =
                KeyValueFileWriterFactory.builder(
                        fileIO,
                        schemaId,
                        keyType,
                        valueType,
                        options.fileFormat(),
                        pathFactory,
                        options.targetFileSize());
        this.keyComparatorSupplier = keyComparatorSupplier;
        this.mfFactory = mfFactory;
        this.options = options;
    }

    @Override
    public WriterContainer<KeyValue> createWriterContainer(
            BinaryRow partition, int bucket, ExecutorService compactExecutor) {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        RecordWriter<KeyValue> writer =
                createMergeTreeWriter(
                        partition,
                        bucket,
                        scanExistingFileMetas(latestSnapshotId, partition, bucket),
                        compactExecutor);
        return new WriterContainer<>(writer, latestSnapshotId);
    }

    @Override
    public WriterContainer<KeyValue> createEmptyWriterContainer(
            BinaryRow partition, int bucket, ExecutorService compactExecutor) {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        RecordWriter<KeyValue> writer =
                createMergeTreeWriter(partition, bucket, Collections.emptyList(), compactExecutor);
        return new WriterContainer<>(writer, latestSnapshotId);
    }

    private MergeTreeWriter createMergeTreeWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            ExecutorService compactExecutor) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Creating merge tree writer for partition {} bucket {} from restored files {}",
                    partition,
                    bucket,
                    restoreFiles);
        }

        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, bucket, options.fileCompressionPerLevel());
        Comparator<InternalRow> keyComparator = keyComparatorSupplier.get();
        Levels levels = new Levels(keyComparator, restoreFiles, options.numLevels());
        CompactManager compactManager =
                createCompactManager(
                        partition,
                        bucket,
                        new UniversalCompaction(
                                options.maxSizeAmplificationPercent(),
                                options.sortedRunSizeRatio(),
                                options.numSortedRunCompactionTrigger(),
                                options.maxSortedRunNum()),
                        compactExecutor,
                        levels);
        return new MergeTreeWriter(
                bufferSpillable(),
                options.localSortMaxNumFileHandles(),
                ioManager,
                compactManager,
                getMaxSequenceNumber(restoreFiles),
                keyComparator,
                mfFactory.create(),
                writerFactory,
                options.commitForceCompact(),
                options.changelogProducer());
    }

    private boolean bufferSpillable() {
        return options.writeBufferSpillable(fileIO.isObjectStore());
    }

    private CompactManager createCompactManager(
            BinaryRow partition,
            int bucket,
            CompactStrategy compactStrategy,
            ExecutorService compactExecutor,
            Levels levels) {
        if (options.writeOnly()) {
            return new NoopCompactManager();
        } else {
            Comparator<InternalRow> keyComparator = keyComparatorSupplier.get();
            CompactRewriter rewriter = createRewriter(partition, bucket, keyComparator);
            return new MergeTreeCompactManager(
                    compactExecutor,
                    levels,
                    compactStrategy,
                    keyComparator,
                    options.targetFileSize(),
                    options.numSortedRunStopTrigger(),
                    rewriter);
        }
    }

    private MergeTreeCompactRewriter createRewriter(
            BinaryRow partition, int bucket, Comparator<InternalRow> keyComparator) {
        KeyValueFileReaderFactory readerFactory = readerFactoryBuilder.build(partition, bucket);
        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, bucket, options.fileCompressionPerLevel());

        if (options.changelogProducer() == CoreOptions.ChangelogProducer.FULL_COMPACTION) {
            return new FullChangelogMergeTreeCompactRewriter(
                    options.numLevels() - 1,
                    readerFactory,
                    writerFactory,
                    keyComparator,
                    mfFactory);
        } else {
            return new MergeTreeCompactRewriter(
                    readerFactory, writerFactory, keyComparator, mfFactory);
        }
    }
}
