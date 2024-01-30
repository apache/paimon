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
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.NoopCompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.IndexMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.lookup.hash.HashLookupStoreFactory;
import org.apache.paimon.mergetree.ContainsLevels;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeWriter;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.mergetree.compact.CompactStrategy;
import org.apache.paimon.mergetree.compact.FirstRowMergeTreeCompactRewriter;
import org.apache.paimon.mergetree.compact.ForceUpLevel0Compaction;
import org.apache.paimon.mergetree.compact.FullChangelogMergeTreeCompactRewriter;
import org.apache.paimon.mergetree.compact.LookupMergeTreeCompactRewriter;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.MergeTreeCompactManager;
import org.apache.paimon.mergetree.compact.MergeTreeCompactRewriter;
import org.apache.paimon.mergetree.compact.UniversalCompaction;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static org.apache.paimon.io.DataFileMeta.getMaxSequenceNumber;
import static org.apache.paimon.lookup.LookupStoreFactory.bfGenerator;

/** {@link FileStoreWrite} for {@link KeyValueFileStore}. */
public class KeyValueFileStoreWrite extends MemoryFileStoreWrite<KeyValue> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueFileStoreWrite.class);

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final KeyValueFileWriterFactory.Builder writerFactoryBuilder;
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;
    private final Supplier<RecordEqualiser> valueEqualiserSupplier;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final CoreOptions options;
    private final FileIO fileIO;
    private final RowType keyType;
    private final RowType valueType;

    public KeyValueFileStoreWrite(
            FileIO fileIO,
            SchemaManager schemaManager,
            long schemaId,
            String commitUser,
            RowType keyType,
            RowType valueType,
            Supplier<Comparator<InternalRow>> keyComparatorSupplier,
            Supplier<RecordEqualiser> valueEqualiserSupplier,
            MergeFunctionFactory<KeyValue> mfFactory,
            FileStorePathFactory pathFactory,
            Map<String, FileStorePathFactory> format2PathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            @Nullable IndexMaintainer.Factory<KeyValue> indexFactory,
            CoreOptions options,
            KeyValueFieldsExtractor extractor,
            String tableName) {
        super(commitUser, snapshotManager, scan, options, indexFactory, tableName, pathFactory);
        this.fileIO = fileIO;
        this.keyType = keyType;
        this.valueType = valueType;
        this.readerFactoryBuilder =
                KeyValueFileReaderFactory.builder(
                        fileIO,
                        schemaManager,
                        schemaId,
                        keyType,
                        valueType,
                        FileFormatDiscover.of(options),
                        pathFactory,
                        extractor,
                        options);
        this.writerFactoryBuilder =
                KeyValueFileWriterFactory.builder(
                        fileIO,
                        schemaId,
                        keyType,
                        valueType,
                        options.fileFormat(),
                        format2PathFactory,
                        options.targetFileSize());
        this.keyComparatorSupplier = keyComparatorSupplier;
        this.valueEqualiserSupplier = valueEqualiserSupplier;
        this.mfFactory = mfFactory;
        this.options = options;
    }

    @Override
    protected MergeTreeWriter createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Creating merge tree writer for partition {} bucket {} from restored files {}",
                    partition,
                    bucket,
                    restoreFiles);
        }

        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, bucket, options);
        Comparator<InternalRow> keyComparator = keyComparatorSupplier.get();
        Levels levels = new Levels(keyComparator, restoreFiles, options.numLevels());
        UniversalCompaction universalCompaction =
                new UniversalCompaction(
                        options.maxSizeAmplificationPercent(),
                        options.sortedRunSizeRatio(),
                        options.numSortedRunCompactionTrigger(),
                        options.optimizedCompactionInterval());
        CompactStrategy compactStrategy =
                options.changelogProducer() == ChangelogProducer.LOOKUP
                        ? new ForceUpLevel0Compaction(universalCompaction)
                        : universalCompaction;
        CompactManager compactManager =
                createCompactManager(partition, bucket, compactStrategy, compactExecutor, levels);

        return new MergeTreeWriter(
                bufferSpillable(),
                options.localSortMaxNumFileHandles(),
                options.spillCompression(),
                ioManager,
                compactManager,
                getMaxSequenceNumber(restoreFiles),
                keyComparator,
                mfFactory.create(),
                writerFactory,
                options.commitForceCompact(),
                options.changelogProducer(),
                restoreIncrement,
                getWriterMetrics(partition, bucket));
    }

    @VisibleForTesting
    public boolean bufferSpillable() {
        return options.writeBufferSpillable(fileIO.isObjectStore(), isStreamingMode);
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
            CompactRewriter rewriter = createRewriter(partition, bucket, keyComparator, levels);
            return new MergeTreeCompactManager(
                    compactExecutor,
                    levels,
                    compactStrategy,
                    keyComparator,
                    options.compactionFileSize(),
                    options.numSortedRunStopTrigger(),
                    rewriter,
                    getCompactionMetrics(partition, bucket));
        }
    }

    private MergeTreeCompactRewriter createRewriter(
            BinaryRow partition, int bucket, Comparator<InternalRow> keyComparator, Levels levels) {
        KeyValueFileReaderFactory readerFactory = readerFactoryBuilder.build(partition, bucket);
        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, bucket, options);
        MergeSorter mergeSorter = new MergeSorter(options, keyType, valueType, ioManager);
        int maxLevel = options.numLevels() - 1;
        CoreOptions.MergeEngine mergeEngine = options.mergeEngine();
        switch (options.changelogProducer()) {
            case FULL_COMPACTION:
                return new FullChangelogMergeTreeCompactRewriter(
                        maxLevel,
                        mergeEngine,
                        readerFactory,
                        writerFactory,
                        keyComparator,
                        mfFactory,
                        mergeSorter,
                        valueEqualiserSupplier.get(),
                        options.changelogRowDeduplicate());
            case LOOKUP:
                if (mergeEngine == CoreOptions.MergeEngine.FIRST_ROW) {
                    KeyValueFileReaderFactory keyOnlyReader =
                            readerFactoryBuilder
                                    .copyWithoutProjection()
                                    .withValueProjection(new int[0][])
                                    .build(partition, bucket);
                    ContainsLevels containsLevels = createContainsLevels(levels, keyOnlyReader);
                    return new FirstRowMergeTreeCompactRewriter(
                            maxLevel,
                            mergeEngine,
                            containsLevels,
                            readerFactory,
                            writerFactory,
                            keyComparator,
                            mfFactory,
                            mergeSorter,
                            valueEqualiserSupplier.get(),
                            options.changelogRowDeduplicate());
                }
                LookupLevels lookupLevels = createLookupLevels(levels, readerFactory);
                return new LookupMergeTreeCompactRewriter(
                        maxLevel,
                        mergeEngine,
                        lookupLevels,
                        readerFactory,
                        writerFactory,
                        keyComparator,
                        mfFactory,
                        mergeSorter,
                        valueEqualiserSupplier.get(),
                        options.changelogRowDeduplicate());
            default:
                return new MergeTreeCompactRewriter(
                        readerFactory, writerFactory, keyComparator, mfFactory, mergeSorter);
        }
    }

    private LookupLevels createLookupLevels(
            Levels levels, KeyValueFileReaderFactory readerFactory) {
        if (ioManager == null) {
            throw new RuntimeException(
                    "Can not use lookup, there is no temp disk directory to use.");
        }
        Options options = this.options.toConfiguration();
        return new LookupLevels(
                levels,
                keyComparatorSupplier.get(),
                keyType,
                valueType,
                file ->
                        readerFactory.createRecordReader(
                                file.schemaId(), file.fileName(), file.fileSize(), file.level()),
                () -> ioManager.createChannel().getPathFile(),
                new HashLookupStoreFactory(
                        cacheManager,
                        this.options.cachePageSize(),
                        options.get(CoreOptions.LOOKUP_HASH_LOAD_FACTOR)),
                options.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                options.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE),
                bfGenerator(options));
    }

    private ContainsLevels createContainsLevels(
            Levels levels, KeyValueFileReaderFactory readerFactory) {
        if (ioManager == null) {
            throw new RuntimeException(
                    "Can not use lookup, there is no temp disk directory to use.");
        }
        Options options = this.options.toConfiguration();
        return new ContainsLevels(
                levels,
                keyComparatorSupplier.get(),
                keyType,
                file ->
                        readerFactory.createRecordReader(
                                file.schemaId(), file.fileName(), file.fileSize(), file.level()),
                () -> ioManager.createChannel().getPathFile(),
                new HashLookupStoreFactory(
                        cacheManager,
                        this.options.cachePageSize(),
                        options.get(CoreOptions.LOOKUP_HASH_LOAD_FACTOR)),
                options.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                options.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE),
                bfGenerator(options));
    }
}
