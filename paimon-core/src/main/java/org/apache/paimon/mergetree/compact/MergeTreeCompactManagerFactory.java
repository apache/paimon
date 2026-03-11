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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.NoopCompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.compact.LookupMergeTreeCompactRewriter.FirstRowMergeFunctionWrapperFactory;
import org.apache.paimon.mergetree.compact.LookupMergeTreeCompactRewriter.LookupMergeFunctionWrapperFactory;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.mergetree.lookup.PersistEmptyProcessor;
import org.apache.paimon.mergetree.lookup.PersistPositionProcessor;
import org.apache.paimon.mergetree.lookup.PersistProcessor;
import org.apache.paimon.mergetree.lookup.PersistValueAndPosProcessor;
import org.apache.paimon.mergetree.lookup.PersistValueProcessor;
import org.apache.paimon.mergetree.lookup.RemoteLookupFileManager;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static org.apache.paimon.CoreOptions.ChangelogProducer.FULL_COMPACTION;
import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;
import static org.apache.paimon.lookup.LookupStoreFactory.bfGenerator;
import static org.apache.paimon.mergetree.LookupFile.localFilePrefix;

/** Factory to create {@link MergeTreeCompactManager}. */
public class MergeTreeCompactManagerFactory implements KvCompactionManagerFactory {

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final KeyValueFileWriterFactory.Builder writerFactoryBuilder;
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;
    private final Supplier<FieldsComparator> udsComparatorSupplier;
    private final Supplier<RecordEqualiser> logDedupEqualSupplier;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final CoreOptions options;
    private final RowType keyType;
    private final RowType valueType;
    private final RowType partitionType;
    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final TableSchema schema;
    @Nullable private final RecordLevelExpire recordLevelExpire;
    private final CacheManager cacheManager;

    @Nullable private IOManager ioManager;
    @Nullable private CompactionMetrics compactionMetrics;
    @Nullable private Cache<String, LookupFile> lookupFileCache;

    public MergeTreeCompactManagerFactory(
            KeyValueFileReaderFactory.Builder readerFactoryBuilder,
            KeyValueFileWriterFactory.Builder writerFactoryBuilder,
            Supplier<Comparator<InternalRow>> keyComparatorSupplier,
            Supplier<FieldsComparator> udsComparatorSupplier,
            Supplier<RecordEqualiser> logDedupEqualSupplier,
            MergeFunctionFactory<KeyValue> mfFactory,
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            RowType partitionType,
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            @Nullable RecordLevelExpire recordLevelExpire,
            CacheManager cacheManager) {
        this.readerFactoryBuilder = readerFactoryBuilder;
        this.writerFactoryBuilder = writerFactoryBuilder;
        this.keyComparatorSupplier = keyComparatorSupplier;
        this.udsComparatorSupplier = udsComparatorSupplier;
        this.logDedupEqualSupplier = logDedupEqualSupplier;
        this.mfFactory = mfFactory;
        this.options = options;
        this.keyType = keyType;
        this.valueType = valueType;
        this.partitionType = partitionType;
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.recordLevelExpire = recordLevelExpire;
        this.cacheManager = cacheManager;
    }

    @Override
    public void withIOManager(@Nullable IOManager ioManager) {
        this.ioManager = ioManager;
    }

    @Override
    public void withCompactionMetrics(
            @Nullable CompactionMetrics compactionMetrics) {
        this.compactionMetrics = compactionMetrics;
    }

    @Override
    public CompactManager create(
            BinaryRow partition,
            int bucket,
            ExecutorService compactExecutor,
            List<DataFileMeta> restoreFiles,
            @Nullable BucketedDvMaintainer dvMaintainer) {
        if (options.writeOnly()) {
            return new NoopCompactManager();
        }

        CompactStrategy compactStrategy = createCompactStrategy(options);
        Comparator<InternalRow> keyComparator = keyComparatorSupplier.get();
        Levels levels = new Levels(keyComparator, restoreFiles, options.numLevels());
        @Nullable FieldsComparator userDefinedSeqComparator = udsComparatorSupplier.get();
        CompactRewriter rewriter =
                createRewriter(
                        partition,
                        bucket,
                        keyComparator,
                        userDefinedSeqComparator,
                        levels,
                        dvMaintainer);
        return new MergeTreeCompactManager(
                compactExecutor,
                levels,
                compactStrategy,
                keyComparator,
                options.compactionFileSize(true),
                options.numSortedRunStopTrigger(),
                rewriter,
                compactionMetrics == null
                        ? null
                        : compactionMetrics.createReporter(partition, bucket),
                dvMaintainer,
                options.prepareCommitWaitCompaction(),
                options.needLookup(),
                recordLevelExpire,
                options.forceRewriteAllFiles(),
                options.isChainTable());
    }

    private CompactStrategy createCompactStrategy(CoreOptions options) {
        if (options.needLookup()) {
            Integer compactMaxInterval = null;
            switch (options.lookupCompact()) {
                case GENTLE:
                    compactMaxInterval = options.lookupCompactMaxInterval();
                    break;
                case RADICAL:
                    break;
            }
            return new ForceUpLevel0Compaction(
                    new UniversalCompaction(
                            options.maxSizeAmplificationPercent(),
                            options.sortedRunSizeRatio(),
                            options.numSortedRunCompactionTrigger(),
                            EarlyFullCompaction.create(options),
                            OffPeakHours.create(options)),
                    compactMaxInterval);
        }

        UniversalCompaction universal =
                new UniversalCompaction(
                        options.maxSizeAmplificationPercent(),
                        options.sortedRunSizeRatio(),
                        options.numSortedRunCompactionTrigger(),
                        EarlyFullCompaction.create(options),
                        OffPeakHours.create(options));
        if (options.compactionForceUpLevel0()) {
            return new ForceUpLevel0Compaction(universal, null);
        } else {
            return universal;
        }
    }

    private MergeTreeCompactRewriter createRewriter(
            BinaryRow partition,
            int bucket,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            Levels levels,
            @Nullable BucketedDvMaintainer dvMaintainer) {
        DeletionVector.Factory dvFactory = DeletionVector.factory(dvMaintainer);
        KeyValueFileReaderFactory keyReaderFactory =
                readerFactoryBuilder.build(partition, bucket, dvFactory);
        FileReaderFactory<KeyValue> readerFactory = keyReaderFactory;
        if (recordLevelExpire != null) {
            readerFactory = recordLevelExpire.wrap(readerFactory);
        }
        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, bucket, options);
        MergeSorter mergeSorter = new MergeSorter(options, keyType, valueType, ioManager);
        int maxLevel = options.numLevels() - 1;
        MergeEngine mergeEngine = options.mergeEngine();
        ChangelogProducer changelogProducer = options.changelogProducer();
        LookupStrategy lookupStrategy = options.lookupStrategy();
        if (changelogProducer.equals(FULL_COMPACTION)) {
            return new FullChangelogMergeTreeCompactRewriter(
                    maxLevel,
                    mergeEngine,
                    readerFactory,
                    writerFactory,
                    keyComparator,
                    userDefinedSeqComparator,
                    mfFactory,
                    mergeSorter,
                    logDedupEqualSupplier.get());
        } else if (lookupStrategy.needLookup) {
            PersistProcessor.Factory<?> processorFactory;
            LookupMergeTreeCompactRewriter.MergeFunctionWrapperFactory<?> wrapperFactory;
            FileReaderFactory<KeyValue> lookupReaderFactory = readerFactory;
            if (lookupStrategy.isFirstRow) {
                if (options.deletionVectorsEnabled()) {
                    throw new UnsupportedOperationException(
                            "First row merge engine does not need deletion vectors because there is no deletion of old data in this merge engine.");
                }
                lookupReaderFactory =
                        readerFactoryBuilder
                                .copyWithoutProjection()
                                .withReadValueType(RowType.of())
                                .build(partition, bucket, dvFactory);
                processorFactory = PersistEmptyProcessor.factory();
                wrapperFactory = new FirstRowMergeFunctionWrapperFactory();
            } else {
                if (lookupStrategy.deletionVector) {
                    if (lookupStrategy.produceChangelog
                            || mergeEngine != DEDUPLICATE
                            || !options.sequenceField().isEmpty()) {
                        processorFactory = PersistValueAndPosProcessor.factory(valueType);
                    } else {
                        processorFactory = PersistPositionProcessor.factory();
                    }
                } else {
                    processorFactory = PersistValueProcessor.factory(valueType);
                }
                wrapperFactory =
                        new LookupMergeFunctionWrapperFactory<>(
                                logDedupEqualSupplier.get(),
                                lookupStrategy,
                                UserDefinedSeqComparator.create(valueType, options));
            }
            LookupLevels<?> lookupLevels =
                    createLookupLevels(
                            partition, bucket, levels, processorFactory, lookupReaderFactory);
            RemoteLookupFileManager<?> remoteLookupFileManager = null;
            if (options.lookupRemoteFileEnabled()) {
                remoteLookupFileManager =
                        new RemoteLookupFileManager<>(
                                fileIO,
                                keyReaderFactory.pathFactory(),
                                lookupLevels,
                                options.lookupRemoteLevelThreshold());
            }
            //noinspection rawtypes,unchecked
            return new LookupMergeTreeCompactRewriter(
                    maxLevel,
                    mergeEngine,
                    lookupLevels,
                    readerFactory,
                    writerFactory,
                    keyComparator,
                    userDefinedSeqComparator,
                    mfFactory,
                    mergeSorter,
                    wrapperFactory,
                    lookupStrategy.produceChangelog,
                    dvMaintainer,
                    options,
                    remoteLookupFileManager);
        } else {
            return new MergeTreeCompactRewriter(
                    readerFactory,
                    writerFactory,
                    keyComparator,
                    userDefinedSeqComparator,
                    mfFactory,
                    mergeSorter);
        }
    }

    private <T> LookupLevels<T> createLookupLevels(
            BinaryRow partition,
            int bucket,
            Levels levels,
            PersistProcessor.Factory<T> processorFactory,
            FileReaderFactory<KeyValue> readerFactory) {
        if (ioManager == null) {
            throw new RuntimeException(
                    "Can not use lookup, there is no temp disk directory to use.");
        }
        LookupStoreFactory lookupStoreFactory =
                LookupStoreFactory.create(
                        options,
                        cacheManager,
                        new RowCompactedSerializer(keyType).createSliceComparator());
        Options options = this.options.toConfiguration();
        if (lookupFileCache == null) {
            lookupFileCache =
                    LookupFile.createCache(
                            options.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                            options.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE));
        }
        return new LookupLevels<>(
                schemaId -> schemaManager.schema(schemaId).logicalRowType(),
                schema.id(),
                levels,
                keyComparatorSupplier.get(),
                keyType,
                processorFactory,
                LookupSerializerFactory.INSTANCE.get(),
                readerFactory::createRecordReader,
                file ->
                        ioManager
                                .createChannel(
                                        localFilePrefix(partitionType, partition, bucket, file))
                                .getPathFile(),
                lookupStoreFactory,
                bfGenerator(options),
                lookupFileCache);
    }

    @Override
    public void close() {
        if (lookupFileCache != null) {
            lookupFileCache.invalidateAll();
        }
    }
}
