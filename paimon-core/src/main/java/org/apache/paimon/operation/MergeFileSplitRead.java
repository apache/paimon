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
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.ChainKeyValueFileReaderFactory;
import org.apache.paimon.io.ChainReadContext;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortBufferWriteBuffer;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.ChangelogResult;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.mergetree.compact.LookupChangelogMergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.LookupMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.mergetree.lookup.PersistValueProcessor;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.EmptyRecordReader;
import org.apache.paimon.reader.ReadBatchSizer;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.UserDefinedSeqComparator;
import org.apache.paimon.utils.ValueEqualiserSupplier;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.lookup.LookupStoreFactory.bloomFilterBuilderFactory;
import static org.apache.paimon.mergetree.LookupFile.localFilePrefix;
import static org.apache.paimon.predicate.PredicateBuilder.containsFields;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * An implementation for {@link KeyValueFileStore}, this class handle LSM merging and changelog row
 * kind things, it will force reading fields such as sequence and row_kind.
 *
 * @see RawFileSplitRead If in batch mode and reading raw files, it is recommended to use this read.
 */
public class MergeFileSplitRead implements SplitRead<KeyValue> {

    private final CoreOptions options;
    private final TableSchema tableSchema;
    private final RowType keyType;
    private final FileIO fileIO;
    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final Comparator<InternalRow> keyComparator;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final MergeSorter mergeSorter;
    private final List<String> sequenceFields;
    private final boolean sequenceOrder;

    @Nullable private RowType readKeyType;
    @Nullable private RowType outerReadType;
    @Nullable private IOManager ioManager;

    @Nullable private List<Predicate> filtersForKeys;
    @Nullable private List<Predicate> filtersForAll;

    private boolean forceKeepDelete = false;

    @Nullable private Cache<String, LookupFile> lookupFileCache;
    @Nullable private LookupStoreFactory lookupStoreFactory;
    private final Map<Pair<BinaryRow, Integer>, LookupLevels<KeyValue>> lookupLevelsMap;

    public MergeFileSplitRead(
            CoreOptions options,
            TableSchema schema,
            RowType keyType,
            RowType valueType,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            KeyValueFileReaderFactory.Builder readerFactoryBuilder) {
        this.options = options;
        this.tableSchema = schema;
        this.keyType = keyType;
        this.readerFactoryBuilder = readerFactoryBuilder;
        this.fileIO = readerFactoryBuilder.fileIO();
        this.keyComparator = keyComparator;
        this.mfFactory = mfFactory;
        this.mergeSorter =
                new MergeSorter(
                        CoreOptions.fromMap(tableSchema.options()), keyType, valueType, null);
        this.sequenceFields = options.sequenceField();
        this.sequenceOrder = options.sequenceFieldSortOrderIsAscending();
        int maxLookupStates = options.changelogOnReadLookupStateMaxSize();
        this.lookupLevelsMap =
                new LinkedHashMap<Pair<BinaryRow, Integer>, LookupLevels<KeyValue>>(
                        16, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(
                            Map.Entry<Pair<BinaryRow, Integer>, LookupLevels<KeyValue>> eldest) {
                        if (size() <= maxLookupStates) {
                            return false;
                        }
                        try {
                            eldest.getValue().close();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                        return true;
                    }
                };
    }

    public Comparator<InternalRow> keyComparator() {
        return keyComparator;
    }

    public MergeSorter mergeSorter() {
        return mergeSorter;
    }

    public TableSchema tableSchema() {
        return tableSchema;
    }

    public MergeFileSplitRead withReadKeyType(RowType readKeyType) {
        readerFactoryBuilder.withReadKeyType(readKeyType);
        this.readKeyType = readKeyType;
        return this;
    }

    @Override
    public MergeFileSplitRead withReadType(RowType readType) {
        RowType adjustedReadType = adjustReadType(readType);

        readerFactoryBuilder.withReadValueType(adjustedReadType);
        mergeSorter.setProjectedValueType(adjustedReadType);

        // reset rather than latch: this method may be called again
        outerReadType = adjustedReadType != readType ? readType : null;

        return this;
    }

    /** Returns the value type required internally by the configured merge function. */
    public RowType adjustReadType(RowType readType) {
        RowType tableRowType = tableSchema.logicalRowType();
        RowType adjustedReadType = readType;

        if (!sequenceFields.isEmpty()) {
            // make sure actual readType contains sequence fields
            List<String> readFieldNames = readType.getFieldNames();
            List<DataField> extraFields = new ArrayList<>();
            for (String seqField : sequenceFields) {
                if (!readFieldNames.contains(seqField)) {
                    extraFields.add(tableRowType.getField(seqField));
                }
            }
            if (!extraFields.isEmpty()) {
                List<DataField> allFields = new ArrayList<>(readType.getFields());
                allFields.addAll(extraFields);
                adjustedReadType = new RowType(allFields);
            }
        }
        adjustedReadType = mfFactory.adjustReadType(adjustedReadType);
        return adjustedReadType;
    }

    @Override
    public MergeFileSplitRead withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        this.mergeSorter.setIOManager(ioManager);
        if (mfFactory instanceof LookupMergeFunction.Factory) {
            ((LookupMergeFunction.Factory) mfFactory).withIOManager(ioManager);
        }
        return this;
    }

    @Override
    public MergeFileSplitRead withReadBatchSizer(ReadBatchSizer sizer) {
        readerFactoryBuilder.withReadBatchSizer(sizer);
        return this;
    }

    @Override
    public MergeFileSplitRead forceKeepDelete() {
        this.forceKeepDelete = true;
        return this;
    }

    @Override
    public MergeFileSplitRead withFilter(Predicate predicate) {
        if (predicate == null) {
            return this;
        }

        List<Predicate> allFilters = new ArrayList<>();
        List<Predicate> pkFilters = null;
        List<String> primaryKeys = tableSchema.trimmedPrimaryKeys();
        Set<String> nonPrimaryKeys =
                tableSchema.fieldNames().stream()
                        .filter(name -> !primaryKeys.contains(name))
                        .collect(Collectors.toSet());
        for (Predicate sub : splitAnd(predicate)) {
            allFilters.add(sub);
            if (!containsFields(sub, nonPrimaryKeys)) {
                if (pkFilters == null) {
                    pkFilters = new ArrayList<>();
                }
                // TODO Actually, the index is wrong, but it is OK.
                //  The orc filter just use name instead of index.
                pkFilters.add(sub);
            }
        }
        // Consider this case:
        // Denote (seqNumber, key, value) as a record. We have two overlapping runs in a section:
        //   * First run: (1, k1, 100), (2, k2, 200)
        //   * Second run: (3, k1, 10), (4, k2, 20)
        // If we push down filter "value >= 100" for this section, only the first run will be read,
        // and the second run is lost. This will produce incorrect results.
        //
        // So for sections with overlapping runs, we only push down key filters.
        // For sections with only one run, as each key only appears once, it is OK to push down
        // value filters.
        filtersForAll = allFilters;
        filtersForKeys = pkFilters;
        return this;
    }

    /**
     * Creates a changelog reader. The previous on-read reader from this instance must be closed
     * before calling this method again, because creating a reader may evict cached lookup states.
     */
    public RecordReader<KeyValue> createChangelogOnReadReader(IncrementalSplit split)
            throws IOException {
        checkArgument(
                split.beforeDeletionFiles() == null && split.afterDeletionFiles() == null,
                "Changelog-on-read does not support deletion vectors.");
        checkArgument(
                split.afterFiles().stream().allMatch(file -> file.level() == 0),
                "Changelog-on-read expects only level-0 delta files.");

        LookupLevels<KeyValue> history =
                refreshLookupLevels(split.partition(), split.bucket(), split.beforeFiles());
        history.prepareLookupFiles(lookupFilesToPrepare(split.beforeFiles(), split.afterFiles()));

        UserDefinedSeqComparator sequenceComparator = createUdsComparator();
        MergeFunctionWrapper<ChangelogResult> mergeFunctionWrapper =
                createChangelogOnReadMergeWrapper(history, sequenceComparator);

        KeyValueFileReaderFactory readerFactory =
                readerFactoryBuilder.build(
                        split.partition(),
                        split.bucket(),
                        DeletionVector.emptyFactory(),
                        false,
                        filtersForKeys);
        RecordReader<ChangelogResult> reader =
                MergeTreeReaders.readerForMergeTree(
                        new IntervalPartition(split.afterFiles(), keyComparator).partition(),
                        readerFactory,
                        keyComparator,
                        sequenceComparator,
                        mergeFunctionWrapper,
                        mergeSorter);
        return finishMergeReader(changelogReader(reader), true);
    }

    private MergeFunctionWrapper<ChangelogResult> createChangelogOnReadMergeWrapper(
            LookupLevels<KeyValue> history, @Nullable UserDefinedSeqComparator sequenceComparator) {
        RowType readType = actualReadType();
        Comparator<KeyValue> keyValueComparator = createSequenceComparator(sequenceComparator);
        MergeFunction<KeyValue> historyMerge = mfFactory.create(readType);

        LookupMergeFunction.Factory lookupFactory =
                (LookupMergeFunction.Factory)
                        LookupMergeFunction.wrap(
                                ignored -> mfFactory.create(readType), options, keyType, readType);
        lookupFactory.withIOManager(ioManager);

        RowType equalType = outerReadType == null ? readType : outerReadType;
        RecordEqualiser equaliser =
                options.changelogRowDeduplicate()
                        ? ValueEqualiserSupplier.fromIgnoreFields(
                                        equalType, options.changelogRowDeduplicateIgnoreFields())
                                .get()
                        : null;
        return new LookupChangelogMergeFunctionWrapper<>(
                lookupFactory,
                key -> {
                    try {
                        List<KeyValue> values = history.lookupAll(key);
                        if (values.isEmpty()) {
                            return null;
                        }
                        values.sort(keyValueComparator);
                        historyMerge.reset();
                        values.forEach(historyMerge::add);
                        return historyMerge.getResult().setLevel(1);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                },
                equaliser,
                LookupStrategy.from(false, true, false, false),
                null,
                sequenceComparator);
    }

    private List<DataFileMeta> lookupFilesToPrepare(
            List<DataFileMeta> beforeFiles, List<DataFileMeta> afterFiles) {
        List<DataFileMeta> result = new ArrayList<>();
        for (DataFileMeta before : beforeFiles) {
            for (DataFileMeta after : afterFiles) {
                if (keyComparator.compare(before.maxKey(), after.minKey()) >= 0
                        && keyComparator.compare(before.minKey(), after.maxKey()) <= 0) {
                    result.add(before);
                    break;
                }
            }
        }
        return result;
    }

    private LookupLevels<KeyValue> refreshLookupLevels(
            BinaryRow partition, int bucket, List<DataFileMeta> files) {
        Pair<BinaryRow, Integer> key = Pair.of(partition, bucket);
        LookupLevels<KeyValue> lookup = lookupLevelsMap.get(key);
        if (lookup == null) {
            lookup = createLookupLevels(partition, bucket, files);
            lookupLevelsMap.put(key, lookup);
        } else {
            lookup.getLevels().update(lookup.getLevels().allFiles(), files);
        }
        return lookup;
    }

    private LookupLevels<KeyValue> createLookupLevels(
            BinaryRow partition, int bucket, List<DataFileMeta> files) {
        checkNotNull(ioManager, "IOManager is required for changelog-on-read.");
        KeyValueFileReaderFactory readerFactory =
                readerFactoryBuilder.build(
                        partition, bucket, DeletionVector.emptyFactory(), false, filtersForKeys);
        RowType readType = actualReadType();
        Options configuration = options.toConfiguration();
        return new LookupLevels<>(
                ignored -> readType,
                tableSchema.id(),
                new Levels(keyComparator, files, options.numLevels()),
                keyComparator,
                keyType,
                PersistValueProcessor.factory(readType),
                LookupSerializerFactory.INSTANCE.get(),
                readerFactory::createRecordReader,
                file ->
                        checkNotNull(ioManager)
                                .createChannel(
                                        localFilePrefix(
                                                tableSchema.logicalPartitionType(),
                                                partition,
                                                bucket,
                                                file))
                                .getPathFile(),
                lookupStoreFactory(),
                bloomFilterBuilderFactory(configuration),
                lookupFileCache());
    }

    private LookupStoreFactory lookupStoreFactory() {
        if (lookupStoreFactory == null) {
            lookupStoreFactory =
                    LookupStoreFactory.create(
                            options,
                            new CacheManager(
                                    options.lookupCacheMaxMemory(),
                                    options.lookupCacheHighPrioPoolRatio()),
                            new RowCompactedSerializer(keyType).createSliceComparator());
        }
        return lookupStoreFactory;
    }

    private Cache<String, LookupFile> lookupFileCache() {
        if (lookupFileCache == null) {
            lookupFileCache =
                    LookupFile.createCache(
                            options.toConfiguration().get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                            options.toConfiguration().get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE));
        }
        return lookupFileCache;
    }

    private Comparator<KeyValue> createSequenceComparator(
            @Nullable UserDefinedSeqComparator comparator) {
        if (comparator == null) {
            return Comparator.comparingLong(KeyValue::sequenceNumber);
        }
        return (left, right) -> {
            int result = comparator.compare(left.value(), right.value());
            return result == 0
                    ? Long.compare(left.sequenceNumber(), right.sequenceNumber())
                    : result;
        };
    }

    private RecordReader<KeyValue> changelogReader(RecordReader<ChangelogResult> reader) {
        return new RecordReader<KeyValue>() {
            @Nullable
            @Override
            public RecordIterator<KeyValue> readBatch() throws IOException {
                RecordIterator<ChangelogResult> batch = reader.readBatch();
                if (batch == null) {
                    return null;
                }
                return new RecordIterator<KeyValue>() {
                    private Iterator<KeyValue> current =
                            Collections.<KeyValue>emptyList().iterator();

                    @Nullable
                    @Override
                    public KeyValue next() throws IOException {
                        while (true) {
                            while (!current.hasNext()) {
                                ChangelogResult result = batch.next();
                                if (result == null) {
                                    return null;
                                }
                                current = result.changelogs().iterator();
                            }
                            KeyValue next = current.next();
                            if (options.changelogProducerIgnoreUpdateBefore()
                                    && next.valueKind() == RowKind.UPDATE_BEFORE) {
                                continue;
                            }
                            if (options.changelogProducerIgnoreDelete()
                                    && next.valueKind() == RowKind.DELETE) {
                                continue;
                            }
                            return next;
                        }
                    }

                    @Override
                    public void releaseBatch() {
                        batch.releaseBatch();
                    }
                };
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    public void close() {
        lookupLevelsMap.clear();
        if (lookupFileCache != null) {
            lookupFileCache.invalidateAll();
            lookupFileCache = null;
        }
    }

    @Override
    public RecordReader<KeyValue> createReader(Split split) throws IOException {
        if (split instanceof DataSplit) {
            return createReader((DataSplit) split);
        } else if (split instanceof ChainSplit) {
            return createChainReader((ChainSplit) split);
        } else {
            throw new IllegalArgumentException(
                    "Un-supported split type: " + split.getClass().getName());
        }
    }

    public RecordReader<KeyValue> createReader(DataSplit split) throws IOException {
        if (split.isStreaming() || split.bucket() == BucketMode.POSTPONE_BUCKET) {
            return createNoMergeReader(
                    split.partition(),
                    split.bucket(),
                    split.dataFiles(),
                    split.deletionFiles().orElse(null),
                    split.isStreaming());
        } else {
            return createMergeReader(
                    split.partition(),
                    split.bucket(),
                    split.dataFiles(),
                    split.deletionFiles().orElse(null),
                    forceKeepDelete);
        }
    }

    /** Reads a writer-grouped postpone split with key predicates only. */
    public RecordReader<KeyValue> createPostponeReader(DataSplit split) throws IOException {
        if (split.isStreaming() || split.bucket() != BucketMode.POSTPONE_BUCKET) {
            throw new IllegalArgumentException(
                    "Postpone reader requires a batch split from the postpone bucket.");
        }
        return createNoMergeReader(
                split.partition(),
                split.bucket(),
                split.dataFiles(),
                split.deletionFiles().orElse(null),
                true);
    }

    public RecordReader<KeyValue> createChainReader(ChainSplit chainSplit) throws IOException {
        List<DataFileMeta> files = chainSplit.dataFiles();
        ChainReadContext chainReadContext =
                new ChainReadContext.Builder()
                        .withLogicalPartition(chainSplit.logicalPartition())
                        .withFileBranchPathMapping(chainSplit.fileBranchMapping())
                        .withFileBucketPathMapping(chainSplit.fileBucketPathMapping())
                        .build();
        DeletionVector.Factory dvFactory =
                DeletionVector.factory(fileIO, files, chainSplit.deletionFiles().orElse(null));
        ChainKeyValueFileReaderFactory.Builder builder =
                ChainKeyValueFileReaderFactory.newBuilder(readerFactoryBuilder);
        ChainKeyValueFileReaderFactory overlappedSectionFactory =
                builder.build(null, dvFactory, false, filtersForKeys, chainReadContext);
        ChainKeyValueFileReaderFactory nonOverlappedSectionFactory =
                builder.build(null, dvFactory, false, filtersForAll, chainReadContext);
        return createMergeReader(
                files,
                overlappedSectionFactory,
                nonOverlappedSectionFactory,
                forceKeepDelete,
                new ReducerMergeFunctionWrapper(unwrapLookup(mfFactory).create(actualReadType())));
    }

    public RecordReader<KeyValue> createMergeReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable List<DeletionFile> deletionFiles,
            boolean keepDelete)
            throws IOException {
        // Sections are read by SortMergeReader, which sorts and merges records by keys.
        // So we cannot project keys or else the sorting will be incorrect.
        DeletionVector.Factory dvFactory = DeletionVector.factory(fileIO, files, deletionFiles);
        KeyValueFileReaderFactory overlappedSectionFactory =
                readerFactoryBuilder.build(partition, bucket, dvFactory, false, filtersForKeys);
        KeyValueFileReaderFactory nonOverlappedSectionFactory =
                readerFactoryBuilder.build(partition, bucket, dvFactory, false, filtersForAll);
        return createMergeReader(
                files,
                overlappedSectionFactory,
                nonOverlappedSectionFactory,
                keepDelete,
                new ReducerMergeFunctionWrapper(mfFactory.create(actualReadType())));
    }

    public RecordReader<KeyValue> createMergeReader(
            List<DataFileMeta> files,
            KeyValueFileReaderFactory overlappedSectionFactory,
            KeyValueFileReaderFactory nonOverlappedSectionFactory,
            boolean keepDelete,
            MergeFunctionWrapper<KeyValue> mergeFuncWrapper)
            throws IOException {
        List<ReaderSupplier<KeyValue>> sectionReaders = new ArrayList<>();
        for (List<SortedRun> section : new IntervalPartition(files, keyComparator).partition()) {
            sectionReaders.add(
                    () ->
                            MergeTreeReaders.readerForSection(
                                    section,
                                    section.size() > 1
                                            ? overlappedSectionFactory
                                            : nonOverlappedSectionFactory,
                                    keyComparator,
                                    createUdsComparator(),
                                    mergeFuncWrapper,
                                    mergeSorter));
        }
        return finishMergeReader(ConcatRecordReader.create(sectionReaders), keepDelete);
    }

    /**
     * Merges one real-bucket split with routed postpone records.
     *
     * <p>The postpone reader is consumed and closed eagerly. Its input order defines sequence
     * numbers after the real files, while user-defined sequence fields take precedence. A null
     * split represents a postpone-only bucket.
     */
    public RecordReader<KeyValue> createPostponeMergeReader(
            @Nullable DataSplit realSplit, RecordReader<KeyValue> postponeRecords)
            throws IOException {
        List<DataFileMeta> realFiles =
                realSplit == null ? Collections.emptyList() : realSplit.dataFiles();

        RowType valueType = actualReadType();
        UserDefinedSeqComparator udsComparator = null;
        SortBufferWriteBuffer postponeBuffer = null;
        long nextSequenceNumber = -1L;
        try (RecordReader<KeyValue> records = postponeRecords) {
            RecordReader.RecordIterator<KeyValue> batch;
            while ((batch = records.readBatch()) != null) {
                try {
                    KeyValue record;
                    while ((record = batch.next()) != null) {
                        if (postponeBuffer == null) {
                            for (DataFileMeta file : realFiles) {
                                nextSequenceNumber =
                                        Math.max(nextSequenceNumber, file.maxSequenceNumber());
                            }
                            udsComparator = createUdsComparator();
                            postponeBuffer =
                                    new SortBufferWriteBuffer(
                                            keyType,
                                            valueType,
                                            udsComparator,
                                            mergeSorter.memoryPool(),
                                            options.writeBufferSpillable(),
                                            options.writeBufferSpillDiskSize(),
                                            options.localSortMaxNumFileHandles(),
                                            options.spillCompressOptions(),
                                            ioManager);
                        }
                        nextSequenceNumber = Math.addExact(nextSequenceNumber, 1L);
                        if (!postponeBuffer.put(
                                nextSequenceNumber,
                                record.valueKind(),
                                record.key(),
                                record.value())) {
                            throw new IOException(
                                    "Postpone merge read buffer is full. Enable write-buffer-spillable or increase sort-spill-buffer-size.");
                        }
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        } catch (IOException | RuntimeException e) {
            if (postponeBuffer != null) {
                postponeBuffer.clear();
            }
            throw e;
        }

        if (postponeBuffer == null) {
            return realSplit == null ? new EmptyRecordReader<>() : createReader(realSplit);
        }

        final RecordReader<KeyValue> postponeReader;
        try {
            postponeReader =
                    postponeBuffer.createReader(keyComparator, mfFactory.create(valueType));
        } catch (IOException | RuntimeException e) {
            postponeBuffer.clear();
            throw e;
        }

        if (realFiles.isEmpty()) {
            return finishMergeReader(postponeReader, forceKeepDelete);
        }

        // Postpone records make real-file value filters unsafe before the final merge.
        final RecordReader<KeyValue> realBucketReader;
        try {
            DeletionVector.Factory dvFactory =
                    DeletionVector.factory(
                            fileIO, realFiles, realSplit.deletionFiles().orElse(null));
            KeyValueFileReaderFactory realFileReaderFactory =
                    readerFactoryBuilder.build(
                            realSplit.partition(),
                            realSplit.bucket(),
                            dvFactory,
                            false,
                            filtersForKeys);
            realBucketReader =
                    MergeTreeReaders.readerForMergeTree(
                            new IntervalPartition(realFiles, keyComparator).partition(),
                            realFileReaderFactory,
                            keyComparator,
                            udsComparator,
                            new ReducerMergeFunctionWrapper(mfFactory.create(valueType)),
                            mergeSorter);
        } catch (IOException | RuntimeException e) {
            closeAndSuppress(postponeReader, e);
            throw e;
        }

        RecordReader<KeyValue> reader;
        try {
            reader =
                    mergeSorter.mergeSortNoSpill(
                            Arrays.asList(() -> realBucketReader, () -> postponeReader),
                            keyComparator,
                            udsComparator,
                            new ReducerMergeFunctionWrapper(mfFactory.create(valueType)));
        } catch (IOException | RuntimeException e) {
            closeAndSuppress(realBucketReader, e);
            closeAndSuppress(postponeReader, e);
            throw e;
        }
        return finishMergeReader(reader, forceKeepDelete);
    }

    private static void closeAndSuppress(RecordReader<?> reader, Exception exception) {
        try {
            reader.close();
        } catch (IOException closeException) {
            exception.addSuppressed(closeException);
        }
    }

    private RecordReader<KeyValue> finishMergeReader(
            RecordReader<KeyValue> reader, boolean keepDelete) {

        if (!keepDelete) {
            reader = new DropDeleteReader(reader);
        }

        return projectOuter(projectKey(reader));
    }

    public RecordReader<KeyValue> createNoMergeReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable List<DeletionFile> deletionFiles,
            boolean onlyFilterKey)
            throws IOException {
        KeyValueFileReaderFactory readerFactory =
                readerFactoryBuilder.build(
                        partition,
                        bucket,
                        DeletionVector.factory(fileIO, files, deletionFiles),
                        true,
                        onlyFilterKey ? filtersForKeys : filtersForAll);
        List<ReaderSupplier<KeyValue>> suppliers = new ArrayList<>();
        for (DataFileMeta file : files) {
            suppliers.add(() -> readerFactory.createRecordReader(file));
        }

        return projectOuter(ConcatRecordReader.create(suppliers));
    }

    /**
     * Returns the pushed read type if {@link #withReadType(RowType)} was called, else the default
     * read type. This is the value layout the merge, comparator and serializer run on internally;
     * when a sequence field was appended for merging, {@link #createMergeReader} projects its
     * output back to {@code outerReadType}, so the emitted rows can be narrower than this type.
     */
    public RowType actualReadType() {
        return readerFactoryBuilder.readValueType();
    }

    private RecordReader<KeyValue> projectKey(RecordReader<KeyValue> reader) {
        if (readKeyType == null) {
            return reader;
        }

        ProjectedRow projectedRow = ProjectedRow.from(readKeyType, tableSchema.logicalRowType());
        return reader.transform(kv -> kv.replaceKey(projectedRow.replaceRow(kv.key())));
    }

    private RecordReader<KeyValue> projectOuter(RecordReader<KeyValue> reader) {
        if (outerReadType != null) {
            ProjectedRow projectedRow = ProjectedRow.from(outerReadType, actualReadType());
            reader = reader.transform(kv -> kv.replaceValue(projectedRow.replaceRow(kv.value())));
        }
        return reader;
    }

    @Nullable
    public UserDefinedSeqComparator createUdsComparator() {
        return UserDefinedSeqComparator.create(actualReadType(), sequenceFields, sequenceOrder);
    }

    private static MergeFunctionFactory<KeyValue> unwrapLookup(
            MergeFunctionFactory<KeyValue> factory) {
        return factory instanceof LookupMergeFunction.Factory
                ? ((LookupMergeFunction.Factory) factory).wrapped()
                : factory;
    }
}
