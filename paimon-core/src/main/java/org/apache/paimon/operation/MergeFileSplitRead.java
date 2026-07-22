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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.ChainKeyValueFileReaderFactory;
import org.apache.paimon.io.ChainReadContext;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortBufferWriteBuffer;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.mergetree.compact.LookupMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.SizedReaderSupplier;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.PostponeFileReadTask;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.PredicateBuilder.excludePredicateWithFields;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

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

        // When finalReadType != readType, need to project the outer read type
        if (adjustedReadType != readType) {
            outerReadType = readType;
        }

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
    public MergeFileSplitRead forceKeepDelete() {
        this.forceKeepDelete = true;
        return this;
    }

    @Override
    public MergeFileSplitRead withFilter(Predicate predicate) {
        if (predicate == null) {
            return this;
        }

        List<Predicate> allFilters = splitAnd(predicate);
        List<String> primaryKeys = tableSchema.trimmedPrimaryKeys();
        Set<String> nonPrimaryKeys =
                tableSchema.fieldNames().stream()
                        .filter(name -> !primaryKeys.contains(name))
                        .collect(Collectors.toSet());
        List<Predicate> pkFilters = excludePredicateWithFields(allFilters, nonPrimaryKeys);
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
        filtersForKeys = pkFilters.isEmpty() ? null : pkFilters;
        return this;
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

    /**
     * Reads one physical postpone file and replaces its file-local sequence with the stable
     * relative replay sequence assigned by the execution plan.
     *
     * <p>Only key predicates are applied. Value predicates are unsafe until the routed postpone run
     * has been merged with the real files.
     */
    public RecordReader<KeyValue> createPostponeReader(PostponeFileReadTask task)
            throws IOException {
        DataSplit split = task.split();
        long sequenceBase = task.replaySequenceBase();
        long fileRowCount = split.rowCount();
        long[] recordOrder = {0L};
        return createNoMergeReader(
                        split.partition(),
                        split.bucket(),
                        split.dataFiles(),
                        split.deletionFiles().orElse(null),
                        true)
                .transform(
                        record -> {
                            long order = recordOrder[0]++;
                            if (order >= fileRowCount) {
                                throw new IllegalStateException(
                                        "Postpone file contains more records than its metadata row count "
                                                + fileRowCount
                                                + ".");
                            }
                            return record.setSequenceNumber(Math.addExact(sequenceBase, order));
                        });
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
                files, overlappedSectionFactory, nonOverlappedSectionFactory, forceKeepDelete);
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
                files, overlappedSectionFactory, nonOverlappedSectionFactory, keepDelete);
    }

    public RecordReader<KeyValue> createMergeReader(
            List<DataFileMeta> files,
            KeyValueFileReaderFactory overlappedSectionFactory,
            KeyValueFileReaderFactory nonOverlappedSectionFactory,
            boolean keepDelete)
            throws IOException {
        return finishMergeReader(
                createMergedFileReader(
                        files, overlappedSectionFactory, nonOverlappedSectionFactory),
                keepDelete);
    }

    private RecordReader<KeyValue> createMergedFileReader(
            List<DataFileMeta> files,
            KeyValueFileReaderFactory overlappedSectionFactory,
            KeyValueFileReaderFactory nonOverlappedSectionFactory)
            throws IOException {
        List<ReaderSupplier<KeyValue>> sectionReaders = new ArrayList<>();
        MergeFunctionWrapper<KeyValue> mergeFuncWrapper =
                new ReducerMergeFunctionWrapper(mfFactory.create(actualReadType()));
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
        return ConcatRecordReader.create(sectionReaders);
    }

    /**
     * Merges records routed from the postpone bucket with the real-bucket splits.
     *
     * <p>This follows the normal bucket writer semantics: postpone records are assigned sequence
     * numbers newer than the restored real files, sorted and reduced as one run, and then merged
     * with the existing real runs. The incoming sequence number is treated as a non-negative
     * relative input order. This lets an execution engine preserve the original postpone scan order
     * across its bucket shuffle. The caller is responsible for routing all records to the correct
     * partition and bucket before invoking this method.
     */
    public RecordReader<KeyValue> createPostponeMergeReader(
            List<DataSplit> realSplits, RecordReader<KeyValue> postponeRecords) throws IOException {
        long maxSequenceNumber = -1L;
        for (DataSplit split : realSplits) {
            if (split.isStreaming() || split.bucket() == BucketMode.POSTPONE_BUCKET) {
                throw new IllegalArgumentException(
                        "Postpone merge reader requires batch splits from real buckets.");
            }
            for (DataFileMeta file : split.dataFiles()) {
                maxSequenceNumber = Math.max(maxSequenceNumber, file.maxSequenceNumber());
            }
        }

        SortBufferWriteBuffer postponeBuffer =
                new SortBufferWriteBuffer(
                        keyType,
                        actualReadType(),
                        createUdsComparator(),
                        mergeSorter.memoryPool(),
                        options.writeBufferSpillable(),
                        options.writeBufferSpillDiskSize(),
                        options.localSortMaxNumFileHandles(),
                        options.spillCompressOptions(),
                        ioManager);

        try (RecordReader<KeyValue> records = postponeRecords) {
            RecordReader.RecordIterator<KeyValue> batch;
            while ((batch = records.readBatch()) != null) {
                try {
                    KeyValue record;
                    while ((record = batch.next()) != null) {
                        if (record.sequenceNumber() < 0) {
                            throw new IllegalArgumentException(
                                    "Postpone merge reader requires non-negative relative sequence numbers.");
                        }
                        long sequenceNumber =
                                Math.addExact(
                                        Math.addExact(maxSequenceNumber, 1L),
                                        record.sequenceNumber());
                        if (!postponeBuffer.put(
                                sequenceNumber, record.valueKind(), record.key(), record.value())) {
                            throw new IOException(
                                    "Postpone merge read buffer is full. Enable write-buffer-spillable or increase sort-spill-buffer-size.");
                        }
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        } catch (IOException | RuntimeException e) {
            postponeBuffer.clear();
            throw e;
        }

        RecordReader<KeyValue> postponeReader;
        try {
            postponeReader =
                    postponeBuffer.createReader(keyComparator, mfFactory.create(actualReadType()));
        } catch (IOException | RuntimeException e) {
            postponeBuffer.clear();
            throw e;
        }
        List<SizedReaderSupplier<KeyValue>> readers = new ArrayList<>();
        for (DataSplit split : realSplits) {
            readers.add(
                    new SizedReaderSupplier<KeyValue>() {
                        @Override
                        public long estimateSize() {
                            return split.dataFiles().stream()
                                    .mapToLong(DataFileMeta::fileSize)
                                    .sum();
                        }

                        @Override
                        public RecordReader<KeyValue> get() throws IOException {
                            DeletionVector.Factory dvFactory =
                                    DeletionVector.factory(
                                            fileIO,
                                            split.dataFiles(),
                                            split.deletionFiles().orElse(null));
                            // The postpone run may update every key in this bucket, so value
                            // filters are unsafe for all real-file sections.
                            KeyValueFileReaderFactory factory =
                                    readerFactoryBuilder.build(
                                            split.partition(),
                                            split.bucket(),
                                            dvFactory,
                                            false,
                                            filtersForKeys);
                            return createMergedFileReader(split.dataFiles(), factory, factory);
                        }
                    });
        }
        readers.add(
                new SizedReaderSupplier<KeyValue>() {
                    @Override
                    public long estimateSize() {
                        return 0L;
                    }

                    @Override
                    public RecordReader<KeyValue> get() {
                        return postponeReader;
                    }
                });

        RecordReader<KeyValue> reader;
        try {
            reader =
                    mergeSorter.mergeSort(
                            readers,
                            keyComparator,
                            createUdsComparator(),
                            new ReducerMergeFunctionWrapper(mfFactory.create(actualReadType())));
        } catch (IOException | RuntimeException e) {
            try {
                postponeReader.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
        return finishMergeReader(reader, forceKeepDelete);
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
     * read type.
     */
    private RowType actualReadType() {
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
}
