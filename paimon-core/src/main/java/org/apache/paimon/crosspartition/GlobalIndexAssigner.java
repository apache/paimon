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

package org.apache.paimon.crosspartition;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.crosspartition.ExistingProcessor.SortOrder;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.RowBuffer;
import org.apache.paimon.lookup.BulkLoader;
import org.apache.paimon.lookup.RocksDBOptions;
import org.apache.paimon.lookup.RocksDBState;
import org.apache.paimon.lookup.RocksDBStateFactory;
import org.apache.paimon.lookup.RocksDBValueState;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.IDMapping;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.OffsetRow;
import org.apache.paimon.utils.PositiveIntInt;
import org.apache.paimon.utils.PositiveIntIntSerializer;
import org.apache.paimon.utils.ProjectToRowFunction;
import org.apache.paimon.utils.RowIterator;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.apache.paimon.lookup.RocksDBOptions.BLOCK_CACHE_SIZE;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Assign UPDATE_BEFORE and bucket for the input record, output record with bucket. */
public class GlobalIndexAssigner implements Serializable, Closeable {

    private static final long serialVersionUID = 1L;

    private static final String INDEX_NAME = "keyIndex";

    private final FileStoreTable table;

    private transient IOManager ioManager;

    private transient int bucketIndex;
    private transient boolean bootstrap;
    private transient BinaryExternalSortBuffer bootstrapKeys;
    private transient RowBuffer bootstrapRecords;

    private transient int targetBucketRowNumber;
    private transient int assignId;
    private transient BiConsumer<InternalRow, Integer> collector;
    private transient int numAssigners;
    private transient PartitionKeyExtractor<InternalRow> extractor;
    private transient PartitionKeyExtractor<InternalRow> keyPartExtractor;
    private transient File path;
    private transient RocksDBStateFactory stateFactory;
    private transient RocksDBValueState<InternalRow, PositiveIntInt> keyIndex;

    private transient IDMapping<BinaryRow> partMapping;
    private transient BucketAssigner bucketAssigner;
    private transient ExistingProcessor existingProcessor;

    public GlobalIndexAssigner(Table table) {
        this.table = (FileStoreTable) table;
    }

    // ================== Start Public API ===================

    public void open(
            long offHeapMemory,
            IOManager ioManager,
            int numAssigners,
            int assignId,
            BiConsumer<InternalRow, Integer> collector)
            throws Exception {
        this.ioManager = ioManager;
        this.numAssigners = numAssigners;
        this.assignId = assignId;
        this.collector = collector;

        RowType bootstrapType = IndexBootstrap.bootstrapType(table.schema());
        this.bucketIndex = bootstrapType.getFieldCount() - 1;
        ProjectToRowFunction setPartition =
                new ProjectToRowFunction(table.rowType(), table.partitionKeys());

        CoreOptions coreOptions = table.coreOptions();
        this.targetBucketRowNumber = (int) coreOptions.dynamicBucketTargetRowNum();
        this.extractor = new RowPartitionKeyExtractor(table.schema());
        this.keyPartExtractor = new KeyPartPartitionKeyExtractor(table.schema());

        // state
        Options options = coreOptions.toConfiguration();
        String rocksDBDir =
                ioManager
                        .tempDirs()[
                        ThreadLocalRandom.current().nextInt(ioManager.tempDirs().length)];
        this.path = new File(rocksDBDir, "rocksdb-" + UUID.randomUUID());

        Options rocksdbOptions = Options.fromMap(new HashMap<>(options.toMap()));
        // we should avoid too small memory
        long blockCache = Math.max(offHeapMemory, rocksdbOptions.get(BLOCK_CACHE_SIZE).getBytes());
        rocksdbOptions.set(BLOCK_CACHE_SIZE, new MemorySize(blockCache));
        this.stateFactory =
                new RocksDBStateFactory(
                        path.toString(),
                        rocksdbOptions,
                        coreOptions.crossPartitionUpsertIndexTtl());
        RowType keyType = table.schema().logicalTrimmedPrimaryKeysType();
        this.keyIndex =
                stateFactory.valueState(
                        INDEX_NAME,
                        new RowCompactedSerializer(keyType),
                        new PositiveIntIntSerializer(),
                        options.get(RocksDBOptions.LOOKUP_CACHE_ROWS));

        this.partMapping = new IDMapping<>(BinaryRow::copy);
        this.bucketAssigner = new BucketAssigner();
        this.existingProcessor =
                ExistingProcessor.create(
                        coreOptions.mergeEngine(), setPartition, bucketAssigner, this::collect);

        // create bootstrap sort buffer
        this.bootstrap = true;
        this.bootstrapKeys = RocksDBState.createBulkLoadSorter(ioManager, coreOptions);
        this.bootstrapRecords =
                RowBuffer.getBuffer(
                        ioManager,
                        new HeapMemorySegmentPool(
                                coreOptions.writeBufferSize() / 2, coreOptions.pageSize()),
                        new InternalRowSerializer(table.rowType()),
                        true,
                        coreOptions.writeBufferSpillDiskSize(),
                        coreOptions.spillCompression());
    }

    public void bootstrapKey(InternalRow value) throws IOException {
        checkArgument(inBoostrap());
        BinaryRow partition = keyPartExtractor.partition(value);
        BinaryRow key = keyPartExtractor.trimmedPrimaryKey(value);
        int partId = partMapping.index(partition);
        int bucket = value.getInt(bucketIndex);
        bucketAssigner.bootstrapBucket(partition, bucket);
        PositiveIntInt partAndBucket = new PositiveIntInt(partId, bucket);
        bootstrapKeys.write(
                GenericRow.of(keyIndex.serializeKey(key), keyIndex.serializeValue(partAndBucket)));
    }

    public boolean inBoostrap() {
        return bootstrap;
    }

    public void endBoostrap(boolean isEndInput) throws Exception {
        try (CloseableIterator<BinaryRow> iterator = endBoostrapWithoutEmit(isEndInput)) {
            while (iterator.hasNext()) {
                processInput(iterator.next());
            }
        }
    }

    public CloseableIterator<BinaryRow> endBoostrapWithoutEmit(boolean isEndInput)
            throws Exception {
        bootstrap = false;
        bootstrapRecords.complete();
        boolean isEmpty = true;
        if (bootstrapKeys.size() > 0) {
            BulkLoader bulkLoader = keyIndex.createBulkLoader();
            MutableObjectIterator<BinaryRow> keyIterator = bootstrapKeys.sortedIterator();
            BinaryRow row = new BinaryRow(2);
            try {
                while ((row = keyIterator.next(row)) != null) {
                    bulkLoader.write(row.getBinary(0), row.getBinary(1));
                }
            } catch (BulkLoader.WriteException e) {
                throw new RuntimeException(
                        "Exception in bulkLoad, the most suspicious reason is that "
                                + "your data contains duplicates, please check your sink table. "
                                + "(The likelihood of duplication is that you used multiple jobs to write the "
                                + "same dynamic bucket table, it only supports single write)",
                        e.getCause());
            }
            bulkLoader.finish();

            isEmpty = false;
        }

        bootstrapKeys.clear();
        bootstrapKeys = null;

        if (isEmpty && isEndInput) {
            // optimization: bulk load mode
            bulkLoadBootstrapRecords();
            return CloseableIterator.empty();
        } else {
            return bootstrapRecords();
        }
    }

    public void processInput(InternalRow value) throws Exception {
        if (inBoostrap()) {
            bootstrapRecords.put(value);
            return;
        }

        BinaryRow partition = extractor.partition(value);
        BinaryRow key = extractor.trimmedPrimaryKey(value);

        int partId = partMapping.index(partition);

        PositiveIntInt partitionBucket = keyIndex.get(key);
        if (partitionBucket != null) {
            int previousPartId = partitionBucket.i1();
            int previousBucket = partitionBucket.i2();
            if (previousPartId == partId) {
                collect(value, previousBucket);
            } else {
                BinaryRow previousPart = partMapping.get(previousPartId);
                boolean processNewRecord =
                        existingProcessor.processExists(value, previousPart, previousBucket);
                if (processNewRecord) {
                    processNewRecord(partition, partId, key, value);
                }
            }
        } else {
            // new record
            processNewRecord(partition, partId, key, value);
        }
    }

    @Override
    public void close() throws IOException {
        if (stateFactory != null) {
            stateFactory.close();
            stateFactory = null;
        }

        if (path != null) {
            FileIOUtils.deleteDirectoryQuietly(path);
        }
    }

    // ================== End Public API ===================

    /** Sort bootstrap records and assign bucket without RocksDB. */
    private void bulkLoadBootstrapRecords() {
        RowType rowType = table.rowType();
        List<DataType> fields =
                new ArrayList<>(TypeUtils.project(rowType, table.primaryKeys()).getFieldTypes());
        fields.add(DataTypes.INT());
        RowType keyWithIdType = DataTypes.ROW(fields.toArray(new DataType[0]));

        fields.addAll(rowType.getFieldTypes());
        RowType keyWithRowType = DataTypes.ROW(fields.toArray(new DataType[0]));

        // 1. insert into external sort buffer
        CoreOptions coreOptions = table.coreOptions();
        BinaryExternalSortBuffer keyIdBuffer =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        keyWithRowType,
                        IntStream.range(0, keyWithIdType.getFieldCount()).toArray(),
                        coreOptions.writeBufferSize() / 2,
                        coreOptions.pageSize(),
                        coreOptions.localSortMaxNumFileHandles(),
                        coreOptions.spillCompression(),
                        coreOptions.writeBufferSpillDiskSize());

        Function<SortOrder, RowIterator> iteratorFunction =
                sortOrder -> {
                    int id = sortOrder == SortOrder.ASCENDING ? 0 : Integer.MAX_VALUE;
                    GenericRow idRow = new GenericRow(1);
                    JoinedRow keyAndId = new JoinedRow();
                    JoinedRow keyAndRow = new JoinedRow();
                    try (RowBuffer.RowBufferIterator iterator = bootstrapRecords.newIterator()) {
                        while (iterator.advanceNext()) {
                            BinaryRow row = iterator.getRow();
                            BinaryRow key = extractor.trimmedPrimaryKey(row);
                            idRow.setField(0, id);
                            keyAndId.replace(key, idRow);
                            keyAndRow.replace(keyAndId, row);
                            try {
                                keyIdBuffer.write(keyAndRow);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            if (sortOrder == SortOrder.ASCENDING) {
                                id++;
                            } else {
                                id--;
                            }
                        }
                    }
                    bootstrapRecords.reset();
                    bootstrapRecords = null;

                    // 2. loop sorted iterator to assign bucket
                    MutableObjectIterator<BinaryRow> iterator;
                    try {
                        iterator = keyIdBuffer.sortedIterator();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    BinaryRow reuseBinaryRow = new BinaryRow(keyWithRowType.getFieldCount());
                    OffsetRow row =
                            new OffsetRow(rowType.getFieldCount(), keyWithIdType.getFieldCount());
                    return new RowIterator() {
                        @Nullable
                        @Override
                        public InternalRow next() {
                            BinaryRow keyWithRow;
                            try {
                                keyWithRow = iterator.next(reuseBinaryRow);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            if (keyWithRow == null) {
                                return null;
                            }
                            row.replace(keyWithRow);
                            return row;
                        }
                    };
                };

        existingProcessor.bulkLoadNewRecords(
                iteratorFunction,
                extractor::trimmedPrimaryKey,
                extractor::partition,
                this::assignBucket);

        keyIdBuffer.clear();
    }

    private CloseableIterator<BinaryRow> bootstrapRecords() {
        RowBuffer.RowBufferIterator iterator = bootstrapRecords.newIterator();
        return new CloseableIterator<BinaryRow>() {

            boolean hasNext = false;
            boolean advanced = false;

            private void advanceIfNeeded() {
                if (!advanced) {
                    hasNext = iterator.advanceNext();
                    advanced = true;
                }
            }

            @Override
            public boolean hasNext() {
                advanceIfNeeded();
                return hasNext;
            }

            @Override
            public BinaryRow next() {
                advanceIfNeeded();
                if (!hasNext) {
                    throw new NoSuchElementException();
                }

                advanced = false;
                return iterator.getRow();
            }

            @Override
            public void close() {
                iterator.close();
                bootstrapRecords.reset();
                bootstrapRecords = null;
            }
        };
    }

    private void processNewRecord(BinaryRow partition, int partId, BinaryRow key, InternalRow value)
            throws IOException {
        int bucket = assignBucket(partition);
        keyIndex.put(key, new PositiveIntInt(partId, bucket));
        collect(value, bucket);
    }

    private int assignBucket(BinaryRow partition) {
        return bucketAssigner.assignBucket(partition, this::isAssignBucket, targetBucketRowNumber);
    }

    private boolean isAssignBucket(int bucket) {
        return computeAssignId(bucket) == assignId;
    }

    private int computeAssignId(int hash) {
        return Math.abs(hash % numAssigners);
    }

    private void collect(InternalRow value, int bucket) {
        collector.accept(value, bucket);
    }
}
