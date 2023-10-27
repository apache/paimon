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
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.RowBuffer;
import org.apache.paimon.lookup.RocksDBOptions;
import org.apache.paimon.lookup.RocksDBStateFactory;
import org.apache.paimon.lookup.RocksDBValueState;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.options.Options;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.IDMapping;
import org.apache.paimon.utils.KeyValueIterator;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.OffsetRow;
import org.apache.paimon.utils.PositiveIntInt;
import org.apache.paimon.utils.PositiveIntIntSerializer;
import org.apache.paimon.utils.ProjectToRowFunction;
import org.apache.paimon.utils.TypeUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Assign UPDATE_BEFORE and bucket for the input record, output record with bucket. */
public class GlobalIndexAssigner implements Serializable, Closeable {

    private static final long serialVersionUID = 1L;

    private static final String INDEX_NAME = "keyIndex";

    private final AbstractFileStoreTable table;

    private transient IOManager ioManager;

    private transient int bucketIndex;
    private transient ProjectToRowFunction setPartition;
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
    private transient ExistsAction existsAction;

    public GlobalIndexAssigner(Table table) {
        this.table = (AbstractFileStoreTable) table;
    }

    // ================== Start Public API ===================

    public void open(
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
        this.setPartition = new ProjectToRowFunction(table.rowType(), table.partitionKeys());

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

        this.stateFactory =
                new RocksDBStateFactory(
                        path.toString(), options, coreOptions.crossPartitionUpsertIndexTtl());
        RowType keyType = table.schema().logicalTrimmedPrimaryKeysType();
        this.keyIndex =
                stateFactory.valueState(
                        INDEX_NAME,
                        new RowCompactedSerializer(keyType),
                        new PositiveIntIntSerializer(),
                        options.get(RocksDBOptions.LOOKUP_CACHE_ROWS));

        this.partMapping = new IDMapping<>(BinaryRow::copy);
        this.bucketAssigner = new BucketAssigner();
        this.existsAction = fromMergeEngine(coreOptions.mergeEngine());

        // create bootstrap sort buffer
        this.bootstrap = true;
        this.bootstrapKeys =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        RowType.of(DataTypes.BYTES()),
                        RowType.of(DataTypes.BYTES(), DataTypes.BYTES()),
                        coreOptions.writeBufferSize() / 2,
                        coreOptions.pageSize(),
                        coreOptions.localSortMaxNumFileHandles());

        this.bootstrapRecords =
                RowBuffer.getBuffer(
                        ioManager,
                        new HeapMemorySegmentPool(
                                coreOptions.writeBufferSize() / 2, coreOptions.pageSize()),
                        new InternalRowSerializer(table.rowType()),
                        true);
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
        bootstrap = false;
        bootstrapRecords.complete();
        boolean isEmpty = true;
        if (bootstrapKeys.size() > 0) {
            MutableObjectIterator<BinaryRow> keyIterator = bootstrapKeys.sortedIterator();
            BinaryRow row = new BinaryRow(2);
            KeyValueIterator<byte[], byte[]> kvIter =
                    new KeyValueIterator<byte[], byte[]>() {

                        private BinaryRow current;

                        @Override
                        public boolean advanceNext() {
                            try {
                                current = keyIterator.next(row);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                            return current != null;
                        }

                        @Override
                        public byte[] getKey() {
                            return current.getBinary(0);
                        }

                        @Override
                        public byte[] getValue() {
                            return current.getBinary(1);
                        }
                    };

            stateFactory.bulkLoad(keyIndex, kvIter);
            isEmpty = false;
        }

        bootstrapKeys.clear();
        bootstrapKeys = null;

        if (isEmpty && isEndInput) {
            // optimization: bulk load mode
            bulkLoadBootstrapRecords();
        } else {
            loopBootstrapRecords();
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
                switch (existsAction) {
                    case DELETE:
                        {
                            // retract old record
                            BinaryRow previousPart = partMapping.get(previousPartId);
                            InternalRow retract = setPartition.apply(value, previousPart);
                            retract.setRowKind(RowKind.DELETE);
                            collect(retract, previousBucket);
                            bucketAssigner.decrement(previousPart, previousBucket);

                            // new record
                            processNewRecord(partition, partId, key, value);
                            break;
                        }
                    case USE_OLD:
                        {
                            BinaryRow previousPart = partMapping.get(previousPartId);
                            InternalRow newValue = setPartition.apply(value, previousPart);
                            collect(newValue, previousBucket);
                            break;
                        }
                    case SKIP_NEW:
                        // do nothing
                        break;
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
    private void bulkLoadBootstrapRecords() throws Exception {
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
                        keyWithIdType,
                        keyWithRowType,
                        coreOptions.writeBufferSize() / 2,
                        coreOptions.pageSize(),
                        coreOptions.localSortMaxNumFileHandles());
        int id = Integer.MAX_VALUE;
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
                keyIdBuffer.write(keyAndRow);
                id--;
            }
        }
        bootstrapRecords.reset();
        bootstrapRecords = null;

        // 2. loop sorted iterator to assign bucket
        MutableObjectIterator<BinaryRow> iterator = keyIdBuffer.sortedIterator();
        BinaryRow keyWithRow = new BinaryRow(keyWithRowType.getFieldCount());
        OffsetRow row = new OffsetRow(rowType.getFieldCount(), keyWithIdType.getFieldCount());
        BinaryRow currentKey = null;
        while ((keyWithRow = iterator.next(keyWithRow)) != null) {
            row.replace(keyWithRow);
            BinaryRow key = extractor.trimmedPrimaryKey(row);
            if (currentKey == null || !currentKey.equals(key)) {
                // output first record
                BinaryRow partition = extractor.partition(row);
                collect(row, assignBucket(partition));
                currentKey = key.copy();
            }
        }

        keyIdBuffer.clear();
    }

    /** Loop bootstrap records to get and put RocksDB. */
    private void loopBootstrapRecords() throws Exception {
        try (RowBuffer.RowBufferIterator iterator = bootstrapRecords.newIterator()) {
            while (iterator.advanceNext()) {
                processInput(iterator.getRow());
            }
        }

        bootstrapRecords.reset();
        bootstrapRecords = null;
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

    private static class BucketAssigner {

        private final Map<BinaryRow, TreeMap<Integer, Integer>> stats = new HashMap<>();

        public void bootstrapBucket(BinaryRow part, int bucket) {
            TreeMap<Integer, Integer> bucketMap = bucketMap(part);
            Integer count = bucketMap.get(bucket);
            if (count == null) {
                count = 0;
            }
            bucketMap.put(bucket, count + 1);
        }

        public int assignBucket(BinaryRow part, Filter<Integer> filter, int maxCount) {
            TreeMap<Integer, Integer> bucketMap = bucketMap(part);
            for (Map.Entry<Integer, Integer> entry : bucketMap.entrySet()) {
                int bucket = entry.getKey();
                int count = entry.getValue();
                if (filter.test(bucket) && count < maxCount) {
                    bucketMap.put(bucket, count + 1);
                    return bucket;
                }
            }

            for (int i = 0; ; i++) {
                if (filter.test(i) && !bucketMap.containsKey(i)) {
                    bucketMap.put(i, 1);
                    return i;
                }
            }
        }

        public void decrement(BinaryRow part, int bucket) {
            bucketMap(part).compute(bucket, (k, v) -> v == null ? 0 : v - 1);
        }

        private TreeMap<Integer, Integer> bucketMap(BinaryRow part) {
            TreeMap<Integer, Integer> map = stats.get(part);
            if (map == null) {
                map = new TreeMap<>();
                stats.put(part.copy(), map);
            }
            return map;
        }
    }

    private ExistsAction fromMergeEngine(MergeEngine mergeEngine) {
        switch (mergeEngine) {
            case DEDUPLICATE:
                return ExistsAction.DELETE;
            case PARTIAL_UPDATE:
            case AGGREGATE:
                return ExistsAction.USE_OLD;
            case FIRST_ROW:
                return ExistsAction.SKIP_NEW;
            default:
                throw new UnsupportedOperationException("Unsupported engine: " + mergeEngine);
        }
    }

    private enum ExistsAction {
        DELETE,
        USE_OLD,
        SKIP_NEW
    }
}
