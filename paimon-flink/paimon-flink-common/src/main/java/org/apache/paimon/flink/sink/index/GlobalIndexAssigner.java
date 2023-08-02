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

package org.apache.paimon.flink.sink.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.flink.lookup.RocksDBStateFactory;
import org.apache.paimon.flink.lookup.RocksDBValueState;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.IDMapping;
import org.apache.paimon.utils.IntInt;
import org.apache.paimon.utils.IntIntSerializer;
import org.apache.paimon.utils.SerBiFunction;
import org.apache.paimon.utils.SerializableFunction;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiConsumer;

/** Assign UPDATE_BEFORE and bucket for the input record, output record with bucket. */
public class GlobalIndexAssigner<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final AbstractFileStoreTable table;
    private final SerializableFunction<TableSchema, PartitionKeyExtractor<T>> extractorFunction;
    private final SerBiFunction<T, BinaryRow, T> setPartition;
    private final SerBiFunction<T, RowKind, T> setRowKind;

    private transient int targetBucketRowNumber;
    private transient int assignId;
    private transient BiConsumer<T, Integer> collector;
    private transient int numAssigners;
    private transient PartitionKeyExtractor<T> extractor;
    private transient File path;
    private transient RocksDBStateFactory stateFactory;
    private transient RocksDBValueState<InternalRow, IntInt> keyIndex;

    private transient IDMapping<BinaryRow> partMapping;
    private transient BucketAssigner bucketAssigner;
    private transient ExistsAction existsAction;

    public GlobalIndexAssigner(
            Table table,
            SerializableFunction<TableSchema, PartitionKeyExtractor<T>> extractorFunction,
            SerBiFunction<T, BinaryRow, T> setPartition,
            SerBiFunction<T, RowKind, T> setRowKind) {
        this.table = (AbstractFileStoreTable) table;
        this.extractorFunction = extractorFunction;
        this.setPartition = setPartition;
        this.setRowKind = setRowKind;
    }

    public void open(File tmpDir, int numAssigners, int assignId, BiConsumer<T, Integer> collector)
            throws Exception {
        this.numAssigners = numAssigners;
        this.assignId = assignId;
        this.collector = collector;

        CoreOptions coreOptions = table.coreOptions();
        this.targetBucketRowNumber = (int) coreOptions.dynamicBucketTargetRowNum();
        this.extractor = extractorFunction.apply(table.schema());

        // state
        Options options = coreOptions.toConfiguration();
        this.path = new File(tmpDir, "lookup-" + UUID.randomUUID());
        this.stateFactory = new RocksDBStateFactory(path.toString(), options);
        long cacheSize = options.get(CoreOptions.LOOKUP_CACHE_MAX_MEMORY_SIZE).getBytes();
        RowType keyType = table.schema().logicalTrimmedPrimaryKeysType();
        this.keyIndex =
                stateFactory.valueState(
                        "keyIndex",
                        new RowCompactedSerializer(keyType),
                        new IntIntSerializer(),
                        cacheSize);

        this.partMapping = new IDMapping<>(BinaryRow::copy);
        this.bucketAssigner = new BucketAssigner();
        this.existsAction = fromMergeEngine(coreOptions.mergeEngine());
    }

    public void process(T value) throws Exception {
        BinaryRow partition = extractor.partition(value);
        BinaryRow key = extractor.trimmedPrimaryKey(value);

        int partId = partMapping.index(partition);

        IntInt partitionBucket = keyIndex.get(key);
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
                            T retract = setPartition.apply(value, previousPart);
                            retract = setRowKind.apply(retract, RowKind.DELETE);
                            collect(retract, previousBucket);
                            bucketAssigner.decrement(previousPart, previousBucket);

                            // new record
                            processNewRecord(partition, partId, key, value);
                            break;
                        }
                    case USE_OLD:
                        {
                            BinaryRow previousPart = partMapping.get(previousPartId);
                            T newValue = setPartition.apply(value, previousPart);
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

    public void bootstrap(T value) throws IOException {
        BinaryRow partition = extractor.partition(value);
        keyIndex.put(
                extractor.trimmedPrimaryKey(value),
                new IntInt(partMapping.index(partition), assignBucket(partition)));
    }

    private void processNewRecord(BinaryRow partition, int partId, BinaryRow key, T value)
            throws IOException {
        int bucket = assignBucket(partition);
        keyIndex.put(key, new IntInt(partId, bucket));
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

    private void collect(T value, int bucket) {
        collector.accept(value, bucket);
    }

    public void close() throws IOException {
        if (stateFactory != null) {
            stateFactory.close();
            stateFactory = null;
        }

        if (path != null) {
            FileIOUtils.deleteDirectoryQuietly(path);
        }
    }

    private static class BucketAssigner {

        private final Map<BinaryRow, TreeMap<Integer, Integer>> stats = new HashMap<>();

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
