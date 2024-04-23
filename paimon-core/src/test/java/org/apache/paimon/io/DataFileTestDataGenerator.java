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

package org.apache.paimon.io;

import org.apache.paimon.KeyValue;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.TableStatsCollector;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.statistics.FullFieldStatsCollector;
import org.apache.paimon.stats.FieldStatsArraySerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

/** Random {@link DataFileMeta} generator. */
public class DataFileTestDataGenerator {

    private final int numBuckets;
    private final int memTableCapacity;

    private final List<Map<BinaryRow, List<KeyValue>>> memTables;
    private final TestKeyValueGenerator gen;

    private DataFileTestDataGenerator(int numBuckets, int memTableCapacity) {
        this.numBuckets = numBuckets;
        this.memTableCapacity = memTableCapacity;

        this.memTables = new ArrayList<>();
        for (int i = 0; i < numBuckets; i++) {
            memTables.add(new HashMap<>());
        }
        this.gen = new TestKeyValueGenerator();
    }

    public Data next() {
        while (true) {
            KeyValue kv = gen.next();
            BinaryRow key = (BinaryRow) kv.key();
            BinaryRow partition = gen.getPartition(kv);
            int bucket = (key.hashCode() % numBuckets + numBuckets) % numBuckets;
            List<KeyValue> memTable =
                    memTables.get(bucket).computeIfAbsent(partition, k -> new ArrayList<>());
            memTable.add(kv);

            if (memTable.size() >= memTableCapacity) {
                List<Data> result = createDataFiles(memTable, 0, partition, bucket);
                memTable.clear();
                assert result.size() == 1;
                return result.get(0);
            }
        }
    }

    public List<Data> createDataFiles(
            List<KeyValue> kvs, int level, BinaryRow partition, int bucket) {
        gen.sort(kvs);
        List<KeyValue> combined = new ArrayList<>();
        for (int i = 0; i + 1 < kvs.size(); i++) {
            KeyValue now = kvs.get(i);
            KeyValue next = kvs.get(i + 1);
            if (!now.key().equals(next.key())) {
                combined.add(now);
            }
        }
        combined.add(kvs.get(kvs.size() - 1));

        int capacity = memTableCapacity;
        for (int i = 0; i < level; i++) {
            capacity *= memTableCapacity;
        }
        List<Data> result = new ArrayList<>();
        for (int i = 0; i < combined.size(); i += capacity) {
            result.add(
                    createDataFile(
                            combined.subList(i, Math.min(i + capacity, combined.size())),
                            level,
                            partition,
                            bucket));
        }
        return result;
    }

    private Data createDataFile(List<KeyValue> kvs, int level, BinaryRow partition, int bucket) {
        TableStatsCollector keyStatsCollector =
                new TableStatsCollector(
                        TestKeyValueGenerator.KEY_TYPE,
                        IntStream.range(0, TestKeyValueGenerator.KEY_TYPE.getFieldCount())
                                .mapToObj(
                                        i ->
                                                (FieldStatsCollector.Factory)
                                                        FullFieldStatsCollector::new)
                                .toArray(FieldStatsCollector.Factory[]::new));
        TableStatsCollector valueStatsCollector =
                new TableStatsCollector(
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        IntStream.range(0, TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFieldCount())
                                .mapToObj(
                                        i ->
                                                (FieldStatsCollector.Factory)
                                                        FullFieldStatsCollector::new)
                                .toArray(FieldStatsCollector.Factory[]::new));
        FieldStatsArraySerializer keyStatsSerializer =
                new FieldStatsArraySerializer(TestKeyValueGenerator.KEY_TYPE);
        FieldStatsArraySerializer valueStatsSerializer =
                new FieldStatsArraySerializer(TestKeyValueGenerator.DEFAULT_ROW_TYPE);

        long totalSize = 0;
        BinaryRow minKey = null;
        BinaryRow maxKey = null;
        long minSequenceNumber = Long.MAX_VALUE;
        long maxSequenceNumber = Long.MIN_VALUE;
        for (KeyValue kv : kvs) {
            BinaryRow key = (BinaryRow) kv.key();
            BinaryRow value = (BinaryRow) kv.value();
            totalSize += key.getSizeInBytes() + value.getSizeInBytes();
            keyStatsCollector.collect(key);
            valueStatsCollector.collect(value);
            if (minKey == null || TestKeyValueGenerator.KEY_COMPARATOR.compare(key, minKey) < 0) {
                minKey = key;
            }
            if (maxKey == null || TestKeyValueGenerator.KEY_COMPARATOR.compare(key, maxKey) > 0) {
                maxKey = key;
            }
            minSequenceNumber = Math.min(minSequenceNumber, kv.sequenceNumber());
            maxSequenceNumber = Math.max(maxSequenceNumber, kv.sequenceNumber());
        }

        return new Data(
                partition,
                bucket,
                new DataFileMeta(
                        "data-" + UUID.randomUUID(),
                        totalSize,
                        kvs.size(),
                        minKey,
                        maxKey,
                        keyStatsSerializer.toBinary(keyStatsCollector.extract()),
                        valueStatsSerializer.toBinary(valueStatsCollector.extract()),
                        minSequenceNumber,
                        maxSequenceNumber,
                        0,
                        level,
                        kvs.stream().filter(kv -> kv.valueKind().isRetract()).count(),
                        null),
                kvs);
    }

    /** An in-memory data file. */
    public static class Data {
        public final BinaryRow partition;
        public final int bucket;
        public final DataFileMeta meta;
        public final List<KeyValue> content;

        private Data(BinaryRow partition, int bucket, DataFileMeta meta, List<KeyValue> content) {
            this.partition = partition;
            this.bucket = bucket;
            this.meta = meta;
            this.content = content;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link DataFileTestDataGenerator}. */
    public static class Builder {
        private int numBuckets = 3;
        private int memTableCapacity = 3;

        public Builder numBuckets(int value) {
            this.numBuckets = value;
            return this;
        }

        public Builder memTableCapacity(int value) {
            this.memTableCapacity = value;
            return this;
        }

        public DataFileTestDataGenerator build() {
            return new DataFileTestDataGenerator(numBuckets, memTableCapacity);
        }
    }
}
