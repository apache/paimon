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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.typeutils.ArrayDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.TestFileStore;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileTestUtils;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.ValueCountMergeFunction;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.stats.BinaryTableStats;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.stats.StatsTestUtils;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.format.FieldStats;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyValueFileStoreRead}. */
public class KeyValueFileStoreReadTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testKeyProjection() throws Exception {
        // (a, b, c) -> (b, a), c is the partition, all integers are in range [0, 2]

        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numRecords = random.nextInt(1000) + 1;
        List<KeyValue> data = new ArrayList<>();
        Map<Integer, Long> expected = new HashMap<>();
        for (int i = 0; i < numRecords; i++) {
            int a = random.nextInt(3);
            int b = random.nextInt(3);
            int c = random.nextInt(3);
            long delta = random.nextLong(21) - 10;
            // count number of occurrence of (b, a)
            expected.compute(b * 10 + a, (k, v) -> v == null ? delta : v + delta);
            data.add(
                    new KeyValue()
                            .replace(
                                    GenericRowData.of(a, b, c),
                                    i,
                                    RowKind.INSERT,
                                    GenericRowData.of(delta)));
        }
        // remove zero occurrence, it might be merged and discarded by the merge tree
        expected.entrySet().removeIf(e -> e.getValue() == 0);

        RowType partitionType =
                RowType.of(new LogicalType[] {new IntType(false)}, new String[] {"c"});
        RowDataSerializer partitionSerializer = new RowDataSerializer(partitionType);
        RowType keyType =
                RowType.of(
                        new LogicalType[] {
                            new IntType(false), new IntType(false), new IntType(false)
                        },
                        new String[] {"a", "b", "c"});
        RowType projectedKeyType = RowType.of(new IntType(false), new IntType(false));
        RowDataSerializer projectedKeySerializer = new RowDataSerializer(projectedKeyType);
        RowType valueType =
                RowType.of(new LogicalType[] {new BigIntType(false)}, new String[] {"count"});
        RowDataSerializer valueSerializer = new RowDataSerializer(valueType);

        TestFileStore store =
                createStore(partitionType, keyType, valueType, new ValueCountMergeFunction());
        List<KeyValue> readData =
                writeThenRead(
                        data,
                        new int[][] {new int[] {1}, new int[] {0}},
                        null,
                        projectedKeySerializer,
                        valueSerializer,
                        store,
                        kv ->
                                partitionSerializer
                                        .toBinaryRow(GenericRowData.of(kv.key().getInt(2)))
                                        .copy());
        Map<Integer, Long> actual = new HashMap<>();
        for (KeyValue kv : readData) {
            assertThat(kv.key().getArity()).isEqualTo(2);
            int key = kv.key().getInt(0) * 10 + kv.key().getInt(1);
            long delta = kv.value().getLong(0);
            actual.compute(key, (k, v) -> v == null ? delta : v + delta);
        }
        actual.entrySet().removeIf(e -> e.getValue() == 0);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testValueProjection() throws Exception {
        // (dt, hr, shopId, orderId, itemId, priceAmount, comment) -> (shopId, itemId, dt, hr)

        TestKeyValueGenerator gen = new TestKeyValueGenerator();
        int numRecords = ThreadLocalRandom.current().nextInt(1000) + 1;
        List<KeyValue> data = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            data.add(gen.next());
        }
        TestFileStore store =
                createStore(
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        new DeduplicateMergeFunction());

        RowDataSerializer projectedValueSerializer =
                new RowDataSerializer(
                        new IntType(false),
                        new BigIntType(),
                        new VarCharType(false, 8),
                        new IntType(false));
        Map<BinaryRowData, BinaryRowData> expected = store.toKvMap(data);
        expected.replaceAll(
                (k, v) ->
                        projectedValueSerializer
                                .toBinaryRow(
                                        GenericRowData.of(
                                                v.getInt(2),
                                                v.isNullAt(4) ? null : v.getLong(4),
                                                v.getString(0),
                                                v.getInt(1)))
                                .copy());

        List<KeyValue> readData =
                writeThenRead(
                        data,
                        null,
                        new int[][] {new int[] {2}, new int[] {4}, new int[] {0}, new int[] {1}},
                        TestKeyValueGenerator.KEY_SERIALIZER,
                        projectedValueSerializer,
                        store,
                        gen::getPartition);
        for (KeyValue kv : readData) {
            assertThat(kv.value().getArity()).isEqualTo(4);
            BinaryRowData key = TestKeyValueGenerator.KEY_SERIALIZER.toBinaryRow(kv.key());
            BinaryRowData value = projectedValueSerializer.toBinaryRow(kv.value());
            assertThat(expected).containsKey(key);
            assertThat(value).isEqualTo(expected.get(key));
        }
    }

    @Test
    public void testAcceptFilterUnderValueCountMode() throws Exception {
        TestFileStore store =
                createStore(
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        RowType.of(
                                new LogicalType[] {new BigIntType(false)},
                                new String[] {"_VALUE_COUNT"}),
                        new ValueCountMergeFunction());
        // no filter
        KeyValueFileStoreRead read = store.newRead();
        assertThat(read.acceptFilter(true).test(DataFileTestUtils.newFile())).isTrue();
        assertThat(read.acceptFilter(false).test(DataFileTestUtils.newFile())).isTrue();

        // test filter
        PredicateBuilder builder = new PredicateBuilder(TestKeyValueGenerator.DEFAULT_ROW_TYPE);
        DataFileMeta file =
                new DataFileMeta(
                        "",
                        100,
                        100,
                        rowData(10),
                        rowData(200),
                        rowStats(),
                        StatsTestUtils.newEmptyTableStats(), // not used
                        0,
                        100,
                        0,
                        0);

        // test shopId = 123
        read.withFilter(builder.equal(2, 123));
        assertThat(read.acceptFilter(true).test(file)).isTrue();
        assertThat(read.acceptFilter(false).test(file)).isTrue();

        // test shopId = 321
        read.withFilter(builder.equal(2, 321));
        assertThat(read.acceptFilter(true).test(file)).isFalse();
        assertThat(read.acceptFilter(false).test(file)).isFalse();
    }

    @Test
    public void testAcceptFilterUnderPrimaryKeyMode() throws Exception {
        TestFileStore store =
                createStore(
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        new DeduplicateMergeFunction());
        // no filter
        KeyValueFileStoreRead read = store.newRead();
        assertThat(read.acceptFilter(true).test(DataFileTestUtils.newFile())).isTrue();
        assertThat(read.acceptFilter(false).test(DataFileTestUtils.newFile())).isTrue();

        // test filter
        PredicateBuilder builder = new PredicateBuilder(TestKeyValueGenerator.DEFAULT_ROW_TYPE);
        DataFileMeta file =
                new DataFileMeta(
                        "",
                        100,
                        100,
                        rowData(10),
                        rowData(200),
                        new FieldStatsArraySerializer(TestKeyValueGenerator.KEY_TYPE)
                                .toBinary(
                                        new FieldStats[] {
                                            new FieldStats(10, 200, 0),
                                            new FieldStats(100L, 100L, 0)
                                        }),
                        rowStats(),
                        0,
                        100,
                        0,
                        0);

        // shopId = 123 (key only)
        read.withFilter(builder.equal(2, 123));
        assertThat(read.acceptFilter(true).test(file)).isTrue();
        assertThat(read.acceptFilter(false).test(file)).isTrue();

        // shopId = 321 (key only)
        read = store.newRead();
        read.withFilter(builder.equal(2, 321));
        assertThat(read.acceptFilter(true).test(file)).isFalse();
        assertThat(read.acceptFilter(false).test(file)).isFalse();

        // hr = 20 (value only)
        read = store.newRead();
        read.withFilter(builder.equal(1, 20));
        assertThat(read.acceptFilter(true).test(file)).isTrue();
        assertThat(read.acceptFilter(false).test(file)).isTrue();

        // hr = 21 (value only)
        read = store.newRead();
        read.withFilter(builder.equal(1, 21));
        assertThat(read.acceptFilter(true).test(file)).isFalse();
        assertThat(read.acceptFilter(false).test(file)).isTrue();

        // hr = 21 and shopId = 123 (key and value)
        read = store.newRead();
        read.withFilter(PredicateBuilder.and(builder.equal(1, 21), builder.equal(2, 123)));
        assertThat(read.acceptFilter(true).test(file)).isFalse();
        assertThat(read.acceptFilter(false).test(file)).isTrue();
    }

    private static BinaryRowData rowData(int value) {
        BinaryRowData row =
                new BinaryRowData(TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFieldCount());
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, StringData.fromString("2022-08-30"));
        writer.writeInt(1, 20);
        writer.writeInt(2, value);
        writer.writeLong(3, 100L);
        writer.writeLong(4, 200L);
        writer.writeArray(
                5, new GenericArrayData(new int[0]), new ArrayDataSerializer(new IntType()));
        writer.writeString(6, StringData.fromString("a comment"));
        writer.complete();
        return row;
    }

    private static BinaryTableStats rowStats() {
        return new FieldStatsArraySerializer(TestKeyValueGenerator.DEFAULT_ROW_TYPE)
                .toBinary(
                        new FieldStats[] {
                            new FieldStats(
                                    StringData.fromString("2022-08-29"),
                                    StringData.fromString("2022-08-30"),
                                    0),
                            new FieldStats(20, 20, 0),
                            new FieldStats(10, 200, 0),
                            new FieldStats(100L, 100L, 0),
                            new FieldStats(200L, 200L, 0),
                            new FieldStats(null, null, 0), // not used
                            new FieldStats(null, null, 0)
                        });
    }

    private List<KeyValue> writeThenRead(
            List<KeyValue> data,
            int[][] keyProjection,
            int[][] valueProjection,
            RowDataSerializer projectedKeySerializer,
            RowDataSerializer projectedValueSerializer,
            TestFileStore store,
            Function<KeyValue, BinaryRowData> partitionCalculator)
            throws Exception {
        store.commitData(data, partitionCalculator, kv -> 0);
        FileStoreScan scan = store.newScan();
        Map<BinaryRowData, List<ManifestEntry>> filesGroupedByPartition =
                scan.withSnapshot(store.snapshotManager().latestSnapshotId()).plan().files()
                        .stream()
                        .collect(Collectors.groupingBy(ManifestEntry::partition));
        KeyValueFileStoreRead read = store.newRead();
        if (keyProjection != null) {
            read.withKeyProjection(keyProjection);
        }
        if (valueProjection != null) {
            read.withValueProjection(valueProjection);
        }

        List<KeyValue> result = new ArrayList<>();
        for (Map.Entry<BinaryRowData, List<ManifestEntry>> entry :
                filesGroupedByPartition.entrySet()) {
            RecordReader<KeyValue> reader =
                    read.createReader(
                            new Split(
                                    entry.getKey(),
                                    0,
                                    entry.getValue().stream()
                                            .map(ManifestEntry::file)
                                            .collect(Collectors.toList()),
                                    false));
            RecordReaderIterator<KeyValue> actualIterator = new RecordReaderIterator<>(reader);
            while (actualIterator.hasNext()) {
                result.add(
                        actualIterator
                                .next()
                                .copy(projectedKeySerializer, projectedValueSerializer));
            }
        }
        return result;
    }

    private TestFileStore createStore(
            RowType partitionType, RowType keyType, RowType valueType, MergeFunction mergeFunction)
            throws Exception {
        SchemaManager schemaManager = new SchemaManager(new Path(tempDir.toUri()));
        boolean valueCountMode = mergeFunction instanceof ValueCountMergeFunction;
        schemaManager.commitNewVersion(
                new UpdateSchema(
                        valueCountMode ? keyType : valueType,
                        partitionType.getFieldNames(),
                        valueCountMode
                                ? Collections.emptyList()
                                : Stream.concat(
                                                keyType.getFieldNames().stream()
                                                        .map(field -> field.replace("key_", "")),
                                                partitionType.getFieldNames().stream())
                                        .collect(Collectors.toList()),
                        Collections.emptyMap(),
                        null));
        return TestFileStore.create(
                "avro", tempDir.toString(), 1, partitionType, keyType, valueType, mergeFunction);
    }
}
