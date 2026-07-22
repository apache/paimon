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

import org.apache.paimon.KeyValue;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.PostponeFileReadTask;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.IteratorRecordReader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MergeFileSplitRead}. */
public class MergeFileSplitReadTest {

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
                                    GenericRow.of(a, b, c),
                                    i,
                                    RowKind.INSERT,
                                    GenericRow.of(delta)));
        }
        // remove zero occurrence, it might be merged and discarded by the merge tree
        expected.entrySet().removeIf(e -> e.getValue() == 0);

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            new IntType(false),
                            new IntType(false),
                            new IntType(false),
                            new BigIntType(false)
                        },
                        new String[] {"a", "b", "c", "count"});

        RowType partitionType = rowType.project("c");
        InternalRowSerializer partitionSerializer = new InternalRowSerializer(partitionType);
        List<String> keyNames = Arrays.asList("a", "b", "c");
        RowType keyType = rowType.project(keyNames);
        RowType readKeyType = rowType.project("b", "a");
        InternalRowSerializer projectedKeySerializer = new InternalRowSerializer(readKeyType);
        RowType valueType = rowType.project("count");
        InternalRowSerializer valueSerializer = new InternalRowSerializer(valueType);

        TestFileStore store =
                createStore(
                        partitionType,
                        keyType,
                        valueType,
                        new KeyValueFieldsExtractor() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public List<DataField> keyFields(TableSchema schema) {
                                return schema.fields().stream()
                                        .filter(f -> keyNames.contains(f.name()))
                                        .collect(Collectors.toList());
                            }

                            @Override
                            public List<DataField> valueFields(TableSchema schema) {
                                return Collections.singletonList(
                                        new DataField(
                                                3,
                                                "count",
                                                new org.apache.paimon.types.BigIntType()));
                            }
                        },
                        TestValueCountMergeFunction.factory());
        List<KeyValue> readData =
                writeThenRead(
                        data,
                        readKeyType,
                        null,
                        projectedKeySerializer,
                        valueSerializer,
                        store,
                        kv ->
                                partitionSerializer
                                        .toBinaryRow(GenericRow.of(kv.key().getInt(2)))
                                        .copy());
        Map<Integer, Long> actual = new HashMap<>();
        for (KeyValue kv : readData) {
            assertThat(kv.key().getFieldCount()).isEqualTo(2);
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
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory());

        InternalRowSerializer projectedValueSerializer =
                new InternalRowSerializer(
                        new IntType(false),
                        new BigIntType(),
                        new VarCharType(false, 8),
                        new IntType(false));
        Map<BinaryRow, BinaryRow> expected = store.toKvMap(data);
        expected.replaceAll(
                (k, v) ->
                        projectedValueSerializer
                                .toBinaryRow(
                                        GenericRow.of(
                                                v.getInt(2),
                                                v.isNullAt(4) ? null : v.getLong(4),
                                                v.getString(0),
                                                v.getInt(1)))
                                .copy());

        List<KeyValue> readData =
                writeThenRead(
                        data,
                        null,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE.project(
                                "shopId", "itemId", "dt", "hr"),
                        TestKeyValueGenerator.KEY_SERIALIZER,
                        projectedValueSerializer,
                        store,
                        gen::getPartition);
        for (KeyValue kv : readData) {
            assertThat(kv.value().getFieldCount()).isEqualTo(4);
            BinaryRow key = TestKeyValueGenerator.KEY_SERIALIZER.toBinaryRow(kv.key());
            BinaryRow value = projectedValueSerializer.toBinaryRow(kv.value());
            assertThat(expected).containsKey(key);
            assertThat(value).isEqualTo(expected.get(key));
        }
    }

    @Test
    public void testPostponeMergeReaderWithValueProjection() throws Exception {
        RowType keyType =
                RowType.of(
                        new DataField(
                                SpecialFields.KEY_FIELD_ID_START,
                                SpecialFields.KEY_FIELD_PREFIX + "k",
                                new IntType(false)));
        RowType valueType =
                RowType.of(
                        new DataField(0, "k", new IntType(false)),
                        new DataField(1, "v", new VarCharType()));
        TestFileStore store =
                createStore(
                        RowType.of(),
                        keyType,
                        valueType,
                        PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory());

        List<KeyValue> realRecords =
                Arrays.asList(
                        keyValue(1, 0, RowKind.INSERT, "base-1", false),
                        keyValue(2, 1, RowKind.INSERT, "base-2", false));
        store.commitData(realRecords, ignored -> BinaryRow.EMPTY_ROW, ignored -> 0);

        long snapshotId = store.snapshotManager().latestSnapshotId();
        List<DataSplit> realSplits =
                Collections.singletonList(
                        DataSplit.builder()
                                .withSnapshot(snapshotId)
                                .withPartition(BinaryRow.EMPTY_ROW)
                                .withBucket(0)
                                .withDataFiles(
                                        store.newScan().withSnapshot(snapshotId).plan().files()
                                                .stream()
                                                .map(ManifestEntry::file)
                                                .collect(Collectors.toList()))
                                .withBucketPath("not used")
                                .build());
        List<KeyValue> postponeRecords =
                Arrays.asList(
                        // The execution engine may deliver shuffled records out of source order.
                        // Their relative sequence numbers must restore the original order in Core.
                        keyValue(1, 2, RowKind.INSERT, "newest-1", true),
                        keyValue(1, 0, RowKind.INSERT, "new-1", true),
                        keyValue(2, 0, RowKind.DELETE, null, true),
                        keyValue(3, 0, RowKind.INSERT, "new-3", true));

        RowType projectedValueType = valueType.project("v");
        try (IOManager ioManager = IOManager.create(tempDir.resolve("io").toString());
                RecordReaderIterator<KeyValue> reader =
                        new RecordReaderIterator<>(
                                store.newRead()
                                        .withReadType(projectedValueType)
                                        .withIOManager(ioManager)
                                        .createPostponeMergeReader(
                                                realSplits,
                                                new IteratorRecordReader<>(
                                                        postponeRecords.iterator())))) {
            Map<Integer, String> actual = new HashMap<>();
            while (reader.hasNext()) {
                KeyValue keyValue = reader.next();
                assertThat(keyValue.value().getFieldCount()).isEqualTo(1);
                actual.put(keyValue.key().getInt(0), keyValue.value().getString(0).toString());
            }
            Map<Integer, String> expected = new HashMap<>();
            expected.put(1, "newest-1");
            expected.put(3, "new-3");
            assertThat(actual).containsExactlyInAnyOrderEntriesOf(expected);
        }
    }

    @Test
    public void testPostponeReader() throws Exception {
        RowType keyType =
                RowType.of(
                        new DataField(
                                SpecialFields.KEY_FIELD_ID_START,
                                SpecialFields.KEY_FIELD_PREFIX + "k",
                                new IntType(false)));
        RowType valueType =
                RowType.of(
                        new DataField(0, "k", new IntType(false)),
                        new DataField(1, "v", new VarCharType()));
        TestFileStore store =
                createStore(
                        BucketMode.POSTPONE_BUCKET,
                        RowType.of(),
                        keyType,
                        valueType,
                        PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory());

        store.commitData(
                Arrays.asList(
                        keyValue(1, 100, RowKind.INSERT, "one", false),
                        keyValue(2, 200, RowKind.INSERT, "two", false)),
                ignored -> BinaryRow.EMPTY_ROW,
                ignored -> BucketMode.POSTPONE_BUCKET);

        long snapshotId = store.snapshotManager().latestSnapshotId();
        List<ManifestEntry> files =
                store.newScan()
                        .withSnapshot(snapshotId)
                        .withBucket(BucketMode.POSTPONE_BUCKET)
                        .plan()
                        .files();
        assertThat(files).hasSize(1);
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(snapshotId)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(BucketMode.POSTPONE_BUCKET)
                        .withDataFiles(Collections.singletonList(files.get(0).file()))
                        .withBucketPath("not used")
                        .build();

        Map<Integer, Long> sequences = new HashMap<>();
        try (RecordReaderIterator<KeyValue> reader =
                new RecordReaderIterator<>(
                        store.newRead()
                                .createPostponeReader(new PostponeFileReadTask(split, 7L)))) {
            while (reader.hasNext()) {
                KeyValue record = reader.next();
                sequences.put(record.key().getInt(0), record.sequenceNumber());
            }
        }
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 7L);
        expected.put(2, 8L);
        assertThat(sequences).containsExactlyInAnyOrderEntriesOf(expected);
    }

    private static KeyValue keyValue(
            int key, long sequenceNumber, RowKind kind, String value, boolean projected) {
        InternalRow row =
                projected
                        ? GenericRow.of(value == null ? null : BinaryString.fromString(value))
                        : GenericRow.of(key, value == null ? null : BinaryString.fromString(value));
        return new KeyValue().replace(GenericRow.of(key), sequenceNumber, kind, row);
    }

    private List<KeyValue> writeThenRead(
            List<KeyValue> data,
            RowType readKeyType,
            RowType readValueType,
            InternalRowSerializer projectedKeySerializer,
            InternalRowSerializer projectedValueSerializer,
            TestFileStore store,
            Function<KeyValue, BinaryRow> partitionCalculator)
            throws Exception {
        store.commitData(data, partitionCalculator, kv -> 0);
        FileStoreScan scan = store.newScan();
        Long snapshotId = store.snapshotManager().latestSnapshotId();
        Map<BinaryRow, List<ManifestEntry>> filesGroupedByPartition =
                scan.withSnapshot(snapshotId).plan().files().stream()
                        .collect(Collectors.groupingBy(ManifestEntry::partition));
        MergeFileSplitRead read = store.newRead();
        if (readKeyType != null) {
            read.withReadKeyType(readKeyType);
        }
        if (readValueType != null) {
            read.withReadType(readValueType);
        }

        List<KeyValue> result = new ArrayList<>();
        for (Map.Entry<BinaryRow, List<ManifestEntry>> entry : filesGroupedByPartition.entrySet()) {
            RecordReader<KeyValue> reader =
                    read.createReader(
                            DataSplit.builder()
                                    .withSnapshot(snapshotId)
                                    .withPartition(entry.getKey())
                                    .withBucket(0)
                                    .withDataFiles(
                                            entry.getValue().stream()
                                                    .map(ManifestEntry::file)
                                                    .collect(Collectors.toList()))
                                    .withBucketPath("not used")
                                    .build());
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
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            KeyValueFieldsExtractor extractor,
            MergeFunctionFactory<KeyValue> mfFactory)
            throws Exception {
        return createStore(1, partitionType, keyType, valueType, extractor, mfFactory);
    }

    private TestFileStore createStore(
            int numBuckets,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            KeyValueFieldsExtractor extractor,
            MergeFunctionFactory<KeyValue> mfFactory)
            throws Exception {
        Path path = new Path(tempDir.toUri());
        SchemaManager schemaManager = new SchemaManager(FileIOFinder.find(path), path);
        boolean valueCountMode = mfFactory.create() instanceof TestValueCountMergeFunction;
        Schema schema =
                new Schema(
                        (valueCountMode ? keyType : valueType).getFields(),
                        partitionType.getFieldNames(),
                        valueCountMode
                                ? Collections.emptyList()
                                : Stream.concat(
                                                keyType.getFieldNames().stream()
                                                        .map(
                                                                field ->
                                                                        field.replace(
                                                                                SpecialFields
                                                                                        .KEY_FIELD_PREFIX,
                                                                                "")),
                                                partitionType.getFieldNames().stream())
                                        .collect(Collectors.toList()),
                        Collections.emptyMap(),
                        null);
        TableSchema tableSchema = schemaManager.createTable(schema);
        return new TestFileStore.Builder(
                        "avro",
                        tempDir.toString(),
                        numBuckets,
                        partitionType,
                        keyType,
                        valueType,
                        extractor,
                        mfFactory,
                        tableSchema)
                .build();
    }

    private static class TestValueCountMergeFunction implements MergeFunction<KeyValue> {

        private KeyValue latestKv;
        private long total;
        private KeyValue reused;

        protected TestValueCountMergeFunction() {}

        @Override
        public void reset() {
            latestKv = null;
            total = 0;
        }

        @Override
        public void add(KeyValue kv) {
            checkArgument(
                    kv.valueKind() == RowKind.INSERT,
                    "In value count mode, only insert records come. This is a bug. Please file an issue.");
            latestKv = kv;
            total += count(kv.value());
        }

        @Override
        public KeyValue getResult() {
            if (reused == null) {
                reused = new KeyValue();
            }
            return reused.replace(
                    latestKv.key(),
                    latestKv.sequenceNumber(),
                    RowKind.INSERT,
                    GenericRow.of(total));
        }

        @Override
        public boolean requireCopy() {
            return false;
        }

        private long count(InternalRow value) {
            checkArgument(!value.isNullAt(0), "Value count should not be null.");
            return value.getLong(0);
        }

        public static MergeFunctionFactory<KeyValue> factory() {
            return new Factory();
        }

        private static class Factory implements MergeFunctionFactory<KeyValue> {

            private static final long serialVersionUID = 1L;

            @Override
            public MergeFunction<KeyValue> create(@Nullable RowType readType) {
                return new TestValueCountMergeFunction();
            }
        }
    }
}
