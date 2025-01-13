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

package org.apache.paimon.hash;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.PagedTypeSerializer;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.KeyValueIterator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for both {@link BytesHashMap}. */
abstract class BytesHashMapTestBase<K> extends BytesMapTestBase {

    private static final int NUM_REWRITES = 10;

    static final DataType[] KEY_TYPES =
            new DataType[] {
                new IntType(),
                VarCharType.STRING_TYPE,
                new DoubleType(),
                new BigIntType(),
                new BooleanType(),
                new FloatType(),
                new SmallIntType()
            };

    static final DataType[] VALUE_TYPES =
            new DataType[] {
                new DoubleType(),
                new BigIntType(),
                new BooleanType(),
                new FloatType(),
                new SmallIntType()
            };

    protected final BinaryRow defaultValue;
    protected final PagedTypeSerializer<K> keySerializer;
    protected final BinaryRowSerializer valueSerializer;

    public BytesHashMapTestBase(PagedTypeSerializer<K> keySerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = new BinaryRowSerializer(VALUE_TYPES.length);
        this.defaultValue = valueSerializer.createInstance();
        int valueSize = defaultValue.getFixedLengthPartSize();
        this.defaultValue.pointTo(MemorySegment.wrap(new byte[valueSize]), 0, valueSize);
    }

    /** Creates the specific BytesHashMap, either {@link BytesHashMap}. */
    public abstract BytesHashMap<K> createBytesHashMap(
            MemorySegmentPool memorySegmentPool, DataType[] keyTypes, DataType[] valueTypes);

    /**
     * Generates {@code num} random keys, the types of key fields are defined in {@link #KEY_TYPES}.
     */
    public abstract K[] generateRandomKeys(int num);

    // ------------------------------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------------------------------

    @Test
    void testHashSetMode() throws IOException {
        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(VALUE_TYPES)),
                        rowLength(RowType.of(KEY_TYPES)),
                        PAGE_SIZE);
        int memorySize = numMemSegments * PAGE_SIZE;
        MemorySegmentPool pool = new HeapMemorySegmentPool(memorySize, PAGE_SIZE);

        BytesHashMap<K> table = createBytesHashMap(pool, KEY_TYPES, new DataType[] {});
        assertThat(table.isHashSetMode()).isTrue();

        K[] keys = generateRandomKeys(NUM_ENTRIES);
        verifyKeyInsert(keys, table);
        verifyKeyPresent(keys, table);
        table.free();
    }

    @Test
    void testBuildAndRetrieve() throws Exception {

        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(VALUE_TYPES)),
                        rowLength(RowType.of(KEY_TYPES)),
                        PAGE_SIZE);
        int memorySize = numMemSegments * PAGE_SIZE;
        MemorySegmentPool pool = new HeapMemorySegmentPool(memorySize, PAGE_SIZE);

        BytesHashMap<K> table = createBytesHashMap(pool, KEY_TYPES, VALUE_TYPES);

        K[] keys = generateRandomKeys(NUM_ENTRIES);
        List<BinaryRow> expected = new ArrayList<>(NUM_ENTRIES);
        verifyInsert(keys, expected, table);
        verifyRetrieve(table, keys, expected);
        table.free();
    }

    @Test
    void testBuildAndUpdate() throws Exception {
        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(VALUE_TYPES)),
                        rowLength(RowType.of(KEY_TYPES)),
                        PAGE_SIZE);
        int memorySize = numMemSegments * PAGE_SIZE;
        MemorySegmentPool pool = new HeapMemorySegmentPool(memorySize, PAGE_SIZE);

        BytesHashMap<K> table = createBytesHashMap(pool, KEY_TYPES, VALUE_TYPES);

        K[] keys = generateRandomKeys(NUM_ENTRIES);
        List<BinaryRow> expected = new ArrayList<>(NUM_ENTRIES);
        verifyInsertAndUpdate(keys, expected, table);
        verifyRetrieve(table, keys, expected);
        table.free();
    }

    @Test
    void testRest() throws Exception {
        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(VALUE_TYPES)),
                        rowLength(RowType.of(KEY_TYPES)),
                        PAGE_SIZE);

        int memorySize = numMemSegments * PAGE_SIZE;

        MemorySegmentPool pool = new HeapMemorySegmentPool(memorySize, PAGE_SIZE);

        BytesHashMap<K> table = createBytesHashMap(pool, KEY_TYPES, VALUE_TYPES);

        final K[] keys = generateRandomKeys(NUM_ENTRIES);
        List<BinaryRow> expected = new ArrayList<>(NUM_ENTRIES);
        verifyInsertAndUpdate(keys, expected, table);
        verifyRetrieve(table, keys, expected);

        table.reset();
        assertThat(table.getNumElements()).isEqualTo(0);
        assertThat(table.getRecordAreaMemorySegments()).hasSize(1);

        expected.clear();
        verifyInsertAndUpdate(keys, expected, table);
        verifyRetrieve(table, keys, expected);
        table.free();
    }

    @Test
    void testResetAndOutput() throws Exception {
        final Random rnd = new Random(RANDOM_SEED);
        final int reservedMemSegments = 64;

        int minMemorySize = reservedMemSegments * PAGE_SIZE;
        MemorySegmentPool pool = new HeapMemorySegmentPool(minMemorySize, PAGE_SIZE);

        BytesHashMap<K> table = createBytesHashMap(pool, KEY_TYPES, VALUE_TYPES);

        K[] keys = generateRandomKeys(NUM_ENTRIES);
        List<BinaryRow> expected = new ArrayList<>(NUM_ENTRIES);
        List<BinaryRow> actualValues = new ArrayList<>(NUM_ENTRIES);
        List<K> actualKeys = new ArrayList<>(NUM_ENTRIES);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            K groupKey = keys[i];
            // look up and insert
            BytesMap.LookupInfo<K, BinaryRow> lookupInfo = table.lookup(groupKey);
            assertThat(lookupInfo.isFound()).isFalse();
            try {
                BinaryRow entry = table.append(lookupInfo, defaultValue);
                assertThat(entry).isNotNull();
                // mock multiple updates
                for (int j = 0; j < NUM_REWRITES; j++) {
                    updateOutputBuffer(entry, rnd);
                }
                expected.add(entry.copy());
            } catch (Exception e) {
                ArrayList<MemorySegment> segments = table.getRecordAreaMemorySegments();
                RandomAccessInputView inView =
                        new RandomAccessInputView(segments, segments.get(0).size());
                K reuseKey = keySerializer.createReuseInstance();
                BinaryRow reuseValue = valueSerializer.createInstance();
                for (int index = 0; index < table.getNumElements(); index++) {
                    reuseKey = keySerializer.mapFromPages(reuseKey, inView);
                    reuseValue = valueSerializer.mapFromPages(reuseValue, inView);
                    actualKeys.add(keySerializer.copy(reuseKey));
                    actualValues.add(reuseValue.copy());
                }
                table.reset();
                // retry
                lookupInfo = table.lookup(groupKey);
                BinaryRow entry = table.append(lookupInfo, defaultValue);
                assertThat(entry).isNotNull();
                // mock multiple updates
                for (int j = 0; j < NUM_REWRITES; j++) {
                    updateOutputBuffer(entry, rnd);
                }
                expected.add(entry.copy());
            }
        }
        KeyValueIterator<K, BinaryRow> iter = table.getEntryIterator(false);
        while (iter.advanceNext()) {
            actualKeys.add(keySerializer.copy(iter.getKey()));
            actualValues.add(iter.getValue().copy());
        }
        assertThat(expected).hasSize(NUM_ENTRIES);
        assertThat(actualKeys).hasSize(NUM_ENTRIES);
        assertThat(actualValues).hasSize(NUM_ENTRIES);
        assertThat(actualValues).isEqualTo(expected);
        table.free();
    }

    @Test
    void testSingleKeyMultipleOps() throws Exception {
        final int numMemSegments =
                needNumMemSegments(
                        NUM_ENTRIES,
                        rowLength(RowType.of(VALUE_TYPES)),
                        rowLength(RowType.of(KEY_TYPES)),
                        PAGE_SIZE);

        int memorySize = numMemSegments * PAGE_SIZE;

        MemorySegmentPool pool = new HeapMemorySegmentPool(memorySize, PAGE_SIZE);

        BytesHashMap<K> table = createBytesHashMap(pool, KEY_TYPES, VALUE_TYPES);
        final K key = generateRandomKeys(1)[0];
        for (int i = 0; i < 3; i++) {
            BytesMap.LookupInfo<K, BinaryRow> lookupInfo = table.lookup(key);
            assertThat(lookupInfo.isFound()).isFalse();
        }

        for (int i = 0; i < 3; i++) {
            BytesMap.LookupInfo<K, BinaryRow> lookupInfo = table.lookup(key);
            BinaryRow entry = lookupInfo.getValue();
            if (i == 0) {
                assertThat(lookupInfo.isFound()).isFalse();
                entry = table.append(lookupInfo, defaultValue);
            } else {
                assertThat(lookupInfo.isFound()).isTrue();
            }
            assertThat(entry).isNotNull();
        }
        table.free();
    }

    // ----------------------------------------------
    /** It will be codegened when in HashAggExec using rnd to mock update/initExprs resultTerm. */
    private void updateOutputBuffer(BinaryRow reuse, Random rnd) {
        long longVal = rnd.nextLong();
        double doubleVal = rnd.nextDouble();
        boolean boolVal = longVal % 2 == 0;
        reuse.setDouble(2, doubleVal);
        reuse.setLong(3, longVal);
        reuse.setBoolean(4, boolVal);
    }

    // ----------------------- Utilities  -----------------------

    private void verifyRetrieve(BytesHashMap<K> table, K[] keys, List<BinaryRow> expected) {
        assertThat(table.getNumElements()).isEqualTo(NUM_ENTRIES);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            K groupKey = keys[i];
            // look up and retrieve
            BytesMap.LookupInfo<K, BinaryRow> lookupInfo = table.lookup(groupKey);
            assertThat(lookupInfo.isFound()).isTrue();
            assertThat(lookupInfo.getValue()).isNotNull();
            assertThat(lookupInfo.getValue()).isEqualTo(expected.get(i));
        }
    }

    private void verifyInsert(K[] keys, List<BinaryRow> inserted, BytesHashMap<K> table)
            throws IOException {
        for (int i = 0; i < NUM_ENTRIES; i++) {
            K groupKey = keys[i];
            // look up and insert
            BytesMap.LookupInfo<K, BinaryRow> lookupInfo = table.lookup(groupKey);
            assertThat(lookupInfo.isFound()).isFalse();
            BinaryRow entry = table.append(lookupInfo, defaultValue);
            assertThat(entry).isNotNull();
            assertThat(defaultValue).isEqualTo(entry);
            inserted.add(entry.copy());
        }
        assertThat(table.getNumElements()).isEqualTo(NUM_ENTRIES);
    }

    private void verifyInsertAndUpdate(K[] keys, List<BinaryRow> inserted, BytesHashMap<K> table)
            throws IOException {
        final Random rnd = new Random(RANDOM_SEED);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            K groupKey = keys[i];
            // look up and insert
            BytesMap.LookupInfo<K, BinaryRow> lookupInfo = table.lookup(groupKey);
            assertThat(lookupInfo.isFound()).isFalse();
            BinaryRow entry = table.append(lookupInfo, defaultValue);
            assertThat(entry).isNotNull();
            // mock multiple updates
            for (int j = 0; j < NUM_REWRITES; j++) {
                updateOutputBuffer(entry, rnd);
            }
            inserted.add(entry.copy());
        }
        assertThat(table.getNumElements()).isEqualTo(NUM_ENTRIES);
    }

    private void verifyKeyPresent(K[] keys, BytesHashMap<K> table) {
        assertThat(table.getNumElements()).isEqualTo(NUM_ENTRIES);
        BinaryRow present = new BinaryRow(0);
        present.pointTo(MemorySegment.wrap(new byte[8]), 0, 8);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            K groupKey = keys[i];
            // look up and retrieve
            BytesMap.LookupInfo<K, BinaryRow> lookupInfo = table.lookup(groupKey);
            assertThat(lookupInfo.isFound()).isTrue();
            assertThat(lookupInfo.getValue()).isNotNull();
            assertThat(lookupInfo.getValue()).isEqualTo(present);
        }
    }

    private void verifyKeyInsert(K[] keys, BytesHashMap<K> table) throws IOException {
        BinaryRow present = new BinaryRow(0);
        present.pointTo(MemorySegment.wrap(new byte[8]), 0, 8);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            K groupKey = keys[i];
            // look up and insert
            BytesMap.LookupInfo<K, BinaryRow> lookupInfo = table.lookup(groupKey);
            assertThat(lookupInfo.isFound()).isFalse();
            BinaryRow entry = table.append(lookupInfo, defaultValue);
            assertThat(entry).isNotNull();
            assertThat(present).isEqualTo(entry);
        }
        assertThat(table.getNumElements()).isEqualTo(NUM_ENTRIES);
    }
}
