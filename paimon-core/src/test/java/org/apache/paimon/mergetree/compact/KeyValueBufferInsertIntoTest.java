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

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.InMemoryBuffer;
import org.apache.paimon.disk.RowBuffer;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.mergetree.compact.KeyValueBuffer.BinaryBuffer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.KeyValueWithLevelNoReusingSerializer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KeyValueBuffer#insertInto(KeyValueBuffer, KeyValue, Comparator)}. */
public class KeyValueBufferInsertIntoTest {

    private RowType keyType;
    private RowType valueType;

    @BeforeEach
    public void beforeEach() {
        this.keyType =
                new RowType(
                        new ArrayList<DataField>() {
                            {
                                add(new DataField(0, "key", new IntType()));
                            }
                        });
        this.valueType =
                new RowType(
                        new ArrayList<DataField>() {
                            {
                                add(new DataField(0, "value", new IntType()));
                            }
                        });
    }

    private KeyValueBuffer createBuffer() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        KeyValueWithLevelNoReusingSerializer kvSerializer =
                new KeyValueWithLevelNoReusingSerializer(keyType, valueType);
        RowBuffer rowBuffer =
                new InMemoryBuffer(
                        new HeapMemorySegmentPool(8 * 1024, 1024),
                        new InternalRowSerializer(kvSerializer.fieldTypes()));
        return rnd.nextBoolean() ? createBuffer() : new BinaryBuffer(rowBuffer, kvSerializer);
    }

    /**
     * Test insertInto method with an empty buffer. It should add the highLevel KeyValue to the
     * buffer.
     */
    @Test
    public void testInsertIntoEmptyBuffer() {
        KeyValueBuffer buffer = createBuffer();
        KeyValue highLevel = createKeyValue(1, 10);
        Comparator<KeyValue> comparator = Comparator.comparingInt(kv -> kv.key().getInt(0));

        KeyValueBuffer.insertInto(buffer, highLevel, comparator);

        List<KeyValue> result = collectKeyValues(buffer);
        assertThat(result).hasSize(1);
        assertKeyValueEquals(result.get(0), highLevel);
    }

    /**
     * Test insertInto method when highLevel should be inserted at the beginning. The highLevel
     * KeyValue has a smaller key than all existing entries.
     */
    @Test
    public void testInsertIntoBeginning() {
        KeyValueBuffer buffer = createBuffer();
        // Add existing entries to buffer
        buffer.put(createKeyValue(3, 30));
        buffer.put(createKeyValue(5, 50));

        KeyValue highLevel = createKeyValue(1, 10);
        Comparator<KeyValue> comparator = Comparator.comparingInt(kv -> kv.key().getInt(0));

        KeyValueBuffer.insertInto(buffer, highLevel, comparator);

        List<KeyValue> result = collectKeyValues(buffer);
        assertThat(result).hasSize(3);
        assertKeyValueEquals(result.get(0), highLevel); // Should be first
        assertKeyValueEquals(result.get(1), createKeyValue(3, 30));
        assertKeyValueEquals(result.get(2), createKeyValue(5, 50));
    }

    /**
     * Test insertInto method when highLevel should be inserted in the middle. The highLevel
     * KeyValue has a key that fits between existing entries.
     */
    @Test
    public void testInsertIntoMiddle() {
        KeyValueBuffer buffer = createBuffer();
        // Add existing entries to buffer
        buffer.put(createKeyValue(1, 10));
        buffer.put(createKeyValue(5, 50));

        KeyValue highLevel = createKeyValue(3, 30);
        Comparator<KeyValue> comparator = Comparator.comparingInt(kv -> kv.key().getInt(0));

        KeyValueBuffer.insertInto(buffer, highLevel, comparator);

        List<KeyValue> result = collectKeyValues(buffer);
        assertThat(result).hasSize(3);
        assertKeyValueEquals(result.get(0), createKeyValue(1, 10));
        assertKeyValueEquals(result.get(1), highLevel); // Should be in the middle
        assertKeyValueEquals(result.get(2), createKeyValue(5, 50));
    }

    /**
     * Test insertInto method when highLevel should be inserted at the end. The highLevel KeyValue
     * has a larger key than all existing entries.
     */
    @Test
    public void testInsertIntoEnd() {
        KeyValueBuffer buffer = createBuffer();
        // Add existing entries to buffer
        buffer.put(createKeyValue(1, 10));
        buffer.put(createKeyValue(3, 30));

        KeyValue highLevel = createKeyValue(5, 50);
        Comparator<KeyValue> comparator = Comparator.comparingInt(kv -> kv.key().getInt(0));

        KeyValueBuffer.insertInto(buffer, highLevel, comparator);

        List<KeyValue> result = collectKeyValues(buffer);
        assertThat(result).hasSize(3);
        assertKeyValueEquals(result.get(0), createKeyValue(1, 10));
        assertKeyValueEquals(result.get(1), createKeyValue(3, 30));
        assertKeyValueEquals(result.get(2), highLevel); // Should be last
    }

    /**
     * Test insertInto method with a buffer that has multiple entries with the same key. It should
     * insert highLevel in the correct position based on the comparator.
     */
    @Test
    public void testInsertIntoWithDuplicateKeys() {
        KeyValueBuffer buffer = createBuffer();
        // Add existing entries to buffer
        buffer.put(createKeyValue(1, 10));
        buffer.put(createKeyValue(3, 30));
        buffer.put(createKeyValue(3, 35));
        buffer.put(createKeyValue(5, 50));

        KeyValue highLevel = createKeyValue(3, 25); // Same key as existing entries
        Comparator<KeyValue> comparator =
                Comparator.comparingInt((KeyValue kv) -> kv.key().getInt(0))
                        .thenComparingLong(KeyValue::sequenceNumber);

        KeyValueBuffer.insertInto(buffer, highLevel, comparator);

        List<KeyValue> result = collectKeyValues(buffer);
        assertThat(result).hasSize(5);
        assertKeyValueEquals(result.get(0), createKeyValue(1, 10));
        assertKeyValueEquals(result.get(1), createKeyValue(3, 30));
        assertKeyValueEquals(result.get(2), createKeyValue(3, 35));
        assertKeyValueEquals(result.get(3), highLevel);
        assertKeyValueEquals(result.get(4), createKeyValue(5, 50));
    }

    // Helper method to create a KeyValue for testing
    private KeyValue createKeyValue(int key, int value) {
        // Create key as BinaryRow
        BinaryRow binaryKey = new BinaryRow(1);
        BinaryRowWriter keyWriter = new BinaryRowWriter(binaryKey);
        keyWriter.writeInt(0, key);
        keyWriter.complete();

        // Create value as BinaryRow
        BinaryRow binaryValue = new BinaryRow(1);
        BinaryRowWriter valueWriter = new BinaryRowWriter(binaryValue);
        valueWriter.writeInt(0, value);
        valueWriter.complete();

        return new KeyValue().replace(binaryKey, key, RowKind.INSERT, binaryValue);
    }

    // Helper method to collect all KeyValues from a buffer
    private List<KeyValue> collectKeyValues(KeyValueBuffer buffer) {
        List<KeyValue> result = new ArrayList<>();
        try (CloseableIterator<KeyValue> iterator = buffer.iterator()) {
            while (iterator.hasNext()) {
                result.add(iterator.next());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    // Helper method to assert that two KeyValues are equal
    private void assertKeyValueEquals(KeyValue actual, KeyValue expected) {
        assertThat(actual.key().getInt(0)).isEqualTo(expected.key().getInt(0));
        assertThat(actual.value().getInt(0)).isEqualTo(expected.value().getInt(0));
        assertThat(actual.sequenceNumber()).isEqualTo(expected.sequenceNumber());
        assertThat(actual.valueKind()).isEqualTo(expected.valueKind());
    }
}
