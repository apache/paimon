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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.mergetree.compact.KeyValueBuffer.BinaryBuffer;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KeyValueBuffer}. */
public class KeyValueBufferTest {

    @TempDir java.nio.file.Path tempDir;

    private IOManager ioManager;
    private RowType keyType;
    private RowType valueType;

    @BeforeEach
    public void beforeEach() {
        this.ioManager = new IOManagerImpl(tempDir.toString());
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

    @AfterEach
    public void afterEach() {
        if (ioManager != null) {
            try {
                ioManager.close();
            } catch (Exception e) {
                // Ignore exception during close
            }
        }
    }

    @Test
    public void testCreateBinaryBufferWithIOManager() {
        Options options = new Options();
        options.set(CoreOptions.LOOKUP_MERGE_BUFFER_SIZE, MemorySize.ofMebiBytes(1L));

        BinaryBuffer binaryBuffer =
                KeyValueBuffer.createBinaryBuffer(
                        new CoreOptions(options), keyType, valueType, ioManager);

        assertThat(binaryBuffer).isNotNull();
    }

    @Test
    public void testCreateBinaryBufferWithoutIOManager() {
        Options options = new Options();
        options.set(CoreOptions.LOOKUP_MERGE_BUFFER_SIZE, MemorySize.ofMebiBytes(1L));

        BinaryBuffer binaryBuffer =
                KeyValueBuffer.createBinaryBuffer(
                        new CoreOptions(options), keyType, valueType, null);

        assertThat(binaryBuffer).isNotNull();
    }

    @Test
    public void testBinaryBufferPutAndIterator() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.LOOKUP_MERGE_BUFFER_SIZE, MemorySize.ofMebiBytes(1L));

        BinaryBuffer binaryBuffer =
                KeyValueBuffer.createBinaryBuffer(
                        new CoreOptions(options), keyType, valueType, ioManager);

        // Create test data
        List<KeyValue> testData = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            // Create key as BinaryRow
            BinaryRow key = new BinaryRow(1);
            BinaryRowWriter keyWriter = new BinaryRowWriter(key);
            keyWriter.writeInt(0, i);
            keyWriter.complete();

            // Create value as BinaryRow
            BinaryRow value = new BinaryRow(1);
            BinaryRowWriter valueWriter = new BinaryRowWriter(value);
            valueWriter.writeInt(0, i * 2);
            valueWriter.complete();

            testData.add(new KeyValue().replace(key, i, RowKind.INSERT, value));
        }

        // Put data into buffer
        for (KeyValue kv : testData) {
            binaryBuffer.put(kv);
        }

        // Verify data through iterator
        try (CloseableIterator<KeyValue> iterator = binaryBuffer.iterator()) {
            int count = 0;
            while (iterator.hasNext()) {
                KeyValue kv = iterator.next();
                KeyValue expected = testData.get(count);
                assertThat(kv.key().getInt(0)).isEqualTo(expected.key().getInt(0));
                assertThat(kv.value().getInt(0)).isEqualTo(expected.value().getInt(0));
                assertThat(kv.sequenceNumber()).isEqualTo(expected.sequenceNumber());
                assertThat(kv.valueKind()).isEqualTo(expected.valueKind());
                count++;
            }
            assertThat(count).isEqualTo(testData.size());
        }
    }
}
