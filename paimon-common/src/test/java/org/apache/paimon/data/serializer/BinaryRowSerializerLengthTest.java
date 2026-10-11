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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the row length validation in {@link BinaryRowSerializer#deserialize}. A length below
 * the fixed-length part leaves fields outside the buffer, and reading one of those is an unchecked
 * {@code UNSAFE} access: until it was checked, a row truncated below its fixed part deserialized
 * without error and returned a value for a field that lay past the end of the buffer.
 */
class BinaryRowSerializerLengthTest {

    private static final int NUM_FIELDS = 3;
    private static final int FIXED_PART = BinaryRow.calculateFixPartSizeInBytes(NUM_FIELDS);

    @Test
    void testNegativeLengthIsRejected() {
        byte[] negativeLength = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        assertThatThrownBy(
                        () ->
                                new BinaryRowSerializer(NUM_FIELDS)
                                        .deserialize(new DataInputDeserializer(negativeLength)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("-1");
    }

    @ParameterizedTest(name = "reuse = {0}")
    @ValueSource(booleans = {false, true})
    void testLengthBelowFixedPartIsRejected(boolean reuse) throws IOException {
        BinaryRowSerializer serializer = new BinaryRowSerializer(NUM_FIELDS);
        int shortened = FIXED_PART - 8;
        byte[] bytes = rowWithLengthPrefix(serializer, shortened);

        assertThatThrownBy(
                        () -> {
                            DataInputDeserializer in = new DataInputDeserializer(bytes);
                            if (reuse) {
                                serializer.deserialize(new BinaryRow(NUM_FIELDS), in);
                            } else {
                                serializer.deserialize(in);
                            }
                        })
                .isInstanceOf(IOException.class)
                .hasMessageContaining(String.valueOf(shortened))
                .hasMessageContaining(String.valueOf(FIXED_PART));
    }

    /** Serializes a valid row, then overwrites its length prefix with {@code length}. */
    private static byte[] rowWithLengthPrefix(BinaryRowSerializer serializer, int length)
            throws IOException {
        BinaryRow row = new BinaryRow(NUM_FIELDS);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, 42);
        writer.writeLong(1, 999L);
        writer.writeInt(2, -7);
        writer.complete();

        DataOutputSerializer out = new DataOutputSerializer(128);
        serializer.serialize(row, out);
        byte[] bytes = out.getCopyOfBuffer();
        bytes[0] = (byte) (length >>> 24);
        bytes[1] = (byte) (length >>> 16);
        bytes[2] = (byte) (length >>> 8);
        bytes[3] = (byte) length;
        return bytes;
    }
}
