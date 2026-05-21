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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link BinaryRowSerializer#deserialize(BinaryRow, org.apache.paimon.io.DataInputView)},
 * focusing on the REUSE_SHRINK_THRESHOLD behavior.
 */
class BinaryRowSerializerShrinkTest {

    private static final int SHRINK_THRESHOLD = 4 * 1024 * 1024; // 4MB

    @Test
    void testDeserializeShrinksOversizedReuseBuffer() throws Exception {
        BinaryRowSerializer serializer = new BinaryRowSerializer(1);

        // Serialize a large record (> 4MB)
        BinaryRow largeRow = createRowWithPayload(5 * 1024 * 1024);
        byte[] largeBytes = serializeRow(serializer, largeRow);

        // Deserialize into a fresh reuse row — buffer grows to hold the large record
        BinaryRow reuse = serializer.createInstance();
        DataInputDeserializer largeInput = new DataInputDeserializer(largeBytes);
        reuse = serializer.deserialize(reuse, largeInput);
        int largeBufferSize = reuse.getSegments()[0].size();
        assertThat(largeBufferSize).isGreaterThanOrEqualTo(5 * 1024 * 1024);

        // Serialize a small record
        BinaryRow smallRow = createRowWithPayload(100);
        byte[] smallBytes = serializeRow(serializer, smallRow);

        // Deserialize the small record into the same reuse row
        // The oversized buffer (> 4MB) should be shrunk to the exact size needed
        DataInputDeserializer smallInput = new DataInputDeserializer(smallBytes);
        reuse = serializer.deserialize(reuse, smallInput);
        int shrunkBufferSize = reuse.getSegments()[0].size();
        assertThat(shrunkBufferSize).isLessThan(SHRINK_THRESHOLD);
    }

    @Test
    void testDeserializeKeepsSmallReuseBuffer() throws Exception {
        BinaryRowSerializer serializer = new BinaryRowSerializer(1);

        // Serialize a small record (< 4MB)
        BinaryRow row1 = createRowWithPayload(1024);
        byte[] bytes1 = serializeRow(serializer, row1);

        BinaryRow reuse = serializer.createInstance();
        DataInputDeserializer input1 = new DataInputDeserializer(bytes1);
        reuse = serializer.deserialize(reuse, input1);
        int bufferSize1 = reuse.getSegments()[0].size();

        // Serialize an even smaller record
        BinaryRow row2 = createRowWithPayload(100);
        byte[] bytes2 = serializeRow(serializer, row2);

        // Deserialize — buffer should be reused (not shrunk), since it's < 4MB
        DataInputDeserializer input2 = new DataInputDeserializer(bytes2);
        reuse = serializer.deserialize(reuse, input2);
        int bufferSize2 = reuse.getSegments()[0].size();
        assertThat(bufferSize2).isEqualTo(bufferSize1);
    }

    @Test
    void testDeserializeGrowsBufferWhenNeeded() throws Exception {
        BinaryRowSerializer serializer = new BinaryRowSerializer(1);

        // Start with a small record
        BinaryRow smallRow = createRowWithPayload(100);
        byte[] smallBytes = serializeRow(serializer, smallRow);

        BinaryRow reuse = serializer.createInstance();
        DataInputDeserializer smallInput = new DataInputDeserializer(smallBytes);
        reuse = serializer.deserialize(reuse, smallInput);

        // Deserialize a larger record — buffer should grow
        BinaryRow largerRow = createRowWithPayload(2048);
        byte[] largerBytes = serializeRow(serializer, largerRow);

        DataInputDeserializer largerInput = new DataInputDeserializer(largerBytes);
        reuse = serializer.deserialize(reuse, largerInput);
        assertThat(reuse.getSegments()[0].size()).isGreaterThanOrEqualTo(2048);
    }

    private static BinaryRow createRowWithPayload(int payloadSize) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row, payloadSize + 32);
        byte[] payload = new byte[payloadSize];
        writer.writeString(0, BinaryString.fromBytes(payload));
        writer.complete();
        return row;
    }

    private static byte[] serializeRow(BinaryRowSerializer serializer, BinaryRow row)
            throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(row.getSizeInBytes() + 4);
        serializer.serialize(row, output);
        return output.getCopyOfBuffer();
    }
}
