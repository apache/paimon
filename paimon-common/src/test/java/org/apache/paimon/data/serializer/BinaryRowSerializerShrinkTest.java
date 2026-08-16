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
 * focusing on the combined cap + ratio shrink behavior.
 */
class BinaryRowSerializerShrinkTest {

    private static final int MAX_RETAINED = 4 * 1024 * 1024; // 4MB

    @Test
    void testShrinksWhenSpikeFollowedBySmallRecord() throws Exception {
        BinaryRowSerializer serializer = new BinaryRowSerializer(1);

        // Inflate buffer with a large record (> 4MB)
        BinaryRow largeRow = createRowWithPayload(5 * 1024 * 1024);
        byte[] largeBytes = serializeRow(serializer, largeRow);

        BinaryRow reuse = serializer.createInstance();
        reuse = serializer.deserialize(reuse, new DataInputDeserializer(largeBytes));
        int largeBufferSize = reuse.getSegments()[0].size();
        assertThat(largeBufferSize).isGreaterThanOrEqualTo(5 * 1024 * 1024);

        // Deserialize a small record — buffer > 4MB and ratio huge > 4x → shrink
        BinaryRow smallRow = createRowWithPayload(100);
        byte[] smallBytes = serializeRow(serializer, smallRow);
        reuse = serializer.deserialize(reuse, new DataInputDeserializer(smallBytes));
        assertThat(reuse.getSegments()[0].size()).isLessThan(MAX_RETAINED);
    }

    @Test
    void testShrinksWhenSpikeFollowedByMediumRecord() throws Exception {
        BinaryRowSerializer serializer = new BinaryRowSerializer(1);

        // Inflate buffer with a very large record (100MB)
        BinaryRow hugeRow = createRowWithPayload(100 * 1024 * 1024);
        byte[] hugeBytes = serializeRow(serializer, hugeRow);

        BinaryRow reuse = serializer.createInstance();
        reuse = serializer.deserialize(reuse, new DataInputDeserializer(hugeBytes));
        int hugeBufferSize = reuse.getSegments()[0].size();
        assertThat(hugeBufferSize).isGreaterThanOrEqualTo(100 * 1024 * 1024);

        // Deserialize a 5MB record — buffer ~100MB, ratio ~20x > 4x → shrink
        BinaryRow mediumRow = createRowWithPayload(5 * 1024 * 1024);
        byte[] mediumBytes = serializeRow(serializer, mediumRow);
        reuse = serializer.deserialize(reuse, new DataInputDeserializer(mediumBytes));
        assertThat(reuse.getSegments()[0].size()).isLessThan(hugeBufferSize);
    }

    @Test
    void testRetainsWhenBufferProportionalToRecordSize() throws Exception {
        BinaryRowSerializer serializer = new BinaryRowSerializer(1);

        // Inflate buffer with a 5MB record
        BinaryRow row1 = createRowWithPayload(5 * 1024 * 1024);
        byte[] bytes1 = serializeRow(serializer, row1);

        BinaryRow reuse = serializer.createInstance();
        reuse = serializer.deserialize(reuse, new DataInputDeserializer(bytes1));
        int bufferAfterFirst = reuse.getSegments()[0].size();
        assertThat(bufferAfterFirst).isGreaterThan(MAX_RETAINED);

        // Deserialize another record just above threshold — ratio ~1.2x < 4x → retain
        BinaryRow row2 = createRowWithPayload(MAX_RETAINED + 100);
        byte[] bytes2 = serializeRow(serializer, row2);
        reuse = serializer.deserialize(reuse, new DataInputDeserializer(bytes2));
        assertThat(reuse.getSegments()[0].size()).isEqualTo(bufferAfterFirst);
    }

    @Test
    void testKeepsSmallBuffer() throws Exception {
        BinaryRowSerializer serializer = new BinaryRowSerializer(1);

        BinaryRow row1 = createRowWithPayload(1024);
        byte[] bytes1 = serializeRow(serializer, row1);

        BinaryRow reuse = serializer.createInstance();
        reuse = serializer.deserialize(reuse, new DataInputDeserializer(bytes1));
        int bufferSize1 = reuse.getSegments()[0].size();

        // Smaller record — buffer < 4MB, should reuse without shrinking
        BinaryRow row2 = createRowWithPayload(100);
        byte[] bytes2 = serializeRow(serializer, row2);
        reuse = serializer.deserialize(reuse, new DataInputDeserializer(bytes2));
        assertThat(reuse.getSegments()[0].size()).isEqualTo(bufferSize1);
    }

    @Test
    void testGrowsBufferWhenNeeded() throws Exception {
        BinaryRowSerializer serializer = new BinaryRowSerializer(1);

        BinaryRow smallRow = createRowWithPayload(100);
        byte[] smallBytes = serializeRow(serializer, smallRow);

        BinaryRow reuse = serializer.createInstance();
        reuse = serializer.deserialize(reuse, new DataInputDeserializer(smallBytes));

        // Larger record arrives — buffer must grow
        BinaryRow largerRow = createRowWithPayload(2048);
        byte[] largerBytes = serializeRow(serializer, largerRow);
        reuse = serializer.deserialize(reuse, new DataInputDeserializer(largerBytes));
        assertThat(reuse.getSegments()[0].size()).isGreaterThanOrEqualTo(2048);
    }

    @Test
    void testRetainsBufferForConsecutiveLargeRecords() throws Exception {
        BinaryRowSerializer serializer = new BinaryRowSerializer(1);

        // Inflate buffer with 5MB record
        BinaryRow largeRow1 = createRowWithPayload(5 * 1024 * 1024);
        byte[] largeBytes1 = serializeRow(serializer, largeRow1);

        BinaryRow reuse = serializer.createInstance();
        reuse = serializer.deserialize(reuse, new DataInputDeserializer(largeBytes1));
        int bufferAfterFirst = reuse.getSegments()[0].size();

        // Another 5MB record — ratio ~1x < 4x → retain
        BinaryRow largeRow2 = createRowWithPayload(5 * 1024 * 1024);
        byte[] largeBytes2 = serializeRow(serializer, largeRow2);
        reuse = serializer.deserialize(reuse, new DataInputDeserializer(largeBytes2));
        assertThat(reuse.getSegments()[0].size()).isEqualTo(bufferAfterFirst);
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
