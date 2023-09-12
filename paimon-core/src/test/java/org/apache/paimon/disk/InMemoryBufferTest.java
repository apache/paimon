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

package org.apache.paimon.disk;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.memory.MemorySegmentPool.DEFAULT_PAGE_SIZE;

/** Tests for {@link InternalRowBuffer}. */
public class InMemoryBufferTest {

    private InternalRowSerializer serializer;

    @BeforeEach
    public void before() {
        this.serializer = new InternalRowSerializer(DataTypes.STRING());
    }

    @Test
    public void testNonSpill() throws Exception {
        InMemoryBuffer buffer =
                new InMemoryBuffer(
                        new HeapMemorySegmentPool(2 * DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE),
                        this.serializer);

        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);

        byte[] s = new byte[20 * 1024];
        Arrays.fill(s, (byte) 'a');
        binaryRowWriter.writeString(0, BinaryString.fromBytes(s));
        binaryRowWriter.complete();

        boolean result = buffer.put(binaryRow);
        Assertions.assertThat(result).isTrue();
        result = buffer.put(binaryRow);
        Assertions.assertThat(result).isTrue();
        result = buffer.put(binaryRow);
        Assertions.assertThat(result).isTrue();
        result = buffer.put(binaryRow);
        Assertions.assertThat(result).isFalse();
    }

    @Test
    public void testPutRead() throws Exception {
        InMemoryBuffer buffer =
                new InMemoryBuffer(
                        new HeapMemorySegmentPool(2 * DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE),
                        this.serializer);

        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);

        byte[] s = new byte[10];
        Arrays.fill(s, (byte) 'a');
        binaryRowWriter.writeString(0, BinaryString.fromBytes(s));
        binaryRowWriter.complete();
        for (int i = 0; i < 100; i++) {
            buffer.put(binaryRow.copy());
        }

        Assertions.assertThat(buffer.size()).isEqualTo(100);
        try (InternalRowBuffer.InternalRowBufferIterator iterator = buffer.newIterator()) {
            while (iterator.advanceNext()) {
                Assertions.assertThat(iterator.getRow()).isEqualTo(binaryRow);
            }
        }
    }

    @Test
    public void testClose() throws Exception {
        InMemoryBuffer buffer =
                new InMemoryBuffer(
                        new HeapMemorySegmentPool(2 * DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE),
                        this.serializer);

        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);

        byte[] s = new byte[10];
        Arrays.fill(s, (byte) 'a');
        binaryRowWriter.writeString(0, BinaryString.fromBytes(s));
        binaryRowWriter.complete();
        buffer.put(binaryRow.copy());

        Assertions.assertThat(buffer.memoryOccupancy()).isGreaterThan(0);
        buffer.reset();
        Assertions.assertThat(buffer.memoryOccupancy()).isEqualTo(0);
    }
}
