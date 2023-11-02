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
import org.apache.paimon.disk.RowBuffer.RowBufferIterator;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.memory.MemorySegmentPool.DEFAULT_PAGE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link InMemoryBuffer}. */
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
        assertThat(result).isTrue();
        result = buffer.put(binaryRow);
        assertThat(result).isTrue();
        result = buffer.put(binaryRow);
        assertThat(result).isTrue();
        result = buffer.put(binaryRow);
        assertThat(result).isFalse();
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

        assertThat(buffer.size()).isEqualTo(100);
        try (RowBufferIterator iterator = buffer.newIterator()) {
            while (iterator.advanceNext()) {
                assertThat(iterator.getRow()).isEqualTo(binaryRow);
            }
        }
    }

    @Test
    public void testReset() throws Exception {
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

        assertThat(buffer.memoryOccupancy()).isGreaterThan(0);
        buffer.reset();
        assertThat(buffer.memoryOccupancy()).isEqualTo(0);

        // test read after reset
        try (RowBufferIterator iterator = buffer.newIterator()) {
            assertThat(iterator.advanceNext()).isFalse();
        }

        // write again
        buffer.put(binaryRow.copy());
        buffer.put(binaryRow.copy());
        buffer.put(binaryRow.copy());
        try (RowBufferIterator iterator = buffer.newIterator()) {
            int count = 0;
            while (iterator.advanceNext()) {
                assertThat(iterator.getRow()).isEqualTo(binaryRow);
                count++;
            }
            assertThat(count).isEqualTo(3);
        }
    }

    @Test
    public void testEmpty() throws Exception {
        InMemoryBuffer buffer =
                new InMemoryBuffer(
                        new HeapMemorySegmentPool(2 * DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE),
                        this.serializer);
        RowBufferIterator iterator = buffer.newIterator();
        assertThat(iterator.advanceNext()).isFalse();
    }

    @Test
    public void testMemoryPoolWorksWellWithInMemoryBuffer() {
        MemoryPoolFactory memoryPoolFactory =
                new MemoryPoolFactory(
                        new HeapMemorySegmentPool(2 * DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE));

        Owner owner1 = new Owner(this.serializer);
        Owner owner2 = new Owner(this.serializer);
        memoryPoolFactory.addOwners(Arrays.asList(owner1, owner2));
        memoryPoolFactory.notifyNewOwner(owner1);
        memoryPoolFactory.notifyNewOwner(owner2);

        owner1.reset();

        for (int i = 0; i < 100; i++) {
            Assertions.assertThatCode(owner2::put).doesNotThrowAnyException();
        }
    }

    /** Used for test. */
    public static class Owner implements MemoryOwner {

        private final InternalRowSerializer internalRowSerializer;
        private InMemoryBuffer inMemoryBuffer;
        private final BinaryRow binaryRow;

        public Owner(InternalRowSerializer internalRowSerializer) {
            this.internalRowSerializer = internalRowSerializer;

            binaryRow = new BinaryRow(1);
            BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRow);

            byte[] s = new byte[1024];
            Arrays.fill(s, (byte) 'a');
            binaryRowWriter.writeString(0, BinaryString.fromBytes(s));
            binaryRowWriter.complete();
        }

        @Override
        public void setMemoryPool(MemorySegmentPool memoryPool) {
            this.inMemoryBuffer = new InMemoryBuffer(memoryPool, internalRowSerializer);
        }

        @Override
        public long memoryOccupancy() {
            return inMemoryBuffer.memoryOccupancy();
        }

        @Override
        public void flushMemory() {
            inMemoryBuffer.complete();
            // emulate real-world flushing data to disk, we need to call newIterator method
            inMemoryBuffer.newIterator();
            inMemoryBuffer.reset();
        }

        public boolean put() throws Exception {
            return inMemoryBuffer.put(binaryRow.copy());
        }

        public void reset() {
            inMemoryBuffer.reset();
        }
    }
}
