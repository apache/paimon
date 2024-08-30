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

import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.options.MemorySize;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.paimon.memory.MemorySegmentPool.DEFAULT_PAGE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ExternalBuffer}. */
public class ExternalBufferTest {

    @TempDir Path tempDir;

    private IOManager ioManager;
    private Random random;
    private BinaryRowSerializer serializer;

    @BeforeEach
    public void before() {
        this.ioManager = IOManager.create(tempDir.toString());
        this.random = new Random();
        this.serializer = new BinaryRowSerializer(1);
    }

    private ExternalBuffer newBuffer() {
        return newBuffer(MemorySize.MAX_VALUE);
    }

    private ExternalBuffer newBuffer(MemorySize maxDiskSize) {
        return new ExternalBuffer(
                ioManager,
                new HeapMemorySegmentPool(2 * DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE),
                this.serializer,
                maxDiskSize,
                CompressOptions.defaultOptions());
    }

    @Test
    public void testLess() throws Exception {
        ExternalBuffer buffer = newBuffer();

        int number = 100;
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(0).isEqualTo(buffer.getSpillChannels().size());

        // repeat read
        assertBuffer(expected, buffer);
        buffer.newIterator();
        assertBuffer(expected, buffer);
        buffer.reset();
    }

    @Test
    public void testSpill() throws Exception {
        ExternalBuffer buffer = newBuffer();

        int number = 5000; // 16 * 5000
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(buffer.getSpillChannels().size()).isGreaterThan(0);

        // repeat read
        assertBuffer(expected, buffer);
        buffer.newIterator();
        assertBuffer(expected, buffer);
        buffer.reset();
    }

    @Test
    public void testSpillMaxDiskSize() throws Exception {
        ExternalBuffer buffer = newBuffer(MemorySize.ofKibiBytes(1));

        int number = 5000; // 16 * 5000
        List<Long> expected = insertMulti(buffer, number);
        assertThat(number).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        assertThat(buffer.getSpillChannels().size()).isGreaterThan(0);
        assertThat(buffer.flushMemory()).isFalse();

        // repeat read
        assertBuffer(expected, buffer);
        buffer.newIterator();
        assertBuffer(expected, buffer);
        buffer.reset();
    }

    @Test
    public void testBufferReset() throws Exception {
        ExternalBuffer buffer = newBuffer();

        // less
        insertMulti(buffer, 10);
        buffer.reset();
        assertThat(0).isEqualTo(buffer.size());

        // not spill
        List<Long> expected = insertMulti(buffer, 100);
        assertThat(100).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        buffer.reset();

        // spill
        expected = insertMulti(buffer, 2500);
        assertThat(2500).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        buffer.reset();
    }

    @Test
    public void testBufferResetWithSpill() throws Exception {
        int inMemoryThreshold = 20;
        ExternalBuffer buffer = newBuffer();

        // spill
        List<Long> expected = insertMulti(buffer, 5000);
        assertThat(5000).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        buffer.reset();

        // spill, but not read the values
        insertMulti(buffer, 5000);
        buffer.newIterator();
        assertThat(5000).isEqualTo(buffer.size());
        buffer.reset();

        // not spill
        expected = insertMulti(buffer, inMemoryThreshold / 2);
        assertBuffer(expected, buffer);
        buffer.reset();
        assertThat(0).isEqualTo(buffer.size());

        // less
        expected = insertMulti(buffer, 100);
        assertThat(100).isEqualTo(buffer.size());
        assertBuffer(expected, buffer);
        buffer.reset();
    }

    @Test
    public void testHugeRecord() {
        ExternalBuffer buffer =
                new ExternalBuffer(
                        ioManager,
                        new HeapMemorySegmentPool(3 * DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE),
                        new BinaryRowSerializer(1),
                        MemorySize.MAX_VALUE,
                        CompressOptions.defaultOptions());
        assertThatThrownBy(() -> writeHuge(buffer)).isInstanceOf(IOException.class);
        buffer.reset();
    }

    private void writeHuge(ExternalBuffer buffer) throws IOException {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        RandomDataGenerator random = new RandomDataGenerator();
        writer.writeString(0, BinaryString.fromString(random.nextHexString(500000)));
        writer.complete();
        buffer.put(row);
    }

    private void assertBuffer(List<Long> expected, RowBuffer buffer) {
        RowBuffer.RowBufferIterator iterator = buffer.newIterator();
        assertBuffer(expected, iterator);
        iterator.close();
    }

    private void assertBuffer(List<Long> expected, RowBuffer.RowBufferIterator iterator) {
        List<Long> values = new ArrayList<>();
        while (iterator.advanceNext()) {
            values.add(iterator.getRow().getLong(0));
        }
        assertThat(values).isEqualTo(expected);
    }

    private List<Long> insertMulti(ExternalBuffer buffer, int cnt) throws IOException {
        ArrayList<Long> expected = new ArrayList<>(cnt);
        insertMulti(buffer, cnt, expected);
        buffer.complete();
        return expected;
    }

    private void insertMulti(ExternalBuffer buffer, int cnt, List<Long> expected)
            throws IOException {
        for (int i = 0; i < cnt; i++) {
            expected.add(randomInsert(buffer));
        }
    }

    private long randomInsert(ExternalBuffer buffer) throws IOException {
        long l = random.nextLong();
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        writer.writeLong(0, l);
        writer.complete();
        buffer.put(row);
        return l;
    }
}
