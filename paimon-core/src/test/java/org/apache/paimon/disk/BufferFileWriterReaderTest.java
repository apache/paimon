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

import org.apache.paimon.memory.Buffer;
import org.apache.paimon.memory.MemorySegment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Random;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Test for {@link BufferFileReader} and {@link BufferFileWriter}. */
public class BufferFileWriterReaderTest {

    private static final int BUFFER_SIZE = 32 * 1024;

    private static final Random random = new Random();

    @TempDir Path tempDir;

    private IOManager ioManager;

    private BufferFileWriter writer;

    private BufferFileReader reader;

    @AfterEach
    public void shutdown() throws Exception {
        if (writer != null) {
            writer.deleteChannel();
        }

        if (reader != null) {
            reader.deleteChannel();
        }

        ioManager.close();
    }

    @BeforeEach
    public void setUpWriterAndReader() {
        this.ioManager = IOManager.create(tempDir.toFile().getAbsolutePath());
        FileIOChannel.ID channel = ioManager.createChannel();

        try {
            writer = ioManager.createBufferFileWriter(channel);
            reader = ioManager.createBufferFileReader(channel);
        } catch (IOException e) {
            if (writer != null) {
                writer.deleteChannel();
            }

            if (reader != null) {
                reader.deleteChannel();
            }

            fail("Failed to setup writer and reader.");
        }
    }

    @Test
    public void testWriteRead() throws IOException {
        int numBuffers = 1024;
        int currentNumber = 0;

        final int minBufferSize = BUFFER_SIZE / 4;

        // Write buffers filled with ascending numbers...
        LinkedList<Buffer> buffers = new LinkedList<>();
        for (int i = 0; i < numBuffers; i++) {
            final Buffer buffer = createBuffer();

            int size = getNextMultipleOf(getRandomNumberInRange(minBufferSize, BUFFER_SIZE), 4);

            currentNumber = fillBufferWithAscendingNumbers(buffer, currentNumber, size);

            writer.writeBlock(buffer);
            buffers.add(buffer);
        }

        // Make sure that the writes are finished
        writer.close();

        // Read buffers back in...
        for (int i = 0; i < numBuffers; i++) {
            assertThat(reader.hasReachedEndOfFile()).isFalse();
            reader.readInto(createBuffer());
        }

        reader.close();

        assertThat(reader.hasReachedEndOfFile()).isTrue();

        // Verify that the content is the same
        assertThat(numBuffers).isEqualTo(buffers.size());

        currentNumber = 0;
        Buffer buffer;

        while ((buffer = buffers.poll()) != null) {
            currentNumber = verifyBufferFilledWithAscendingNumbers(buffer, currentNumber);
        }
    }

    private int getRandomNumberInRange(int min, int max) {
        return random.nextInt((max - min) + 1) + min;
    }

    private int getNextMultipleOf(int number, int multiple) {
        final int mod = number % multiple;

        if (mod == 0) {
            return number;
        }

        return number + multiple - mod;
    }

    private Buffer createBuffer() {
        return Buffer.create(MemorySegment.allocateHeapMemory(BUFFER_SIZE));
    }

    static int fillBufferWithAscendingNumbers(Buffer buffer, int currentNumber, int size) {
        checkArgument(size % 4 == 0);

        MemorySegment segment = buffer.getMemorySegment();

        for (int i = 0; i < size; i += 4) {
            segment.putInt(i, currentNumber++);
        }
        buffer.setSize(size);

        return currentNumber;
    }

    static int verifyBufferFilledWithAscendingNumbers(Buffer buffer, int currentNumber) {
        MemorySegment segment = buffer.getMemorySegment();

        int size = buffer.getSize();

        for (int i = 0; i < size; i += 4) {
            if (segment.getInt(i) != currentNumber++) {
                throw new IllegalStateException("Read unexpected number from buffer.");
            }
        }

        return currentNumber;
    }
}
