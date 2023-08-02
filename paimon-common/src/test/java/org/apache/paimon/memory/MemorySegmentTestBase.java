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

package org.apache.paimon.memory;

import org.apache.paimon.testutils.junit.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.within;

/** Tests for the access and transfer methods of the {@link MemorySegment}. */
public abstract class MemorySegmentTestBase {

    private final Random random = new Random();

    private final int pageSize;

    MemorySegmentTestBase(int pageSize) {
        this.pageSize = pageSize;
    }

    // ------------------------------------------------------------------------
    //  Access to primitives
    // ------------------------------------------------------------------------

    abstract MemorySegment createSegment(int size);

    // ------------------------------------------------------------------------
    //  Access to primitives
    // ------------------------------------------------------------------------

    @TestTemplate
    public void testByteAccess() {
        final MemorySegment segment = createSegment(pageSize);

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i < pageSize; i++) {
            segment.put(i, (byte) random.nextInt());
        }

        random.setSeed(seed);
        for (int i = 0; i < pageSize; i++) {
            assertThat(segment.get(i)).isEqualTo((byte) random.nextInt());
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize);

            if (occupied[pos]) {
                continue;
            } else {
                occupied[pos] = true;
            }

            segment.put(pos, (byte) random.nextInt());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize);

            if (occupied[pos]) {
                continue;
            } else {
                occupied[pos] = true;
            }

            assertThat(segment.get(pos)).isEqualTo((byte) random.nextInt());
        }
    }

    @TestTemplate
    public void testBooleanAccess() {
        final MemorySegment segment = createSegment(pageSize);

        long seed = random.nextLong();

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize);

            if (occupied[pos]) {
                continue;
            } else {
                occupied[pos] = true;
            }

            segment.putBoolean(pos, random.nextBoolean());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize);

            if (occupied[pos]) {
                continue;
            } else {
                occupied[pos] = true;
            }

            assertThat(segment.getBoolean(pos)).isEqualTo(random.nextBoolean());
        }
    }

    @TestTemplate
    public void testEqualTo() {
        MemorySegment seg1 = createSegment(pageSize);
        MemorySegment seg2 = createSegment(pageSize);

        byte[] referenceArray = new byte[pageSize];
        seg1.put(0, referenceArray);
        seg2.put(0, referenceArray);

        int i = new Random().nextInt(pageSize - 8);

        seg1.put(i, (byte) 10);
        assertThat(seg1.equalTo(seg2, i, i, 9)).isFalse();

        seg1.put(i, (byte) 0);
        assertThat(seg1.equalTo(seg2, i, i, 9)).isTrue();

        seg1.put(i + 8, (byte) 10);
        assertThat(seg1.equalTo(seg2, i, i, 9)).isFalse();
    }

    @TestTemplate
    public void testCharAccess() {
        final MemorySegment segment = createSegment(pageSize);

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 2; i += 2) {
            segment.putChar(i, (char) (random.nextInt(Character.MAX_VALUE)));
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 2; i += 2) {
            assertThat(segment.getChar(i)).isEqualTo((char) (random.nextInt(Character.MAX_VALUE)));
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 1);

            if (occupied[pos] || occupied[pos + 1]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
            }

            segment.putChar(pos, (char) (random.nextInt(Character.MAX_VALUE)));
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 1);

            if (occupied[pos] || occupied[pos + 1]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
            }

            assertThat(segment.getChar(pos))
                    .isEqualTo((char) (random.nextInt(Character.MAX_VALUE)));
        }
    }

    @TestTemplate
    public void testShortAccess() {
        final MemorySegment segment = createSegment(pageSize);

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 2; i += 2) {
            segment.putShort(i, (short) random.nextInt());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 2; i += 2) {
            assertThat(segment.getShort(i)).isEqualTo((short) random.nextInt());
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 1);

            if (occupied[pos] || occupied[pos + 1]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
            }

            segment.putShort(pos, (short) random.nextInt());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 1);

            if (occupied[pos] || occupied[pos + 1]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
            }

            assertThat(segment.getShort(pos)).isEqualTo((short) random.nextInt());
        }
    }

    @TestTemplate
    public void testIntAccess() {
        final MemorySegment segment = createSegment(pageSize);

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 4; i += 4) {
            segment.putInt(i, random.nextInt());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 4; i += 4) {
            assertThat(segment.getInt(i)).isEqualTo(random.nextInt());
        }

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 3);

            if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
            }

            segment.putInt(pos, random.nextInt());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 3);

            if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
            }

            assertThat(segment.getInt(pos)).isEqualTo(random.nextInt());
        }
    }

    @TestTemplate
    public void testLongAccess() {
        final MemorySegment segment = createSegment(pageSize);

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 8; i += 8) {
            segment.putLong(i, random.nextLong());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 8; i += 8) {
            assertThat(segment.getLong(i)).isEqualTo(random.nextLong());
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 7);

            if (occupied[pos]
                    || occupied[pos + 1]
                    || occupied[pos + 2]
                    || occupied[pos + 3]
                    || occupied[pos + 4]
                    || occupied[pos + 5]
                    || occupied[pos + 6]
                    || occupied[pos + 7]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
                occupied[pos + 4] = true;
                occupied[pos + 5] = true;
                occupied[pos + 6] = true;
                occupied[pos + 7] = true;
            }

            segment.putLong(pos, random.nextLong());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 7);

            if (occupied[pos]
                    || occupied[pos + 1]
                    || occupied[pos + 2]
                    || occupied[pos + 3]
                    || occupied[pos + 4]
                    || occupied[pos + 5]
                    || occupied[pos + 6]
                    || occupied[pos + 7]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
                occupied[pos + 4] = true;
                occupied[pos + 5] = true;
                occupied[pos + 6] = true;
                occupied[pos + 7] = true;
            }

            assertThat(segment.getLong(pos)).isEqualTo(random.nextLong());
        }
    }

    @TestTemplate
    public void testFloatAccess() {
        final MemorySegment segment = createSegment(pageSize);

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 4; i += 4) {
            segment.putFloat(i, random.nextFloat());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 4; i += 4) {
            assertThat(segment.getFloat(i)).isCloseTo(random.nextFloat(), within(0.0f));
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 3);

            if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
            }

            segment.putFloat(pos, random.nextFloat());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 3);

            if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
            }

            assertThat(segment.getFloat(pos)).isCloseTo(random.nextFloat(), within(0.0f));
        }
    }

    @TestTemplate
    public void testDoubleAccess() {
        final MemorySegment segment = createSegment(pageSize);

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 8; i += 8) {
            segment.putDouble(i, random.nextDouble());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 8; i += 8) {
            assertThat(segment.getDouble(i)).isCloseTo(random.nextDouble(), within(0.0d));
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 7);

            if (occupied[pos]
                    || occupied[pos + 1]
                    || occupied[pos + 2]
                    || occupied[pos + 3]
                    || occupied[pos + 4]
                    || occupied[pos + 5]
                    || occupied[pos + 6]
                    || occupied[pos + 7]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
                occupied[pos + 4] = true;
                occupied[pos + 5] = true;
                occupied[pos + 6] = true;
                occupied[pos + 7] = true;
            }

            segment.putDouble(pos, random.nextDouble());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 7);

            if (occupied[pos]
                    || occupied[pos + 1]
                    || occupied[pos + 2]
                    || occupied[pos + 3]
                    || occupied[pos + 4]
                    || occupied[pos + 5]
                    || occupied[pos + 6]
                    || occupied[pos + 7]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
                occupied[pos + 4] = true;
                occupied[pos + 5] = true;
                occupied[pos + 6] = true;
                occupied[pos + 7] = true;
            }

            assertThat(segment.getDouble(pos)).isCloseTo(random.nextDouble(), within(0.0d));
        }
    }

    // ------------------------------------------------------------------------
    //  Bulk Byte Movements
    // ------------------------------------------------------------------------

    @TestTemplate
    public void testBulkByteAccess() {
        // test expected correct behavior with default offset / length
        {
            final MemorySegment segment = createSegment(pageSize);
            long seed = random.nextLong();

            random.setSeed(seed);
            byte[] src = new byte[pageSize / 8];
            for (int i = 0; i < 8; i++) {
                random.nextBytes(src);
                segment.put(i * (pageSize / 8), src);
            }

            random.setSeed(seed);
            byte[] expected = new byte[pageSize / 8];
            byte[] actual = new byte[pageSize / 8];
            for (int i = 0; i < 8; i++) {
                random.nextBytes(expected);
                segment.get(i * (pageSize / 8), actual);

                assertThat(actual).containsExactly(expected);
            }
        }

        // test expected correct behavior with specific offset / length
        {
            final MemorySegment segment = createSegment(pageSize);
            byte[] expected = new byte[pageSize];
            random.nextBytes(expected);

            for (int i = 0; i < 16; i++) {
                segment.put(i * (pageSize / 16), expected, i * (pageSize / 16), pageSize / 16);
            }

            byte[] actual = new byte[pageSize];
            for (int i = 0; i < 16; i++) {
                segment.get(i * (pageSize / 16), actual, i * (pageSize / 16), pageSize / 16);
            }

            assertThat(actual).containsExactly(expected);
        }

        // put segments of various lengths to various positions
        {
            final MemorySegment segment = createSegment(pageSize);
            byte[] expected = new byte[pageSize];
            segment.put(0, expected, 0, pageSize);

            for (int i = 0; i < 200; i++) {
                int numBytes = random.nextInt(pageSize - 10) + 1;
                int pos = random.nextInt(pageSize - numBytes + 1);

                byte[] data = new byte[(random.nextInt(3) + 1) * numBytes];
                int dataStartPos = random.nextInt(data.length - numBytes + 1);

                random.nextBytes(data);

                // copy to the expected
                System.arraycopy(data, dataStartPos, expected, pos, numBytes);

                // put to the memory segment
                segment.put(pos, data, dataStartPos, numBytes);
            }

            byte[] validation = new byte[pageSize];
            segment.get(0, validation);

            assertThat(validation).containsExactly(expected);
        }

        // get segments with various contents
        {
            final MemorySegment segment = createSegment(pageSize);
            byte[] contents = new byte[pageSize];
            random.nextBytes(contents);
            segment.put(0, contents);

            for (int i = 0; i < 200; i++) {
                int numBytes = random.nextInt(pageSize / 8) + 1;
                int pos = random.nextInt(pageSize - numBytes + 1);

                byte[] data = new byte[(random.nextInt(3) + 1) * numBytes];
                int dataStartPos = random.nextInt(data.length - numBytes + 1);

                segment.get(pos, data, dataStartPos, numBytes);

                byte[] expected = Arrays.copyOfRange(contents, pos, pos + numBytes);
                byte[] validation = Arrays.copyOfRange(data, dataStartPos, dataStartPos + numBytes);
                assertThat(validation).containsExactly(expected);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Writing / Reading to/from DataInput / DataOutput
    // ------------------------------------------------------------------------

    @TestTemplate
    public void testDataInputOutput() throws IOException {
        MemorySegment seg = createSegment(pageSize);
        byte[] contents = new byte[pageSize];
        random.nextBytes(contents);
        seg.put(0, contents);

        ByteArrayOutputStream buffer = new ByteArrayOutputStream(pageSize);
        DataOutputStream out = new DataOutputStream(buffer);

        // write the segment in chunks into the stream
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(200);
            len = Math.min(len, pageSize - pos);
            seg.get(out, pos, len);
            pos += len;
        }

        // verify that we wrote the same bytes
        byte[] result = buffer.toByteArray();
        assertThat(result).containsExactly(contents);

        // re-read the bytes into a new memory segment
        MemorySegment reader = createSegment(pageSize);
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(result));

        pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(200);
            len = Math.min(len, pageSize - pos);
            reader.put(in, pos, len);
            pos += len;
        }

        byte[] targetBuffer = new byte[pageSize];
        reader.get(0, targetBuffer);

        assertThat(targetBuffer).containsExactly(contents);
    }

    @TestTemplate
    public void testDataInputOutputStreamUnderflowOverflow() throws IOException {
        final int segmentSize = 1337;

        // segment with random contents
        MemorySegment seg = createSegment(segmentSize);
        byte[] bytes = new byte[segmentSize];
        random.nextBytes(bytes);
        seg.put(0, bytes);

        // a stream that we cannot fully write to
        DataOutputStream out =
                new DataOutputStream(
                        new OutputStream() {

                            int bytesSoFar = 0;

                            @Override
                            public void write(int b) throws IOException {
                                bytesSoFar++;
                                if (bytesSoFar > segmentSize / 2) {
                                    throw new IOException("overflow");
                                }
                            }
                        });

        // write the segment in chunks into the stream
        try {
            int pos = 0;
            while (pos < pageSize) {
                int len = random.nextInt(segmentSize / 10);
                len = Math.min(len, pageSize - pos);
                seg.get(out, pos, len);
                pos += len;
            }
            fail("Should fail with an IOException");
        } catch (IOException e) {
            // expected
        }

        DataInputStream in =
                new DataInputStream(new ByteArrayInputStream(new byte[segmentSize / 2]));

        try {
            int pos = 0;
            while (pos < pageSize) {
                int len = random.nextInt(segmentSize / 10);
                len = Math.min(len, pageSize - pos);
                seg.put(in, pos, len);
                pos += len;
            }
            fail("Should fail with an EOFException");
        } catch (EOFException e) {
            // expected
        }
    }

    // ------------------------------------------------------------------------
    //  ByteBuffer Ops
    // ------------------------------------------------------------------------

    @TestTemplate
    public void testByteBufferGet() {
        testByteBufferGet(false);
        testByteBufferGet(true);
    }

    private void testByteBufferGet(boolean directBuffer) {
        MemorySegment seg = createSegment(pageSize);
        byte[] bytes = new byte[pageSize];
        random.nextBytes(bytes);
        seg.put(0, bytes);

        ByteBuffer target =
                directBuffer
                        ? ByteBuffer.allocateDirect(3 * pageSize)
                        : ByteBuffer.allocate(3 * pageSize);
        target.position(2 * pageSize);

        // transfer the segment in chunks into the byte buffer
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(pageSize / 10);
            len = Math.min(len, pageSize - pos);
            seg.get(pos, target, len);
            pos += len;
        }

        // verify that we wrote the same bytes
        byte[] result = new byte[pageSize];
        target.position(2 * pageSize);
        target.get(result);

        assertThat(result).containsExactly(bytes);
    }

    @TestTemplate
    public void testHeapByteBufferGetReadOnly() {
        assertThatThrownBy(() -> testByteBufferGetReadOnly(false))
                .isInstanceOf(ReadOnlyBufferException.class);
    }

    @TestTemplate
    public void testOffHeapByteBufferGetReadOnly() {
        assertThatThrownBy(() -> testByteBufferGetReadOnly(true))
                .isInstanceOf(ReadOnlyBufferException.class);
    }

    /**
     * Tries to write into a {@link ByteBuffer} instance which is read-only. This should fail with a
     * {@link ReadOnlyBufferException}.
     *
     * @param directBuffer whether the {@link ByteBuffer} instance should be a direct byte buffer or
     *     not
     * @throws ReadOnlyBufferException expected exception due to writing to a read-only buffer
     */
    private void testByteBufferGetReadOnly(boolean directBuffer) throws ReadOnlyBufferException {
        MemorySegment seg = createSegment(pageSize);

        ByteBuffer target =
                (directBuffer ? ByteBuffer.allocateDirect(pageSize) : ByteBuffer.allocate(pageSize))
                        .asReadOnlyBuffer();

        seg.get(0, target, pageSize);
    }

    @TestTemplate
    public void testByteBufferPut() {
        testByteBufferPut(false);
        testByteBufferPut(true);
    }

    private void testByteBufferPut(boolean directBuffer) {
        byte[] bytes = new byte[pageSize];
        random.nextBytes(bytes);

        ByteBuffer source =
                directBuffer ? ByteBuffer.allocateDirect(pageSize) : ByteBuffer.allocate(pageSize);

        source.put(bytes);
        source.clear();

        MemorySegment seg = createSegment(3 * pageSize);

        int offset = 2 * pageSize;

        // transfer the segment in chunks into the byte buffer
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(pageSize / 10);
            len = Math.min(len, pageSize - pos);
            seg.put(offset + pos, source, len);
            pos += len;
        }

        // verify that we read the same bytes
        byte[] result = new byte[pageSize];
        seg.get(offset, result);

        assertThat(result).containsExactly(bytes);
    }

    // ------------------------------------------------------------------------
    //  ByteBuffer Ops on sliced byte buffers
    // ------------------------------------------------------------------------

    @TestTemplate
    public void testSlicedByteBufferGet() {
        testSlicedByteBufferGet(false);
        testSlicedByteBufferGet(true);
    }

    private void testSlicedByteBufferGet(boolean directBuffer) {
        MemorySegment seg = createSegment(pageSize);
        byte[] bytes = new byte[pageSize];
        random.nextBytes(bytes);
        seg.put(0, bytes);

        ByteBuffer target =
                directBuffer
                        ? ByteBuffer.allocateDirect(pageSize + 49)
                        : ByteBuffer.allocate(pageSize + 49);

        target.position(19).limit(19 + pageSize);

        ByteBuffer slicedTarget = target.slice();

        // transfer the segment in chunks into the byte buffer
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(pageSize / 10);
            len = Math.min(len, pageSize - pos);
            seg.get(pos, slicedTarget, len);
            pos += len;
        }

        // verify that we wrote the same bytes
        byte[] result = new byte[pageSize];
        target.position(19);
        target.get(result);

        assertThat(result).containsExactly(bytes);
    }

    @TestTemplate
    public void testSlicedByteBufferPut() {
        testSlicedByteBufferPut(false);
        testSlicedByteBufferPut(true);
    }

    private void testSlicedByteBufferPut(boolean directBuffer) {
        byte[] bytes = new byte[pageSize + 49];
        random.nextBytes(bytes);

        ByteBuffer source =
                directBuffer
                        ? ByteBuffer.allocateDirect(pageSize + 49)
                        : ByteBuffer.allocate(pageSize + 49);

        source.put(bytes);
        source.position(19).limit(19 + pageSize);
        ByteBuffer slicedSource = source.slice();

        MemorySegment seg = createSegment(3 * pageSize);

        final int offset = 2 * pageSize;

        // transfer the segment in chunks into the byte buffer
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(pageSize / 10);
            len = Math.min(len, pageSize - pos);
            seg.put(offset + pos, slicedSource, len);
            pos += len;
        }

        // verify that we read the same bytes
        byte[] result = new byte[pageSize];
        seg.get(offset, result);

        byte[] expected = Arrays.copyOfRange(bytes, 19, 19 + pageSize);
        assertThat(result).containsExactly(expected);
    }

    @TestTemplate
    public void testCompareBytes() {
        final byte[] bytes1 = new byte[pageSize];
        final byte[] bytes2 = new byte[pageSize];

        final int stride = pageSize / 255;
        final int shift = 16666;

        for (int i = 0; i < pageSize; i++) {
            byte val = (byte) ((i / stride) & 0xff);
            bytes1[i] = val;

            if (i + shift < bytes2.length) {
                bytes2[i + shift] = val;
            }
        }

        MemorySegment seg1 = createSegment(pageSize);
        MemorySegment seg2 = createSegment(pageSize);
        seg1.put(0, bytes1);
        seg2.put(0, bytes2);

        for (int i = 0; i < 1000; i++) {
            int pos1 = random.nextInt(bytes1.length);
            int pos2 = random.nextInt(bytes2.length);

            int len =
                    Math.min(
                            Math.min(bytes1.length - pos1, bytes2.length - pos2),
                            random.nextInt(pageSize / 50));

            int cmp = seg1.compare(seg2, pos1, pos2, len);

            if (pos1 < pos2 - shift) {
                assertThat(cmp <= 0).isTrue();
            } else {
                assertThat(cmp >= 0).isTrue();
            }
        }
    }

    @TestTemplate
    public void testCompareBytesWithDifferentLength() {
        final byte[] bytes1 = new byte[] {'a', 'b', 'c'};
        final byte[] bytes2 = new byte[] {'a', 'b', 'c', 'd'};

        MemorySegment seg1 = createSegment(4);
        MemorySegment seg2 = createSegment(4);
        seg1.put(0, bytes1);
        seg2.put(0, bytes2);

        assertThat(seg1.compare(seg2, 0, 0, 3, 4)).isLessThan(0);
        assertThat(seg1.compare(seg2, 0, 0, 3, 3)).isEqualTo(0);
        assertThat(seg1.compare(seg2, 0, 0, 3, 2)).isGreaterThan(0);
        // test non-zero offset
        assertThat(seg1.compare(seg2, 1, 1, 2, 3)).isLessThan(0);
        assertThat(seg1.compare(seg2, 1, 1, 2, 2)).isEqualTo(0);
        assertThat(seg1.compare(seg2, 1, 1, 2, 1)).isGreaterThan(0);
    }

    @TestTemplate
    public void testSwapBytes() {
        final int halfPageSize = pageSize / 2;

        final byte[] bytes1 = new byte[pageSize];
        final byte[] bytes2 = new byte[halfPageSize];

        Arrays.fill(bytes2, (byte) 1);

        MemorySegment seg1 = createSegment(pageSize);
        MemorySegment seg2 = createSegment(halfPageSize);
        seg1.put(0, bytes1);
        seg2.put(0, bytes2);

        // wap the second half of the first segment with the second segment

        int pos = 0;
        while (pos < halfPageSize) {
            int len = random.nextInt(pageSize / 40);
            len = Math.min(len, halfPageSize - pos);
            seg1.swapBytes(new byte[len], seg2, pos + halfPageSize, pos, len);
            pos += len;
        }

        // the second segment should now be all zeros, the first segment should have one in its
        // second half

        for (int i = 0; i < halfPageSize; i++) {
            assertThat(seg1.get(i)).isEqualTo((byte) 0);
            assertThat(seg2.get(i)).isEqualTo((byte) 0);
            assertThat(seg1.get(i + halfPageSize)).isEqualTo((byte) 1);
        }
    }

    // ------------------------------------------------------------------------
    //  Miscellaneous
    // ------------------------------------------------------------------------

    @TestTemplate
    public void testByteBufferWrapping() {
        MemorySegment seg = createSegment(1024);

        ByteBuffer buf1 = seg.wrap(13, 47);
        assertThat(buf1.position()).isEqualTo(13);
        assertThat(buf1.limit()).isEqualTo(60);
        assertThat(buf1.remaining()).isEqualTo(47);

        ByteBuffer buf2 = seg.wrap(500, 267);
        assertThat(buf2.position()).isEqualTo(500);
        assertThat(buf2.limit()).isEqualTo(767);
        assertThat(buf2.remaining()).isEqualTo(267);

        ByteBuffer buf3 = seg.wrap(0, 1024);
        assertThat(buf3.position()).isEqualTo(0);
        assertThat(buf3.limit()).isEqualTo(1024);
        assertThat(buf3.remaining()).isEqualTo(1024);

        // verify that operations on the byte buffer are correctly reflected
        // in the memory segment
        buf3.order(ByteOrder.LITTLE_ENDIAN);
        buf3.putInt(112, 651797651);
        assertThat(seg.getIntLittleEndian(112)).isEqualTo(651797651);

        buf3.order(ByteOrder.BIG_ENDIAN);
        buf3.putInt(187, 992288337);
        assertThat(seg.getIntBigEndian(187)).isEqualTo(992288337);

        // existing wraps should stay valid after freeing
        buf3.order(ByteOrder.LITTLE_ENDIAN);
        buf3.putInt(112, 651797651);
        assertThat(buf3.getInt(112)).isEqualTo(651797651);
        buf3.order(ByteOrder.BIG_ENDIAN);
        buf3.putInt(187, 992288337);
        assertThat(buf3.getInt(187)).isEqualTo(992288337);
    }

    // ------------------------------------------------------------------------
    //  Parametrization to run with different segment sizes
    // ------------------------------------------------------------------------

    @Parameters(name = "segment-size = {0}")
    public static Collection<Object[]> executionModes() {
        return Arrays.asList(
                new Object[] {32 * 1024}, new Object[] {4 * 1024}, new Object[] {512 * 1024});
    }
}
