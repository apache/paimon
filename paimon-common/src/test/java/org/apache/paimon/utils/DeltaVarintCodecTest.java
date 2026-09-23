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

package org.apache.paimon.utils;

import org.apache.paimon.io.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for count-prefixed streaming delta/varint payloads. */
class DeltaVarintCodecTest {
    @Test
    void fixedBytesAndSlicedBuffer() throws Exception {
        long[] values = {10, 12, 14, 16, 16, 1024};
        byte[] encoded = encode(10, values);
        assertThat(encoded).containsExactly(new byte[] {6, 0, 2, 2, 2, 0, (byte) 0xf0, 7});
        ByteBuffer data = ByteBuffer.allocate(encoded.length + 4);
        data.position(2);
        data.put(encoded);
        data.limit(data.position()).position(2);
        DeltaVarintCodec.Reader reader = new DeltaVarintCodec.Reader(data, 10, 1024);
        assertThat(reader.count()).isEqualTo(values.length);
        for (long value : values) {
            assertThat(reader.hasNext()).isTrue();
            assertThat(reader.next()).isEqualTo(value);
        }
        assertThat(reader.hasNext()).isFalse();
        assertThat(data.hasRemaining()).isFalse();
        assertThatThrownBy(reader::next).isInstanceOf(IOException.class);
    }

    @Test
    void longBoundariesAndEmptySequence() throws Exception {
        byte[] encoded = encode(0, Long.MAX_VALUE);
        assertThat(encoded).hasSize(10);
        assertThat(new DeltaVarintCodec.Reader(ByteBuffer.wrap(encoded), 0, Long.MAX_VALUE).next())
                .isEqualTo(Long.MAX_VALUE);
        assertThat(encode(Long.MAX_VALUE - 1, Long.MAX_VALUE, Long.MAX_VALUE))
                .containsExactly(new byte[] {2, 1, 0});
        DeltaVarintCodec.Reader empty =
                new DeltaVarintCodec.Reader(ByteBuffer.wrap(new byte[] {0}), 0, 0);
        assertThat(empty.hasNext()).isFalse();
        assertThatThrownBy(empty::next).isInstanceOf(IOException.class);
        assertThat(encode(0)).containsExactly((byte) 0);
    }

    @Test
    void stopsBeforeUnusedMalformedData() throws Exception {
        DeltaVarintCodec.Reader reader =
                new DeltaVarintCodec.Reader(
                        ByteBuffer.wrap(new byte[] {2, 0, (byte) 0x80}), 10, 100);
        assertThat(reader.next()).isEqualTo(10);
        assertThat(reader.hasNext()).isTrue();
        assertThatThrownBy(reader::next).isInstanceOf(IOException.class);
    }

    @Test
    void rejectsInvalidCountsBoundsAndOrdering() throws Exception {
        assertThatThrownBy(() -> new DeltaVarintCodec.Reader(ByteBuffer.allocate(0), 0, 1))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(
                        () -> new DeltaVarintCodec.Reader(ByteBuffer.wrap(new byte[] {2, 0}), 0, 1))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(
                        () ->
                                new DeltaVarintCodec.Reader(
                                        ByteBuffer.wrap(new byte[] {(byte) 0x81, 0, 0}), 0, 1))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> new DeltaVarintCodec.Reader(ByteBuffer.wrap(new byte[] {0}), 2, 1))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> new DeltaVarintCodec.Writer(new DataOutputSerializer(8), 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> encode(10, 9)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> encode(0, 2, 1)).isInstanceOf(IOException.class);
        DeltaVarintCodec.Reader overflow =
                new DeltaVarintCodec.Reader(
                        ByteBuffer.wrap(encode(0, Long.MAX_VALUE)), 1, Long.MAX_VALUE);
        assertThatThrownBy(overflow::next).isInstanceOf(IOException.class);
    }

    @Test
    void rejectsMalformedVarints() throws Exception {
        byte[] overlong = new byte[10];
        Arrays.fill(overlong, (byte) 0x80);
        for (byte[] bytes :
                Arrays.asList(new byte[] {(byte) 0x80}, new byte[] {(byte) 0x81, 0}, overlong)) {
            byte[] payload = new byte[bytes.length + 1];
            payload[0] = 1;
            System.arraycopy(bytes, 0, payload, 1, bytes.length);
            DeltaVarintCodec.Reader reader =
                    new DeltaVarintCodec.Reader(ByteBuffer.wrap(payload), 0, Long.MAX_VALUE);
            assertThatThrownBy(reader::next).isInstanceOf(IOException.class);
        }
    }

    @Test
    void randomizedRoundTrips() throws Exception {
        Random random = new Random(9845);
        for (int trial = 0; trial < 1000; trial++) {
            long base = random.nextInt(10000);
            long[] values = new long[random.nextInt(128)];
            long value = base;
            for (int i = 0; i < values.length; i++) {
                value += random.nextInt(10000);
                values[i] = value;
            }
            DeltaVarintCodec.Reader reader =
                    new DeltaVarintCodec.Reader(ByteBuffer.wrap(encode(base, values)), base, value);
            for (long expected : values) {
                assertThat(reader.next()).isEqualTo(expected);
            }
            assertThat(reader.hasNext()).isFalse();
        }
    }

    private static byte[] encode(long base, long... values) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        DeltaVarintCodec.Writer writer = new DeltaVarintCodec.Writer(out, values.length, base);
        for (long value : values) {
            writer.write(value);
        }
        return out.getCopyOfBuffer();
    }

    @Test
    void concatenatedPayloadsRetainTheirOwnBoundaries() throws Exception {
        byte[] first = encode(0, 1, 1, 3);
        byte[] second = encode(0, 4, 8, 16);
        ByteBuffer buffer = ByteBuffer.allocate(first.length + second.length);
        buffer.put(first).put(second).flip();
        DeltaVarintCodec.Reader reader = new DeltaVarintCodec.Reader(buffer, 0, 3);
        assertThat(reader.count()).isEqualTo(3);
        assertThat(reader.next()).isEqualTo(1);
        assertThat(reader.next()).isEqualTo(1);
        assertThat(reader.next()).isEqualTo(3);
        assertThat(reader.hasNext()).isFalse();
        assertThat(buffer.position()).isEqualTo(first.length);
        reader = new DeltaVarintCodec.Reader(buffer, 0, 16);
        assertThat(reader.next()).isEqualTo(4);
        assertThat(reader.next()).isEqualTo(8);
        assertThat(reader.next()).isEqualTo(16);
        assertThat(buffer.hasRemaining()).isFalse();
    }

    @Test
    void signedIntDeltasPreserveDecreasingTotals() throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(32);
        DeltaVarintCodec.Writer writer = new DeltaVarintCodec.Writer(out, 3, 0, true);
        for (int value : new int[] {4, 8, 4}) {
            writer.write(value);
        }
        assertThat(out.getCopyOfBuffer()).containsExactly(new byte[] {3, 8, 8, 7});
        DeltaVarintCodec.Reader reader =
                new DeltaVarintCodec.Reader(ByteBuffer.wrap(out.getCopyOfBuffer()), 0, 8, true);
        assertThat(reader.next()).isEqualTo(4);
        assertThat(reader.next()).isEqualTo(8);
        assertThat(reader.next()).isEqualTo(4);
        assertThat(reader.hasNext()).isFalse();
        assertThatThrownBy(() -> writer.write(4)).isInstanceOf(IOException.class);
    }

    @Test
    void signedIntBoundariesAndRandomSequences() throws Exception {
        Random random = new Random(20260916);
        for (int trial = 0; trial < 100; trial++) {
            int[] values = new int[128];
            for (int i = 0; i < values.length; i++) {
                values[i] = random.nextInt(Integer.MAX_VALUE);
            }
            values[0] = Integer.MAX_VALUE;
            values[1] = 0;
            values[2] = Integer.MAX_VALUE;
            DataOutputSerializer out = new DataOutputSerializer(32);
            DeltaVarintCodec.Writer writer =
                    new DeltaVarintCodec.Writer(out, values.length, 0, true);
            for (int value : values) {
                writer.write(value);
            }
            ByteBuffer buffer = ByteBuffer.wrap(out.getCopyOfBuffer());
            DeltaVarintCodec.Reader reader =
                    new DeltaVarintCodec.Reader(buffer, 0, Integer.MAX_VALUE, true);
            for (int value : values) {
                assertThat(reader.next()).isEqualTo(value);
            }
            assertThat(buffer.hasRemaining()).isFalse();
        }
        DeltaVarintCodec.Reader negative =
                new DeltaVarintCodec.Reader(ByteBuffer.wrap(new byte[] {1, 1}), 0, 10, true);
        assertThatThrownBy(negative::next).isInstanceOf(IOException.class);
        DeltaVarintCodec.Reader tooLarge =
                new DeltaVarintCodec.Reader(ByteBuffer.wrap(new byte[] {1, 22}), 0, 10, true);
        assertThatThrownBy(tooLarge::next).isInstanceOf(IOException.class);
        assertThatThrownBy(
                        () ->
                                new DeltaVarintCodec.Writer(
                                        new DataOutputSerializer(8),
                                        0,
                                        (long) Integer.MAX_VALUE + 1,
                                        true))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
