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

/** Tests for the shared streaming unsigned delta/varint codec. */
class DeltaVarintCodecTest {
    @Test
    void fixedBytesAndSlicedBuffer() throws Exception {
        long[] values = {10, 12, 14, 16, 16, 1024};
        byte[] encoded = encode(10, values);
        assertThat(encoded).containsExactly(new byte[] {0, 2, 2, 2, 0, (byte) 0xf0, 7});
        ByteBuffer data = ByteBuffer.allocate(encoded.length + 4);
        data.position(2);
        data.put(encoded);
        data.limit(data.position()).position(2);
        DeltaVarintCodec.Reader reader = new DeltaVarintCodec.Reader(data, values.length, 10, 1024);
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
        assertThat(encoded).hasSize(9);
        assertThat(
                        new DeltaVarintCodec.Reader(ByteBuffer.wrap(encoded), 1, 0, Long.MAX_VALUE)
                                .next())
                .isEqualTo(Long.MAX_VALUE);
        assertThat(encode(Long.MAX_VALUE - 1, Long.MAX_VALUE, Long.MAX_VALUE))
                .containsExactly(new byte[] {1, 0});
        DeltaVarintCodec.Reader empty =
                new DeltaVarintCodec.Reader(ByteBuffer.allocate(0), 0, 0, 0);
        assertThat(empty.hasNext()).isFalse();
        assertThatThrownBy(empty::next).isInstanceOf(IOException.class);
        assertThat(encode(0)).isEmpty();
    }

    @Test
    void stopsBeforeUnusedMalformedData() throws Exception {
        DeltaVarintCodec.Reader reader =
                new DeltaVarintCodec.Reader(
                        ByteBuffer.wrap(new byte[] {0, (byte) 0x80}), 2, 10, 100);
        assertThat(reader.next()).isEqualTo(10);
        assertThat(reader.hasNext()).isTrue();
        assertThatThrownBy(reader::next).isInstanceOf(IOException.class);
    }

    @Test
    void rejectsInvalidCountsBoundsAndOrdering() throws Exception {
        assertThatThrownBy(() -> new DeltaVarintCodec.Reader(ByteBuffer.allocate(0), -1, 0, 1))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(
                        () -> new DeltaVarintCodec.Reader(ByteBuffer.wrap(new byte[] {0}), 2, 0, 1))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(
                        () -> new DeltaVarintCodec.Reader(ByteBuffer.wrap(new byte[] {0}), 0, 0, 1))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> new DeltaVarintCodec.Reader(ByteBuffer.allocate(0), 0, 2, 1))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> new DeltaVarintCodec.Writer(new DataOutputSerializer(8), -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> encode(10, 9)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> encode(0, 2, 1)).isInstanceOf(IOException.class);
        DeltaVarintCodec.Reader trailing =
                new DeltaVarintCodec.Reader(ByteBuffer.wrap(new byte[] {1, 2}), 1, 0, 10);
        assertThatThrownBy(trailing::next).isInstanceOf(IOException.class);
        DeltaVarintCodec.Reader overflow =
                new DeltaVarintCodec.Reader(
                        ByteBuffer.wrap(encode(0, Long.MAX_VALUE)), 1, 1, Long.MAX_VALUE);
        assertThatThrownBy(overflow::next).isInstanceOf(IOException.class);
    }

    @Test
    void rejectsMalformedVarints() throws Exception {
        byte[] overlong = new byte[10];
        Arrays.fill(overlong, (byte) 0x80);
        for (byte[] bytes :
                Arrays.asList(new byte[] {(byte) 0x80}, new byte[] {(byte) 0x81, 0}, overlong)) {
            DeltaVarintCodec.Reader reader =
                    new DeltaVarintCodec.Reader(ByteBuffer.wrap(bytes), 1, 0, Long.MAX_VALUE);
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
                    new DeltaVarintCodec.Reader(
                            ByteBuffer.wrap(encode(base, values)), values.length, base, value);
            for (long expected : values) {
                assertThat(reader.next()).isEqualTo(expected);
            }
            assertThat(reader.hasNext()).isFalse();
        }
    }

    private static byte[] encode(long base, long... values) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        DeltaVarintCodec.Writer writer = new DeltaVarintCodec.Writer(out, base);
        for (long value : values) {
            writer.write(value);
        }
        return out.getCopyOfBuffer();
    }
}
