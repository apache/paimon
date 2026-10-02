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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.paimon.utils.VarLengthIntUtils.decodeInt;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeLong;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for variable-length integer buffer decoding. */
class VarLengthIntUtilsTest {

    @Test
    void decodeIntBoundariesAndBufferPosition() throws Exception {
        int[] values = {
            0,
            1,
            127,
            128,
            16383,
            16384,
            (1 << 21) - 1,
            1 << 21,
            (1 << 28) - 1,
            1 << 28,
            Integer.MAX_VALUE
        };
        DataOutputSerializer out = new DataOutputSerializer(64);
        for (int value : values) {
            encodeInt(out, value);
        }
        byte[] bytes = out.getCopyOfBuffer();
        for (ByteBuffer buffer :
                Arrays.asList(
                        ByteBuffer.allocate(bytes.length + 4),
                        ByteBuffer.allocateDirect(bytes.length + 4))) {
            buffer.position(2);
            buffer.put(bytes).put((byte) 42).flip();
            buffer.position(2);
            ByteBuffer in = buffer.slice().asReadOnlyBuffer();
            for (int value : values) {
                assertThat(decodeInt(in)).isEqualTo(value);
            }
            assertThat(in.position()).isEqualTo(bytes.length);
            assertThat(in.get()).isEqualTo((byte) 42);
        }
    }

    @Test
    void decodeIntRejectsOverflow() throws Exception {
        for (long value : new long[] {(long) Integer.MAX_VALUE + 1, 1L << 32, Long.MAX_VALUE}) {
            DataOutputSerializer out = new DataOutputSerializer(16);
            encodeLong(out, value);
            assertThatThrownBy(() -> decodeInt(ByteBuffer.wrap(out.getCopyOfBuffer())))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Integer.MAX_VALUE");
        }
    }

    @Test
    void decodeIntRejectsTruncatedAndMalformedValues() {
        for (byte[] bytes :
                Arrays.asList(new byte[0], new byte[] {(byte) 0x80}, new byte[] {(byte) 0xff})) {
            assertThatThrownBy(() -> decodeInt(ByteBuffer.wrap(bytes)))
                    .isInstanceOf(EOFException.class);
        }
        byte[] overlong = new byte[10];
        Arrays.fill(overlong, (byte) 0x80);
        for (byte[] bytes :
                Arrays.asList(new byte[] {(byte) 0x80, 0}, new byte[] {(byte) 0x81, 0}, overlong)) {
            assertThatThrownBy(() -> decodeInt(ByteBuffer.wrap(bytes)))
                    .isInstanceOf(IOException.class);
        }
    }
}
