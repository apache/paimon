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

package org.apache.paimon.fs;

import org.apache.paimon.utils.RandomUtil;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.util.Random;

/** Tests for {@link SeekablePositionedMappingInputStream}. */
public class SeekablePositionedMappingInputStreamTest {

    private static final Random RANDOM = new Random();

    @Test
    public void test() throws Exception {
        byte[] b = RandomUtil.randomBytes(100000);

        SeekableInputStream seekableInputStream = new ByteArraySeekableStream(b);
        int start = RANDOM.nextInt(50000);
        int length = RANDOM.nextInt(20000) + 30000;

        SeekablePositionedMappingInputStream seekablePositionedMappingInputStream =
                new SeekablePositionedMappingInputStream(seekableInputStream, start, length);

        // test seek
        int position = RANDOM.nextInt(20000);
        seekablePositionedMappingInputStream.seek(position);

        for (int i = 0; i < 100; i++) {
            Assertions.assertThat(b[start + position + i])
                    .isEqualTo((byte) seekablePositionedMappingInputStream.read());
        }

        // test read a lot
        position = RANDOM.nextInt(20000);
        seekablePositionedMappingInputStream.seek(position);

        byte[] bytes = new byte[100];
        byte[] copy = new byte[100];
        System.arraycopy(b, start + position, copy, 0, 100);
        seekablePositionedMappingInputStream.read(bytes, 0, 100);
        Assertions.assertThat(bytes).containsExactly(copy);

        // test getPos
        Assertions.assertThat(seekableInputStream.getPos())
                .isEqualTo(seekablePositionedMappingInputStream.getPos() + start);

        // test read fully
        bytes = new byte[length];
        System.arraycopy(b, start, bytes, 0, length);
        Assertions.assertThat(bytes)
                .containsExactly(seekablePositionedMappingInputStream.readAllBytes());

        Assertions.assertThatCode(() -> seekablePositionedMappingInputStream.seek(length - 1))
                .isEqualTo(null);
        Assertions.assertThatCode(() -> seekablePositionedMappingInputStream.seek(length))
                .isInstanceOf(EOFException.class);
        Assertions.assertThatCode(() -> seekablePositionedMappingInputStream.seek(length + 1))
                .isInstanceOf(EOFException.class);
    }
}
