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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;

import static org.apache.paimon.utils.RandomUtil.randomBytes;

/** Test for {@link ByteArraySeekableStream}. */
public class ByteArraySeekableStreamTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testBasic() throws IOException {
        int bl = 100000;
        byte[] b = randomBytes(bl);
        ByteArraySeekableStream byteArraySeekableStream = new ByteArraySeekableStream(b);

        Assertions.assertThat(byteArraySeekableStream.available()).isEqualTo(b.length);

        for (int i = 0; i < RANDOM.nextInt(1000); i++) {
            int position = RANDOM.nextInt(bl - 1);
            int length = RANDOM.nextInt(b.length - position - 1);
            byte[] expected = new byte[length];
            System.arraycopy(b, position, expected, 0, length);

            byte[] actual = new byte[length];
            byteArraySeekableStream.seek(position);
            byteArraySeekableStream.read(actual);
            Assertions.assertThat(actual).containsExactly(expected);
        }

        for (int i = 0; i < RANDOM.nextInt(1000); i++) {
            int position = RANDOM.nextInt(bl);
            byteArraySeekableStream.seek(position);
            for (int j = 0; j < 100; j++) {
                int testPosition = position + j;
                if (testPosition >= b.length) {
                    break;
                }
                Assertions.assertThat(b[testPosition])
                        .isEqualTo((byte) byteArraySeekableStream.read());
            }
        }
    }

    @Test
    public void testThrow() {
        int bl = 10;
        byte[] b = randomBytes(bl);
        ByteArraySeekableStream byteArraySeekableStream = new ByteArraySeekableStream(b);
        Assertions.assertThatCode(() -> byteArraySeekableStream.seek(10))
                .hasMessage("Can't seek position: 10, length is 10");
    }
}
