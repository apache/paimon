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

package org.apache.paimon.io;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataInputDeserializer}. */
class DataInputDeserializerTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                "a",
                "ab",
                "\n",
                "\r",
                "\r\n",
                "ab\n",
                "ab\r",
                "ab\r\n",
                "ab\rcd",
                "ab\ncd",
                "ab\r\ncd",
                "\r\r",
                "\n\n",
                "ab\r\n\r\ncd",
                "\0\u0080\u00ff"
            })
    void testReadLineMatchesDataInputStream(String text) throws IOException {
        byte[] bytes = text.getBytes(ISO_8859_1);
        assertLines(new DataInputDeserializer(bytes), bytes);

        byte[] padded = new byte[bytes.length + 2];
        padded[0] = 'x';
        padded[padded.length - 1] = '\n';
        System.arraycopy(bytes, 0, padded, 1, bytes.length);
        assertLines(new DataInputDeserializer(padded, 1, bytes.length), bytes);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\n", "\r", "\r\n"})
    void testReadBinaryValuesAfterLine(String separator) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeBytes("line" + separator);
            output.writeInt(0x12345678);
            output.writeLong(0x0102030405060708L);
            output.writeUTF("\u4e2d\u6587");
        }

        DataInputDeserializer input = new DataInputDeserializer(bytes.toByteArray());
        assertThat(input.readLine()).isEqualTo("line");
        assertThat(input.readInt()).isEqualTo(0x12345678);
        assertThat(input.readLong()).isEqualTo(0x0102030405060708L);
        assertThat(input.readUTF()).isEqualTo("\u4e2d\u6587");
        assertThat(input.available()).isZero();
    }

    @SuppressWarnings("deprecation")
    private static void assertLines(DataInputDeserializer input, byte[] bytes) throws IOException {
        int start = input.getPosition();
        try (DataInputStream reference = new DataInputStream(new ByteArrayInputStream(bytes))) {
            String expected;
            do {
                expected = reference.readLine();
                assertThat(input.readLine()).isEqualTo(expected);
                assertThat(input.available()).isEqualTo(reference.available());
                assertThat(input.getPosition())
                        .isEqualTo(start + bytes.length - reference.available());
            } while (expected != null);
            assertThat(input.readLine()).isNull();
        }
    }
}
