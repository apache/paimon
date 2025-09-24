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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for {@link OffsetSeekableInputStream}. */
public class OffsetSeekableInputStreamTest {

    private byte[] testData;
    private ByteArraySeekableStream wrapped;

    @BeforeEach
    public void setUp() {
        testData = new byte[20];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) i;
        }
        wrapped = new ByteArraySeekableStream(testData);
    }

    @Test
    public void testConstructor() throws IOException {
        long offset = 5;
        long length = 10;
        try (OffsetSeekableInputStream stream =
                new OffsetSeekableInputStream(wrapped, offset, length)) {
            assertThat(wrapped.getPos()).isEqualTo(offset);
            assertThat(stream.getPos()).isZero();
        }
    }

    @Test
    public void testGetPosAndSeek() throws IOException {
        long offset = 5;
        long length = 10;
        try (OffsetSeekableInputStream stream =
                new OffsetSeekableInputStream(wrapped, offset, length)) {
            stream.seek(3);
            assertThat(stream.getPos()).isEqualTo(3);
            assertThat(wrapped.getPos()).isEqualTo(offset + 3);

            stream.seek(length);
            assertThat(stream.getPos()).isEqualTo(length);
            assertThat(wrapped.getPos()).isEqualTo(offset + length);

            stream.seek(0);
            assertThat(stream.getPos()).isZero();
            assertThat(wrapped.getPos()).isEqualTo(offset);
        }
    }

    @Test
    public void testReadSingleByte() throws IOException {
        long offset = 5;
        long length = 10;
        try (OffsetSeekableInputStream stream =
                new OffsetSeekableInputStream(wrapped, offset, length)) {
            assertThat(stream.read()).isEqualTo(testData[5]);
            assertThat(stream.getPos()).isEqualTo(1);
            assertThat(wrapped.getPos()).isEqualTo(offset + 1);

            stream.seek(length - 1);
            assertThat(stream.read()).isEqualTo(testData[(int) (offset + length - 1)]);
            assertThat(stream.getPos()).isEqualTo(length);
        }
    }

    @Test
    public void testReadSingleByteAtEnd() throws IOException {
        long offset = 5;
        long length = 10;
        try (OffsetSeekableInputStream stream =
                new OffsetSeekableInputStream(wrapped, offset, length)) {
            stream.seek(length);
            assertThat(stream.read()).isEqualTo(-1);
        }
    }

    @Test
    public void testReadByteArray() throws IOException {
        long offset = 5;
        long length = 10;
        try (OffsetSeekableInputStream stream =
                new OffsetSeekableInputStream(wrapped, offset, length)) {
            byte[] buffer = new byte[5];
            int bytesRead = stream.read(buffer, 0, 5);

            assertThat(bytesRead).isEqualTo(5);
            assertThat(buffer).containsExactly(Arrays.copyOfRange(testData, 5, 10));
            assertThat(stream.getPos()).isEqualTo(5);
            assertThat(wrapped.getPos()).isEqualTo(offset + 5);
        }
    }

    @Test
    public void testReadByteArrayHittingEnd() throws IOException {
        long offset = 5;
        long length = 10;
        try (OffsetSeekableInputStream stream =
                new OffsetSeekableInputStream(wrapped, offset, length)) {
            stream.seek(7);
            byte[] buffer = new byte[5]; // Request more than available
            int bytesRead = stream.read(buffer, 0, 5);

            assertThat(bytesRead).isEqualTo(3); // Only 3 bytes should be read (10 - 7)
            byte[] expected = new byte[5];
            System.arraycopy(testData, 12, expected, 0, 3);
            assertThat(buffer).isEqualTo(expected);
            assertThat(stream.getPos()).isEqualTo(length);
            assertThat(wrapped.getPos()).isEqualTo(offset + length);
        }
    }

    @Test
    public void testReadByteArrayAtEnd() throws IOException {
        long offset = 5;
        long length = 10;
        try (OffsetSeekableInputStream stream =
                new OffsetSeekableInputStream(wrapped, offset, length)) {
            stream.seek(length);
            byte[] buffer = new byte[5];
            int bytesRead = stream.read(buffer, 0, 5);
            assertThat(bytesRead).isEqualTo(-1);
        }
    }

    @Test
    public void testClose() throws IOException {
        SeekableInputStream mockStream = mock(SeekableInputStream.class);
        OffsetSeekableInputStream offsetStream = new OffsetSeekableInputStream(mockStream, 0, 10);
        offsetStream.close();
        verify(mockStream, times(1)).close();
    }

    @Test
    public void testReadWithUnlimitedLength() throws IOException {
        long offset = 5;
        long length = -1; // Unlimited length
        try (OffsetSeekableInputStream stream =
                new OffsetSeekableInputStream(wrapped, offset, length)) {
            // Should be able to read beyond the original testData length
            byte[] buffer = new byte[10];
            int bytesRead = stream.read(buffer, 0, 10);

            assertThat(bytesRead).isEqualTo(10);
            assertThat(buffer).containsExactly(Arrays.copyOfRange(testData, 5, 15));
            assertThat(stream.getPos()).isEqualTo(10);
        }
    }

    @Test
    public void testReadByteArrayWithZeroLength() throws IOException {
        long offset = 5;
        long length = 10;
        try (OffsetSeekableInputStream stream =
                new OffsetSeekableInputStream(wrapped, offset, length)) {
            byte[] buffer = new byte[0];
            int bytesRead = stream.read(buffer, 0, 0);

            assertThat(bytesRead).isEqualTo(0);
        }
    }

    @Test
    public void testSeekBeyondLength() throws IOException {
        long offset = 5;
        long length = 10;
        try (OffsetSeekableInputStream stream =
                new OffsetSeekableInputStream(wrapped, offset, length)) {
            // Seeking beyond length should be allowed, but reading should return -1
            stream.seek(15);
            assertThat(stream.getPos()).isEqualTo(15);

            int result = stream.read();
            assertThat(result).isEqualTo(-1);
        }
    }
}
