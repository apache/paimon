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

package org.apache.paimon.format.text;

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link StandardLineReader}. */
public class StandardLineReaderTest {

    @TempDir java.nio.file.Path tempDir;

    private LocalFileIO fileIO;
    private java.nio.file.Path testFile;

    @BeforeEach
    public void setUp() {
        fileIO = new LocalFileIO();
        testFile = tempDir.resolve("test.txt");
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (java.nio.file.Files.exists(testFile)) {
            java.nio.file.Files.delete(testFile);
        }
    }

    /**
     * After the stream is closed, we cannot use getPos, so we need to ensure that it is manually
     * closed.
     */
    @Test
    public void testCloseShouldNotBeInvoked() throws IOException {
        String content = "line1\nline2\nline3";
        AtomicBoolean closed = new AtomicBoolean(false);
        try (TextLineReader reader =
                TextLineReader.create(
                        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)) {
                            @Override
                            public void close() {
                                closed.set(true);
                            }
                        },
                        "\n",
                        0,
                        null)) {
            assertThat(reader.readLine()).isEqualTo("line1");
            assertThat(reader.readLine()).isEqualTo("line2");
            assertThat(reader.readLine()).isEqualTo("line3");
            assertThat(reader.readLine()).isNull();
            assertThat(closed.get()).isFalse();
        }
    }

    @Test
    public void testReadLineWithLineFeedDelimiter() throws IOException {
        // Test basic functionality with \n delimiter
        String content = "line1\nline2\nline3";
        try (TextLineReader reader =
                TextLineReader.create(
                        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                        "\n",
                        0,
                        null)) {
            assertThat(reader.readLine()).isEqualTo("line1");
            assertThat(reader.readLine()).isEqualTo("line2");
            assertThat(reader.readLine()).isEqualTo("line3");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithCarriageReturnDelimiter() throws IOException {
        // Test basic functionality with \r delimiter
        String content = "line1\rline2\rline3";
        try (TextLineReader reader =
                TextLineReader.create(
                        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                        "\r",
                        0,
                        null)) {
            assertThat(reader.readLine()).isEqualTo("line1");
            assertThat(reader.readLine()).isEqualTo("line2");
            assertThat(reader.readLine()).isEqualTo("line3");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithCRLF() throws IOException {
        // Test basic functionality with \r\n delimiter
        String content = "line1\r\nline2\r\nline3";
        try (TextLineReader reader =
                TextLineReader.create(
                        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                        "\r\n",
                        0,
                        null)) {
            assertThat(reader.readLine()).isEqualTo("line1");
            assertThat(reader.readLine()).isEqualTo("line2");
            assertThat(reader.readLine()).isEqualTo("line3");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithEmptyLines() throws IOException {
        // Test reading empty lines
        String content = "line1\n\nline3\n";
        try (TextLineReader reader =
                TextLineReader.create(
                        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                        null,
                        0,
                        null)) {
            assertThat(reader.readLine()).isEqualTo("line1");
            assertThat(reader.readLine()).isEqualTo("");
            assertThat(reader.readLine()).isEqualTo("line3");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithBOM() throws IOException {
        // Test reading file with BOM
        byte[] bom = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
        byte[] content = "line1\nline2".getBytes(StandardCharsets.UTF_8);
        byte[] fullContent = new byte[bom.length + content.length];
        System.arraycopy(bom, 0, fullContent, 0, bom.length);
        System.arraycopy(content, 0, fullContent, bom.length, content.length);

        try (TextLineReader reader =
                TextLineReader.create(new ByteArrayInputStream(fullContent), null, 0, null)) {
            assertThat(reader.readLine()).isEqualTo("line1");
            assertThat(reader.readLine()).isEqualTo("line2");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithSplitAtBeginning() throws IOException {
        // Test reading with split at the beginning
        String content = "line1\nline2\nline3\nline4";
        try (TextLineReader reader =
                TextLineReader.create(
                        new ByteArraySeekableStream(content.getBytes(StandardCharsets.UTF_8)),
                        null,
                        0,
                        10L)) {
            assertThat(reader.readLine()).isEqualTo("line1");
            assertThat(reader.readLine()).isEqualTo("line2");
            // Should stop reading after reaching the split length
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithSplitInMiddle() throws IOException {
        // Test reading with split in the middle
        String content = "line1\nline2\nline3\nline4";
        try (TextLineReader reader =
                TextLineReader.create(
                        new ByteArraySeekableStream(content.getBytes(StandardCharsets.UTF_8)),
                        null,
                        3,
                        null)) {
            // Should skip the first line and start from the second line
            assertThat(reader.readLine()).isEqualTo("line2");
            assertThat(reader.readLine()).isEqualTo("line3");
            assertThat(reader.readLine()).isEqualTo("line4");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithEmptyFile() throws IOException {
        // Test reading an empty file
        String content = "";
        try (TextLineReader reader =
                TextLineReader.create(
                        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                        null,
                        0,
                        null)) {
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithSingleLine() throws IOException {
        // Test reading a single line without line terminator
        String content = "single line";
        try (TextLineReader reader =
                TextLineReader.create(
                        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                        null,
                        0,
                        null)) {
            assertThat(reader.readLine()).isEqualTo("single line");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithSingleLineAndNewline() throws IOException {
        // Test reading a single line with line terminator
        String content = "single line\n";
        try (TextLineReader reader =
                TextLineReader.create(
                        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                        null,
                        0,
                        null)) {
            assertThat(reader.readLine()).isEqualTo("single line");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithVeryLongLine() throws IOException {
        // Test reading a very long line
        StringBuilder longLine = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            longLine.append("a");
        }
        String content = longLine.toString() + "\nline2";
        try (TextLineReader reader =
                TextLineReader.create(
                        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                        null,
                        0,
                        null)) {
            assertThat(reader.readLine()).isEqualTo(longLine.toString());
            assertThat(reader.readLine()).isEqualTo("line2");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithSeekableInputStream() throws IOException {
        // Test reading with seekable input stream
        String content = "line1\nline2\nline3";
        createFile(content);

        try (SeekableInputStream inputStream =
                fileIO.newInputStream(new org.apache.paimon.fs.Path(testFile.toString()))) {
            try (TextLineReader reader = TextLineReader.create(inputStream, null, 3, null)) {
                // Should skip the first line and start from the second line
                assertThat(reader.readLine()).isEqualTo("line2");
                assertThat(reader.readLine()).isEqualTo("line3");
                assertThat(reader.readLine()).isNull();
            }
        }
    }

    @Test
    public void testReadLineWithNonSeekableInputStreamAndNonZeroOffset() throws IOException {
        // Test that non-seekable input stream with non-zero offset throws exception
        String content = "line1\nline2\nline3";
        ByteArrayInputStream inputStream =
                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> new StandardLineReader(inputStream, 1, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Current position only supported for uncompressed files");
    }

    /** Helper method to create a test file with the given content. */
    private void createFile(String content) throws IOException {
        try (PositionOutputStream out =
                fileIO.newOutputStream(new org.apache.paimon.fs.Path(testFile.toString()), false)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }
}
