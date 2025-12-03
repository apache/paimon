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

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link StandardLineReader}. */
public class StandardLineReaderTest {

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
    public void testReadLineNoSplitting() throws IOException {
        // Test basic functionality with \n delimiter
        String content = "line1\nline2\nline3";
        try (TextLineReader reader = create(content, 0, null)) {
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
        try (TextLineReader reader = create(content, 0, null)) {
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
        try (TextLineReader reader = create(content, 0, null)) {
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
        try (TextLineReader reader = create(content, 0, null)) {
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

        try (TextLineReader reader = create(fullContent, 0, null)) {
            assertThat(reader.readLine()).isEqualTo("line1");
            assertThat(reader.readLine()).isEqualTo("line2");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithSplitAtBeginning() throws IOException {
        String content = "line1\nline2\nline3\nline4";
        try (TextLineReader reader = create(content, 0, 10L)) {
            assertThat(reader.readLine()).isEqualTo("line1");
            assertThat(reader.readLine()).isEqualTo("line2");
            assertThat(reader.readLine()).isNull();
        }
        try (TextLineReader reader = create(content, 10, 6L)) {
            assertThat(reader.readLine()).isEqualTo("line3");
            assertThat(reader.readLine()).isNull();
        }
        try (TextLineReader reader = create(content, 16, 7L)) {
            assertThat(reader.readLine()).isEqualTo("line4");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithSplitInEndTwoLines() throws IOException {
        String content = "line1\nline2\nline3\nline4";
        try (TextLineReader reader = create(content, 10, 13L)) {
            assertThat(reader.readLine()).isEqualTo("line3");
            assertThat(reader.readLine()).isEqualTo("line4");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithSplitInEndEmpty() throws IOException {
        String content = "line1\nline2\nline3\nline4";
        try (TextLineReader reader = create(content, 0, 18L)) {
            assertThat(reader.readLine()).isEqualTo("line1");
            assertThat(reader.readLine()).isEqualTo("line2");
            assertThat(reader.readLine()).isEqualTo("line3");
            assertThat(reader.readLine()).isEqualTo("line4");
            assertThat(reader.readLine()).isNull();
        }
        try (TextLineReader reader = create(content, 18, 5L)) {
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithRandomSplit() throws IOException {
        String content = "line1\nline2\nline3\nline4";
        int splitSize = ThreadLocalRandom.current().nextInt(content.length()) + 1;
        int remainLen = content.length();
        List<String> lines = new ArrayList<>();
        while (remainLen > 0) {
            int len = Math.min(remainLen, splitSize);
            try (TextLineReader reader =
                    create(content, content.length() - remainLen, (long) len)) {
                while (true) {
                    String line = reader.readLine();
                    if (line != null) {
                        lines.add(line);
                    } else {
                        break;
                    }
                }
            }
            remainLen -= len;
        }
        assertThat(lines).containsExactly("line1", "line2", "line3", "line4");
    }

    @Test
    public void testReadLineWithEmptyFile() throws IOException {
        String content = "";
        try (TextLineReader reader = create(content, 0, null)) {
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithSingleLine() throws IOException {
        String content = "single line";
        try (TextLineReader reader = create(content, 0, null)) {
            assertThat(reader.readLine()).isEqualTo("single line");
            assertThat(reader.readLine()).isNull();
        }
    }

    @Test
    public void testReadLineWithSingleLineAndNewline() throws IOException {
        String content = "single line\n";
        try (TextLineReader reader = create(content, 0, null)) {
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
        String content = longLine + "\nline2";
        try (TextLineReader reader = create(content, 0, null)) {
            assertThat(reader.readLine()).isEqualTo(longLine.toString());
            assertThat(reader.readLine()).isEqualTo("line2");
            assertThat(reader.readLine()).isNull();
        }
    }

    private TextLineReader create(String content, long offset, @Nullable Long length)
            throws IOException {
        return create(content.getBytes(StandardCharsets.UTF_8), offset, length);
    }

    private TextLineReader create(byte[] content, long offset, @Nullable Long length)
            throws IOException {
        ByteArraySeekableStream input = new ByteArraySeekableStream(content);
        return TextLineReader.create(input, null, offset, length);
    }
}
