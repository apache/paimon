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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TextFileReader}. */
public class TextFileReaderTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path testFile;
    private RowType rowType;

    @BeforeEach
    public void setUp() {
        fileIO = new LocalFileIO();
        testFile = new Path(tempDir.toString(), "test.txt");
        rowType = DataTypes.ROW(DataTypes.STRING());
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (fileIO.exists(testFile)) {
            fileIO.delete(testFile, false);
        }
    }

    @Test
    public void testReadLineWithDefaultDelimiter() throws IOException {
        // Write test data with \n delimiter
        writeFile("line1\nline2\nline3");

        List<String> lines = readAllLines(null);

        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).isEqualTo("line1");
        assertThat(lines.get(1)).isEqualTo("line2");
        assertThat(lines.get(2)).isEqualTo("line3");
    }

    @Test
    public void testReadLineWithCarriageReturn() throws IOException {
        writeFile("line1\rline2\rline3");

        List<String> lines = readAllLines("\r");

        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).isEqualTo("line1");
        assertThat(lines.get(1)).isEqualTo("line2");
        assertThat(lines.get(2)).isEqualTo("line3");
    }

    @Test
    public void testReadLineWithCRLF() throws IOException {
        writeFile("line1\r\nline2\r\nline3");

        List<String> lines = readAllLines("\r\n");

        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).isEqualTo("line1");
        assertThat(lines.get(1)).isEqualTo("line2");
        assertThat(lines.get(2)).isEqualTo("line3");
    }

    @Test
    public void testReadLineWithCustomMultiCharDelimiter() throws IOException {
        writeFile("||ab|aba||||line3||line4||");

        List<String> lines = readAllLines("||");

        assertThat(lines).hasSize(5);
        assertThat(lines.get(0)).isEqualTo("");
        assertThat(lines.get(1)).isEqualTo("ab|aba");
        assertThat(lines.get(2)).isEqualTo("");
        assertThat(lines.get(3)).isEqualTo("line3");
        assertThat(lines.get(4)).isEqualTo("line4");
    }

    @Test
    public void testReadLineWithEmptyFile() throws IOException {
        writeFile("");

        List<String> lines = readAllLines("||");

        assertThat(lines).isEmpty();
    }

    @Test
    public void test() throws IOException {
        writeFile("ababcab");

        List<String> lines = readAllLines("abc");

        assertThat(lines).hasSize(2);
    }

    @Test
    public void testReadLineWithSingleLine() throws IOException {
        writeFile("single line");

        List<String> lines = readAllLines("||");

        assertThat(lines).hasSize(1);
        assertThat(lines.get(0)).isEqualTo("single line");
    }

    @Test
    public void testReadLineWithUnicodeDelimiter() throws IOException {
        writeFile("line1§§line2§§line3");

        List<String> lines = readAllLines("§§");

        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).isEqualTo("line1");
        assertThat(lines.get(1)).isEqualTo("line2");
        assertThat(lines.get(2)).isEqualTo("line3");
    }

    @Test
    public void testReadLineWithOverlappingPattern() throws IOException {
        // Test delimiter "aba" with content "xababa"
        // Should correctly split at "aba" boundaries
        writeFile("xababay");

        List<String> lines = readAllLines("aba");

        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).isEqualTo("x");
        assertThat(lines.get(1)).isEqualTo("bay");
    }

    @Test
    public void testReadLineWithMultiByteUTF8Delimiter() throws IOException {
        // Emoji delimiter
        writeFile("line1😀line2😀line3");

        List<String> lines = readAllLines("😀");

        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).isEqualTo("line1");
        assertThat(lines.get(1)).isEqualTo("line2");
        assertThat(lines.get(2)).isEqualTo("line3");
    }

    private void writeFile(String content) throws IOException {
        try (PositionOutputStream out = fileIO.newOutputStream(testFile, false)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    private List<String> readAllLines(@Nullable String delimiter) throws IOException {
        List<String> lines = new ArrayList<>();
        try (TestTextFileReader reader =
                new TestTextFileReader(fileIO, testFile, rowType, delimiter)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }

    /** Concrete implementation of BaseTextFileReader for testing. */
    private static class TestTextFileReader extends AbstractTextFileReader {

        public TestTextFileReader(
                FileIO fileIO, Path filePath, RowType rowType, String recordDelimiter)
                throws IOException {
            super(fileIO, filePath, rowType, recordDelimiter, 0, null);
        }

        @Nullable
        @Override
        protected InternalRow parseLine(String line) throws IOException {
            // Not used in these tests
            return null;
        }
    }
}
