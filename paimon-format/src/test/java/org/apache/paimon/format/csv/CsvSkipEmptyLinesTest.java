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

package org.apache.paimon.format.csv;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CsvOptions#SKIP_EMPTY_LINES}. */
class CsvSkipEmptyLinesTest {

    // Tab-separated, with an empty line, a single-space line and a tab-only line in between.
    private static final String CONTENT = "a\t1\n\n \nb\t2\n\t\n\r\nc\t3\n";

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testDefaultReadsBlankLinesAsNullRows() throws IOException {
        List<String> rows = read(CONTENT, false, "plain.csv");
        assertThat(rows)
                .containsExactly(
                        "a,1", "null,null", "null,null", "b,2", "null,null", "null,null", "c,3");
    }

    @Test
    void testSkipEmptyLines() throws IOException {
        List<String> rows = read(CONTENT, true, "plain.csv");
        // Like Spark, a line holding only a tab delimiter trims to empty and is skipped too.
        assertThat(rows).containsExactly("a,1", "b,2", "c,3");
    }

    @Test
    void testSkipEmptyLinesInGzipFile() throws IOException {
        List<String> rows = read(CONTENT, true, "data.csv.gz");
        assertThat(rows).containsExactly("a,1", "b,2", "c,3");
    }

    @Test
    void testSkipEmptyLinesWithHeader() throws IOException {
        Options options = baseOptions(true);
        options.set(CsvOptions.INCLUDE_HEADER, true);
        List<String> rows = read("h1\th2\n\na\t1\n \nb\t2\n", options, "header.csv");
        assertThat(rows).containsExactly("a,1", "b,2");
    }

    @Test
    void testSkipEmptyLinesWithHiveDefaultDelimiter() throws IOException {
        // An all-NULL row written with Hive's default delimiter is a single \u0001, which
        // String#trim removes, so it is skipped like in Spark.
        Options options = baseOptions(true);
        options.set(CsvOptions.FIELD_DELIMITER, "\u0001");
        List<String> rows = read("a\u00011\n\u0001\nb\u00012\n", options, "hive.csv");
        assertThat(rows).containsExactly("a,1", "b,2");
    }

    private static Options baseOptions(boolean skipEmptyLines) {
        Options options = new Options();
        options.set(CsvOptions.FIELD_DELIMITER, "\t");
        options.set(CsvOptions.SKIP_EMPTY_LINES, skipEmptyLines);
        return options;
    }

    private List<String> read(String content, boolean skipEmptyLines, String fileName)
            throws IOException {
        return read(content, baseOptions(skipEmptyLines), fileName);
    }

    private List<String> read(String content, Options options, String fileName) throws IOException {
        java.nio.file.Path file = tempDir.resolve(fileName);
        try (OutputStream out =
                fileName.endsWith(".gz")
                        ? new GZIPOutputStream(Files.newOutputStream(file))
                        : Files.newOutputStream(file)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
        }

        CsvFileFormat format =
                new CsvFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));
        RowType rowType = RowType.of(DataTypes.STRING(), DataTypes.STRING());
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(file.toString());

        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, path, fileIO.getFileSize(path), null, null))) {
            reader.forEachRemaining(
                    row ->
                            rows.add(
                                    (row.isNullAt(0) ? "null" : row.getString(0).toString())
                                            + ","
                                            + (row.isNullAt(1)
                                                    ? "null"
                                                    : row.getString(1).toString())));
        }
        return rows;
    }
}
