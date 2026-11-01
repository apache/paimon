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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipError;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link CoreOptions#SCAN_IGNORE_CORRUPT_FILE} for Format Table files that only fail in the
 * middle of reading, such as truncated compressed text files, or already while the reader is
 * created.
 */
class FormatTableIgnoreCorruptFilesTest {

    private static final int TRUNCATED_FILE_ROWS = 20_000;
    private static final int INTACT_FILE_FIRST_ID = 1_000_000;
    private static final int INTACT_FILE_ROWS = 3;
    private static final String ZIP_ERROR_AT_OPEN = "zip-error-at-open";
    private static final String ZIP_ERROR_WHILE_READING = "zip-error-while-reading";
    private static final int ZIP_ERROR_AFTER_BYTES = 64 * 1024;

    @TempDir Path tempDir;

    @ParameterizedTest(name = "format = {0}")
    @ValueSource(strings = {"csv", "json"})
    void testTruncatedCompressedFile(String format) throws Exception {
        writeGzip(tempDir.resolve("truncated." + format + ".gz"), format, 0, TRUNCATED_FILE_ROWS);
        truncateToHalf(tempDir.resolve("truncated." + format + ".gz"));
        writeGzip(
                tempDir.resolve("intact." + format + ".gz"),
                format,
                INTACT_FILE_FIRST_ID,
                INTACT_FILE_ROWS);

        FormatTable table = createTable(format);

        // The truncated file opens fine and only fails once decompression reaches the cut.
        assertThatThrownBy(() -> readIds(table)).isInstanceOf(EOFException.class);

        List<Integer> ids =
                readIds(
                        table.copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_IGNORE_CORRUPT_FILE.key(), "true")));

        List<Integer> truncatedIds = new ArrayList<>();
        List<Integer> intactIds = new ArrayList<>();
        for (int id : ids) {
            (id >= INTACT_FILE_FIRST_ID ? intactIds : truncatedIds).add(id);
        }
        // The other file of the split is still read completely.
        assertThat(intactIds)
                .containsExactly(
                        INTACT_FILE_FIRST_ID, INTACT_FILE_FIRST_ID + 1, INTACT_FILE_FIRST_ID + 2);
        // The rows before the cut are kept, the rest of the corrupt file is skipped.
        assertThat(truncatedIds).isNotEmpty().hasSizeLessThan(TRUNCATED_FILE_ROWS);
        for (int i = 0; i < truncatedIds.size(); i++) {
            assertThat(truncatedIds.get(i)).isEqualTo(i);
        }
    }

    @ParameterizedTest(name = "format = {0}")
    @ValueSource(strings = {"csv", "json"})
    void testCompressedFileCorruptAtOpen(String format) throws Exception {
        // Text readers fill their first buffer while being constructed, so these files already
        // fail while the reader is created: an empty gzip file and one cut inside its header.
        Files.write(tempDir.resolve("empty." + format + ".gz"), new byte[0]);
        writeGzip(tempDir.resolve("header." + format + ".gz"), format, 0, 1_000);
        byte[] bytes = Files.readAllBytes(tempDir.resolve("header." + format + ".gz"));
        byte[] header = new byte[5];
        System.arraycopy(bytes, 0, header, 0, header.length);
        Files.write(tempDir.resolve("header." + format + ".gz"), header);
        writeGzip(
                tempDir.resolve("intact." + format + ".gz"),
                format,
                INTACT_FILE_FIRST_ID,
                INTACT_FILE_ROWS);

        FormatTable table = createTable(format);

        assertThatThrownBy(() -> readIds(table))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to read file");

        List<Integer> ids =
                readIds(
                        table.copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_IGNORE_CORRUPT_FILE.key(), "true")));
        assertThat(ids)
                .containsExactly(
                        INTACT_FILE_FIRST_ID, INTACT_FILE_FIRST_ID + 1, INTACT_FILE_FIRST_ID + 2);
    }

    @ParameterizedTest(name = "format = {0}")
    @ValueSource(strings = {"csv", "json"})
    void testZipErrorWhileReading(String format) throws Exception {
        // ZipError is an InternalError, not an Exception, but the option treats it as corruption.
        writeGzip(
                tempDir.resolve(ZIP_ERROR_WHILE_READING + "." + format + ".gz"), format, 0, 20_000);
        writeGzip(
                tempDir.resolve("intact." + format + ".gz"),
                format,
                INTACT_FILE_FIRST_ID,
                INTACT_FILE_ROWS);

        FormatTable table = createTable(format, new ZipErrorFileIO());

        assertThatThrownBy(() -> readIds(table)).isInstanceOf(ZipError.class);

        List<Integer> ids =
                readIds(
                        table.copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_IGNORE_CORRUPT_FILE.key(), "true")));

        List<Integer> brokenIds = new ArrayList<>();
        List<Integer> intactIds = new ArrayList<>();
        for (int id : ids) {
            (id >= INTACT_FILE_FIRST_ID ? intactIds : brokenIds).add(id);
        }
        assertThat(intactIds)
                .containsExactly(
                        INTACT_FILE_FIRST_ID, INTACT_FILE_FIRST_ID + 1, INTACT_FILE_FIRST_ID + 2);
        assertThat(brokenIds).isNotEmpty().hasSizeLessThan(20_000);
        for (int i = 0; i < brokenIds.size(); i++) {
            assertThat(brokenIds.get(i)).isEqualTo(i);
        }
    }

    @ParameterizedTest(name = "format = {0}")
    @ValueSource(strings = {"csv", "json"})
    void testZipErrorAtOpen(String format) throws Exception {
        writeGzip(tempDir.resolve(ZIP_ERROR_AT_OPEN + "." + format + ".gz"), format, 0, 1_000);
        writeGzip(
                tempDir.resolve("intact." + format + ".gz"),
                format,
                INTACT_FILE_FIRST_ID,
                INTACT_FILE_ROWS);

        FormatTable table = createTable(format, new ZipErrorFileIO());

        assertThatThrownBy(() -> readIds(table)).isInstanceOf(ZipError.class);

        List<Integer> ids =
                readIds(
                        table.copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_IGNORE_CORRUPT_FILE.key(), "true")));
        assertThat(ids)
                .containsExactly(
                        INTACT_FILE_FIRST_ID, INTACT_FILE_FIRST_ID + 1, INTACT_FILE_FIRST_ID + 2);
    }

    @Test
    void testLostFileIsNotTreatedAsCorrupt() throws Exception {
        writeGzip(tempDir.resolve("lost.csv.gz"), "csv", 0, INTACT_FILE_ROWS);
        FormatTable table =
                createTable("csv")
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_IGNORE_CORRUPT_FILE.key(), "true"));
        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        // Plans list files lazily: materialize the splits before the file disappears.
        assertThat(plan.splits()).isNotEmpty();
        Files.delete(tempDir.resolve("lost.csv.gz"));

        assertThatThrownBy(
                        () -> {
                            try (RecordReader<InternalRow> reader =
                                    table.newReadBuilder().newRead().createReader(plan)) {
                                reader.forEachRemaining(row -> {});
                            }
                        })
                .isInstanceOf(FileNotFoundException.class);
    }

    private FormatTable createTable(String format) {
        return createTable(format, LocalFileIO.create());
    }

    private FormatTable createTable(String format, FileIO fileIO) {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.FILE_FORMAT.key(), format);
        return FormatTable.builder()
                .fileIO(fileIO)
                .identifier(Identifier.create("test_db", "corrupt_" + format))
                .rowType(
                        RowType.builder()
                                .field("id", DataTypes.INT())
                                .field("name", DataTypes.STRING())
                                .build())
                .partitionKeys(Collections.emptyList())
                .location(tempDir.toString())
                .format(FormatTable.Format.valueOf(format.toUpperCase()))
                .options(options)
                .build();
    }

    private static List<Integer> readIds(FormatTable table) throws IOException {
        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                table.newReadBuilder().newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        return ids;
    }

    private static void writeGzip(Path file, String format, int firstId, int rows)
            throws IOException {
        // Random names keep the compression ratio low, so half of the file still holds many rows.
        Random random = new Random(firstId);
        try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(file))) {
            for (int id = firstId; id < firstId + rows; id++) {
                String name = Long.toHexString(random.nextLong());
                String line =
                        "csv".equals(format)
                                ? id + "," + name
                                : "{\"id\":" + id + ",\"name\":\"" + name + "\"}";
                out.write((line + "\n").getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static void truncateToHalf(Path file) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        byte[] half = new byte[bytes.length / 2];
        System.arraycopy(bytes, 0, half, 0, half.length);
        Files.write(file, half);
    }

    /**
     * Throws {@link ZipError} for files named after the failure to simulate: either as soon as the
     * file is opened, or once the first {@link #ZIP_ERROR_AFTER_BYTES} bytes have been read.
     */
    private static class ZipErrorFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        @Override
        public SeekableInputStream newInputStream(org.apache.paimon.fs.Path path)
                throws IOException {
            String name = path.getName();
            if (name.startsWith(ZIP_ERROR_AT_OPEN)) {
                throw new ZipError("simulated zip error at open: " + name);
            }
            SeekableInputStream in = super.newInputStream(path);
            if (!name.startsWith(ZIP_ERROR_WHILE_READING)) {
                return in;
            }
            return new SeekableInputStreamWrapper(in) {
                private long bytesRead;

                @Override
                public int read() throws IOException {
                    checkLimit();
                    int b = super.read();
                    if (b >= 0) {
                        bytesRead++;
                    }
                    return b;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    checkLimit();
                    int n = super.read(b, off, len);
                    if (n > 0) {
                        bytesRead += n;
                    }
                    return n;
                }

                private void checkLimit() {
                    if (bytesRead >= ZIP_ERROR_AFTER_BYTES) {
                        throw new ZipError("simulated zip error while reading: " + name);
                    }
                }
            };
        }
    }
}
