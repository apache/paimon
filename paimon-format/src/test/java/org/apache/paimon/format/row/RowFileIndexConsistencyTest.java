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

package org.apache.paimon.format.row;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The footer and the block index describe the same blocks twice, and until they were cross-checked
 * only the index was consulted: {@code blockCount} was written into every row file and never read,
 * so a Java reader bounded its block loop by the index while the Python reader bounded it by the
 * footer. These tests pin the agreement the spec implies — the compressed sizes sum to {@code
 * indexOffset}, and the block count matches.
 */
class RowFileIndexConsistencyTest {

    private static final RowType ROW_TYPE = RowType.of(DataTypes.INT());

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testBlockIndexArraysMustAgreeOnTheBlockCount() {
        assertThatThrownBy(
                        () ->
                                new RowBlockIndex(
                                        new long[] {10, 20}, new long[] {100, 200}, new long[] {0}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2 compressed sizes")
                .hasMessageContaining("1 row starts");
    }

    @Test
    void testFooterBlockCountMustMatchTheIndex() throws Exception {
        Path path = writeRowFile("block-count.row");
        // footer blockCount is a little-endian int at footer offset 8
        patch(path, footerOffset(path) + 8, intLE(99));

        assertThatThrownBy(() -> openReader(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("the footer declares 99");
    }

    @Test
    void testCompressedSizesMustSumToTheIndexOffset() {
        // two blocks of 10 and 20 compressed bytes occupy [0, 30), so the index starts at 30
        RowBlockIndex index =
                new RowBlockIndex(new long[] {10, 20}, new long[] {100, 200}, new long[] {0, 5});
        assertThatCode(() -> index.validate(new RowFileFooter(9, 2, 30, 7)))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> index.validate(new RowFileFooter(9, 2, 31, 7)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("blocks end at 30")
                .hasMessageContaining("block index at 31");
    }

    @Test
    void testRowStartsMustCoverEveryRowExactlyOnce() {
        // RowFormatReader turns consecutive starts into a block's row range and skips a block whose
        // range the selection does not intersect, so each of these drops rows without an error
        assertThatThrownBy(() -> validateRowStarts(new long[] {10, 20}, 30))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("block 0 starts at row 10");
        assertThatThrownBy(() -> validateRowStarts(new long[] {0, 0}, 30))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not after block 0 at row 0");
        assertThatThrownBy(() -> validateRowStarts(new long[] {0, 5}, 5))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("the declared row count 5 does not reach");
        assertThatThrownBy(() -> validateRowStarts(new long[] {0}, 0))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("the declared row count 0 does not reach");
    }

    @Test
    void testAnEmptyIndexNeedsAnEmptyFile() {
        RowBlockIndex empty = new RowBlockIndex(new long[0], new long[0], new long[0]);
        assertThatCode(() -> empty.validate(new RowFileFooter(0, 0, 0, 7)))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> empty.validate(new RowFileFooter(7, 0, 0, 7)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("empty, but the footer declares 7 rows");
    }

    @Test
    void testNegativeCompressedSizeIsRejected() {
        // the sizes sum to the declared indexOffset only because the second cancels the first
        RowBlockIndex index =
                new RowBlockIndex(new long[] {200, -100}, new long[] {100, 200}, new long[] {0, 5});
        assertThatThrownBy(() -> index.validate(new RowFileFooter(9, 2, 100, 7)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("block 1 has a negative compressed size -100");
    }

    private static void validateRowStarts(long[] rowStarts, long totalRowCount) throws IOException {
        long[] sizes = new long[rowStarts.length];
        Arrays.fill(sizes, 10);
        new RowBlockIndex(sizes, sizes.clone(), rowStarts)
                .validate(
                        new RowFileFooter(totalRowCount, rowStarts.length, 10L * sizes.length, 7));
    }

    @Test
    void testIndexOutsideTheFileIsRejected() throws Exception {
        Path path = writeRowFile("index-outside.row");
        patch(path, footerOffset(path) + 12, longLE(1L << 40));

        assertThatThrownBy(() -> openReader(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Invalid row file block index location");
    }

    @Test
    void testFileTooShortForAFooterIsRejected() throws Exception {
        Path path = new Path(new Path(tempDir.toString()), "short.row");
        try (PositionOutputStream out = new LocalFileIO().newOutputStream(path, false)) {
            out.write(new byte[RowFileFooter.FOOTER_SIZE - 1]);
        }

        assertThatThrownBy(() -> openReader(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("too few for a 32-byte footer");
    }

    private Path writeRowFile(String name) throws IOException {
        Path path = new Path(new Path(tempDir.toString()), name);
        LocalFileIO fileIO = new LocalFileIO();
        FileFormat format = FileFormat.fromIdentifier("row", new Options());
        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            FormatWriter writer = format.createWriterFactory(ROW_TYPE).create(out, "zstd");
            for (int i = 0; i < 1000; i++) {
                writer.addElement(GenericRow.of(i));
            }
            writer.close();
        }
        assertThat(fileIO.getFileSize(path)).isGreaterThan(RowFileFooter.FOOTER_SIZE);
        return path;
    }

    private void openReader(Path path) throws IOException {
        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                FileFormat.fromIdentifier("row", new Options())
                        .createReaderFactory(ROW_TYPE, ROW_TYPE, new ArrayList<>());
        readerFactory.createReader(
                new FormatReaderContext(fileIO, path, fileIO.getFileSize(path), null, null));
    }

    private long footerOffset(Path path) throws IOException {
        return new LocalFileIO().getFileSize(path) - RowFileFooter.FOOTER_SIZE;
    }

    private void patch(Path path, long offset, byte[] bytes) throws IOException {
        java.nio.file.Path file = java.nio.file.Paths.get(path.toUri().getPath());
        byte[] all = Files.readAllBytes(file);
        System.arraycopy(bytes, 0, all, (int) offset, bytes.length);
        Files.write(file, all);
    }

    private static byte[] intLE(int value) {
        byte[] buf = new byte[4];
        RowFileFooter.writeIntLE(buf, 0, value);
        return buf;
    }

    private static byte[] longLE(long value) {
        byte[] buf = new byte[8];
        RowFileFooter.writeLongLE(buf, 0, value);
        return buf;
    }
}
