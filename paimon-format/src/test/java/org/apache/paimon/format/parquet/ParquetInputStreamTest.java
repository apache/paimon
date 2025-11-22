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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ParquetFileRange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ParquetInputStream}. */
public class ParquetInputStreamTest {

    @TempDir File tempDir;

    @Test
    public void testReadVectoredWithVectoredReadable() throws Exception {
        // Create a Parquet file with enough data to trigger vectored reads
        Path parquetFile = createLargeParquetFile();
        LocalFileIO fileIO = new LocalFileIO();

        // Open the file with LocalFileIO which supports VectoredReadable
        try (SeekableInputStream seekableStream = fileIO.newInputStream(parquetFile)) {

            // Verify the underlying stream supports VectoredReadable
            assertThat(seekableStream).isInstanceOf(VectoredReadable.class);

            ParquetInputStream parquetInputStream = new ParquetInputStream(seekableStream);

            // Verify readVectoredAvailable returns true
            ByteBufferAllocator allocator = new HeapByteBufferAllocator();
            assertThat(parquetInputStream.readVectoredAvailable(allocator)).isTrue();

            // Test readVectored with multiple ranges
            List<ParquetFileRange> ranges = new ArrayList<>();
            ranges.add(new ParquetFileRange(0, 100));
            ranges.add(new ParquetFileRange(200, 100));
            ranges.add(new ParquetFileRange(500, 100));

            parquetInputStream.readVectored(ranges, allocator);

            // Verify all ranges have data
            for (ParquetFileRange range : ranges) {
                ByteBuffer buffer = range.getDataReadFuture().get();
                assertThat(buffer).isNotNull();
                assertThat(buffer.remaining()).isEqualTo(range.getLength());
            }
        }
    }

    @Test
    public void testReadVectoredWithoutVectoredReadable() throws Exception {
        // Create a Parquet file
        Path parquetFile = createLargeParquetFile();
        LocalFileIO fileIO = new LocalFileIO();

        // Open the file and wrap it in a non-VectoredReadable stream
        try (SeekableInputStream seekableStream = fileIO.newInputStream(parquetFile)) {

            // Create a wrapper that doesn't implement VectoredReadable
            NonVectoredSeekableInputStream nonVectoredStream =
                    new NonVectoredSeekableInputStream(seekableStream);

            ParquetInputStream parquetInputStream = new ParquetInputStream(nonVectoredStream);

            // Verify readVectoredAvailable returns false
            ByteBufferAllocator allocator = new HeapByteBufferAllocator();
            assertThat(parquetInputStream.readVectoredAvailable(allocator)).isFalse();

            // Test readVectored falls back to serial reads
            List<ParquetFileRange> ranges = new ArrayList<>();
            ranges.add(new ParquetFileRange(0, 100));
            ranges.add(new ParquetFileRange(200, 100));

            parquetInputStream.readVectored(ranges, allocator);

            // Verify all ranges have data (using fallback path)
            for (ParquetFileRange range : ranges) {
                ByteBuffer buffer = range.getDataReadFuture().get();
                assertThat(buffer).isNotNull();
                assertThat(buffer.remaining()).isEqualTo(range.getLength());
            }
        }
    }

    @Test
    public void testReadVectoredWithEmptyRanges() throws Exception {
        // Create a Parquet file
        Path parquetFile = createLargeParquetFile();
        LocalFileIO fileIO = new LocalFileIO();

        try (SeekableInputStream seekableStream = fileIO.newInputStream(parquetFile)) {

            ParquetInputStream parquetInputStream = new ParquetInputStream(seekableStream);

            // Test with empty ranges list - should not throw
            List<ParquetFileRange> ranges = new ArrayList<>();
            ByteBufferAllocator allocator = new HeapByteBufferAllocator();

            parquetInputStream.readVectored(ranges, allocator);
            // No exception means test passed
        }
    }

    @Test
    public void testReadVectoredEndToEnd() throws Exception {
        // Create a Parquet file with multiple row groups to ensure vectored read is used
        Path parquetFile = createLargeParquetFile();
        LocalFileIO fileIO = new LocalFileIO();

        // Use ParquetFileReader to read the file, which will internally use readVectored
        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(
                        fileIO, parquetFile, fileIO.getFileSize(parquetFile))) {

            // Reading metadata and blocks will internally trigger vectored reads
            assertThat(reader.getFooter()).isNotNull();
            assertThat(reader.getRecordCount()).isGreaterThan(0);

            // Read all row groups to ensure vectored read path is exercised
            while (reader.readNextRowGroup() != null) {
                // Just consume the data
            }
        }
    }

    private Path createLargeParquetFile() throws IOException {
        RowType rowType =
                RowType.builder()
                        .fields(
                                DataTypes.INT(),
                                DataTypes.STRING(),
                                DataTypes.BIGINT(),
                                DataTypes.DOUBLE())
                        .build();

        Path path = new Path(tempDir.getPath(), UUID.randomUUID().toString() + ".parquet");

        Options conf = new Options();
        // Use a small block size to create multiple row groups
        conf.setInteger("parquet.block.size", 1024);
        conf.setInteger("parquet.page.size", 512);

        ParquetWriterFactory factory =
                new ParquetWriterFactory(new RowDataParquetBuilder(rowType, conf));
        FormatWriter writer =
                factory.create(new LocalFileIO().newOutputStream(path, false), "snappy");

        // Write enough data to create multiple pages and trigger vectored reads
        for (int i = 0; i < 10000; i++) {
            writer.addElement(
                    GenericRow.of(
                            i,
                            BinaryString.fromString("string_" + i),
                            (long) i * 100,
                            (double) i * 1.5));
        }

        writer.close();
        return path;
    }

    /**
     * A wrapper stream that does not implement VectoredReadable, used for testing the fallback
     * path.
     */
    private static class NonVectoredSeekableInputStream extends SeekableInputStream {

        private final SeekableInputStream delegate;

        public NonVectoredSeekableInputStream(SeekableInputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void seek(long pos) throws IOException {
            delegate.seek(pos);
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return delegate.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
