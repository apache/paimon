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

package org.apache.paimon.format.avro;

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for physical Avro block metadata. */
class AvroBlockReaderTest {

    private static final Schema SCHEMA = Schema.create(Schema.Type.LONG);

    @TempDir private java.nio.file.Path tempDir;

    @ParameterizedTest
    @ValueSource(strings = {"null", "deflate", "snappy", "zstandard"})
    void blockMetadataMatchesWriterBoundaries(String codec) throws Exception {
        long[][] values = {{0L, 1L, Long.MAX_VALUE}, {100L}, {1000L, 1001L}};
        long[] boundaries = new long[values.length + 1];
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (DataFileWriter<Long> writer = new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA))) {
            writer.setCodec(CodecFactory.fromString(codec));
            // Exercise headers larger than the decoder's read-ahead buffer.
            writer.setMeta("test.padding", new byte[20_000]);
            writer.create(SCHEMA, output);
            boundaries[0] = writer.sync();
            for (int i = 0; i < values.length; i++) {
                for (long value : values[i]) {
                    writer.append(value);
                }
                boundaries[i + 1] = writer.sync();
            }
        }
        byte[] bytes = output.toByteArray();
        assertThat(boundaries[values.length]).isEqualTo(bytes.length);

        for (int maxRead : new int[] {1, 7, Integer.MAX_VALUE}) {
            List<Long> seeks = new ArrayList<>();
            SeekableInputStream input =
                    new SeekableInputStreamWrapper(open(bytes)) {
                        @Override
                        public int read(byte[] data, int offset, int length) throws IOException {
                            return super.read(data, offset, Math.min(length, maxRead));
                        }

                        @Override
                        public void seek(long position) throws IOException {
                            seeks.add(position);
                            super.seek(position);
                        }
                    };
            try (AvroBlockReader reader = new AvroBlockReader(input)) {
                assertThat(seeks).isEmpty();
                long resumePosition = input.getPos();
                byte[] header = reader.headerBytes();
                assertThat(header).isEqualTo(Arrays.copyOf(bytes, (int) boundaries[0]));
                assertThat(input.getPos()).isEqualTo(resumePosition);
                assertThat(seeks).containsExactly(0L, resumePosition);
                byte[] anotherHeader = reader.headerBytes();
                anotherHeader[0] = 0;
                assertThat(reader.headerBytes()).isEqualTo(header);
                assertThat(seeks).hasSize(2);
                AvroRawBlock previous = null;
                for (int i = 0; i < values.length; i++) {
                    // Exercise next() both directly and after repeated look-ahead calls.
                    if (i > 0) {
                        assertThat(reader.hasNextBlock()).isTrue();
                        assertThat(reader.hasNextBlock()).isTrue();
                    }
                    AvroRawBlock block = reader.nextBorrowedRawBlock();
                    if (previous != null) {
                        assertThat(block).isSameAs(previous);
                    }
                    previous = block;
                    assertThat(block.recordCount()).isEqualTo(values[i].length);
                    assertThat(reader.blockOffset()).isEqualTo(boundaries[i]);
                    assertThat(reader.blockLength()).isEqualTo(boundaries[i + 1] - boundaries[i]);
                    assertBlockReadable(
                            header, bytes, reader.blockOffset(), reader.blockLength(), values[i]);
                }
                assertThat(reader.hasNextBlock()).isFalse();
                assertThat(reader.hasNextBlock()).isFalse();
                assertThatThrownBy(reader::nextBorrowedRawBlock)
                        .isInstanceOf(NoSuchElementException.class);
            }
        }
    }

    @Test
    void emptyFileContainsOnlyTheHeader() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (DataFileWriter<Long> writer = new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA))) {
            writer.create(SCHEMA, output);
        }
        byte[] bytes = output.toByteArray();
        try (AvroBlockReader reader = new AvroBlockReader(open(bytes))) {
            assertThat(reader.headerBytes()).isEqualTo(bytes);
            assertThat(reader.hasNextBlock()).isFalse();
            assertThatThrownBy(reader::nextBorrowedRawBlock)
                    .isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void failedHeaderReadRestoresThePositionAndCanBeRetried() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        long headerLength;
        long secondBlockOffset;
        try (DataFileWriter<Long> writer = new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA))) {
            writer.create(SCHEMA, output);
            headerLength = writer.sync();
            writer.append(11L);
            secondBlockOffset = writer.sync();
            writer.append(22L);
        }
        byte[] bytes = output.toByteArray();
        AtomicBoolean failRead = new AtomicBoolean();
        SeekableInputStream input =
                new SeekableInputStreamWrapper(open(bytes)) {
                    @Override
                    public int read(byte[] data, int offset, int length) throws IOException {
                        if (failRead.getAndSet(false)) {
                            throw new IOException("header read failed");
                        }
                        return super.read(data, offset, length);
                    }
                };
        try (AvroBlockReader reader = new AvroBlockReader(input)) {
            reader.nextBorrowedRawBlock();
            assertThat(reader.hasNextBlock()).isTrue();
            long resumePosition = input.getPos();
            failRead.set(true);
            assertThatThrownBy(reader::headerBytes)
                    .isInstanceOf(IOException.class)
                    .hasMessage("header read failed");
            assertThat(input.getPos()).isEqualTo(resumePosition);

            byte[] header = reader.headerBytes();
            assertThat(header).isEqualTo(Arrays.copyOf(bytes, (int) headerLength));
            assertThat(input.getPos()).isEqualTo(resumePosition);
            assertThat(reader.nextBorrowedRawBlock().recordCount()).isEqualTo(1);
            assertThat(reader.blockOffset()).isEqualTo(secondBlockOffset);
            assertBlockReadable(
                    header, bytes, reader.blockOffset(), reader.blockLength(), new long[] {22L});
            assertThat(reader.hasNextBlock()).isFalse();
        }
    }

    @Test
    void headerFromMemoryCanRestoreEof() throws IOException {
        for (int records : new int[] {0, 1}) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            long headerLength;
            try (DataFileWriter<Long> writer =
                    new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA))) {
                writer.create(SCHEMA, output);
                headerLength = writer.sync();
                if (records > 0) {
                    writer.append(17L);
                }
            }
            byte[] bytes = output.toByteArray();
            ByteArraySeekableStream input = new ByteArraySeekableStream(bytes);
            try (AvroBlockReader reader = new AvroBlockReader(input)) {
                assertThat(input.getPos()).isEqualTo(bytes.length);
                byte[] header = reader.headerBytes();
                assertThat(header).isEqualTo(Arrays.copyOf(bytes, (int) headerLength));
                assertThat(input.getPos()).isEqualTo(bytes.length);
                if (records > 0) {
                    assertThat(reader.nextBorrowedRawBlock().recordCount()).isEqualTo(records);
                    assertBlockReadable(
                            header,
                            bytes,
                            reader.blockOffset(),
                            reader.blockLength(),
                            new long[] {17L});
                }
                assertThat(reader.hasNextBlock()).isFalse();
            }
        }
    }

    @Test
    void headerStartsAtInitialStreamPosition() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        long headerLength;
        try (DataFileWriter<Long> writer = new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA))) {
            writer.create(SCHEMA, output);
            headerLength = writer.sync();
            writer.append(17L);
        }
        byte[] avro = output.toByteArray();
        int prefixLength = 13;
        byte[] bytes = new byte[prefixLength + avro.length];
        System.arraycopy(avro, 0, bytes, prefixLength, avro.length);
        SeekableInputStream input = open(bytes);
        input.seek(prefixLength);
        try (AvroBlockReader reader = new AvroBlockReader(input)) {
            long resumePosition = input.getPos();
            byte[] header = reader.headerBytes();
            assertThat(header).isEqualTo(Arrays.copyOf(avro, (int) headerLength));
            assertThat(input.getPos()).isEqualTo(resumePosition);
            assertThat(reader.nextBorrowedRawBlock().recordCount()).isEqualTo(1);
            assertThat(reader.blockOffset()).isEqualTo(prefixLength + headerLength);
            assertBlockReadable(
                    header, bytes, reader.blockOffset(), reader.blockLength(), new long[] {17L});
            assertThat(reader.hasNextBlock()).isFalse();
        }
    }

    private SeekableInputStream open(byte[] bytes) throws IOException {
        java.nio.file.Path file = Files.createTempFile(tempDir, "blocks-", ".avro");
        Files.write(file, bytes);
        return LocalFileIO.create().newInputStream(new Path(file.toUri()));
    }

    private static void assertBlockReadable(
            byte[] header, byte[] file, long offset, long length, long[] expected)
            throws IOException {
        ByteArrayOutputStream selected = new ByteArrayOutputStream();
        selected.write(header);
        selected.write(file, (int) offset, (int) length);
        try (DataFileStream<Long> reader =
                new DataFileStream<>(
                        new ByteArrayInputStream(selected.toByteArray()),
                        new GenericDatumReader<>())) {
            for (long value : expected) {
                assertThat(reader.next()).isEqualTo(value);
            }
            assertThat(reader.hasNext()).isFalse();
        }
    }
}
