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

package org.apache.paimon.spark;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.InstantiationUtil;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.ToLongFunction;
import java.util.zip.CRC32;

import scala.collection.JavaConverters;
import scala.collection.Seq;

/** Externalizes large Spark input partitions to storage shared by all executors. */
final class SharedSplitMetadataExternalizer {

    private static final int MAGIC = 0x504D534D;
    private static final int FORMAT_VERSION = 1;
    private static final int SPLIT_MARKER = 0x53504C54;
    private static final int END_MARKER = 0x454E4421;
    private static final int CHUNK_SIZE = 8192;

    private SharedSplitMetadataExternalizer() {}

    /** Immutable metadata carried by a Spark task instead of its full Split graph. */
    public static final class Encoded {
        private final String path;
        private final long offset;
        private final long length;
        private final int formatVersion;
        private final int splitCount;
        private final long rowCount;
        private final long estimatedDataBytes;

        private Encoded(
                String path,
                long offset,
                long length,
                int formatVersion,
                int splitCount,
                long rowCount,
                long estimatedDataBytes) {
            this.path = path;
            this.offset = offset;
            this.length = length;
            this.formatVersion = formatVersion;
            this.splitCount = splitCount;
            this.rowCount = rowCount;
            this.estimatedDataBytes = estimatedDataBytes;
        }

        public boolean external() {
            return path != null;
        }

        public String path() {
            return path;
        }

        public long offset() {
            return offset;
        }

        public long length() {
            return length;
        }

        public int formatVersion() {
            return formatVersion;
        }

        public int splitCount() {
            return splitCount;
        }

        public long rowCount() {
            return rowCount;
        }

        public long estimatedDataBytes() {
            return estimatedDataBytes;
        }
    }

    /** Writes every large partition from one scan to one shared container. */
    public static final class ScanWriter implements Closeable {

        private final FileIO fileIO;
        private final Path directory;
        private final Path file;
        private final long threshold;
        private PositionOutputStream output;
        private long size;
        private long externalEntryCount;
        private long externalBytes;
        private boolean failed;
        private boolean closed;

        public ScanWriter(FileIO fileIO, Path directory, long threshold) {
            if (threshold <= 0) {
                throw new IllegalArgumentException("The inline threshold must be positive");
            }
            this.fileIO = fileIO;
            this.directory = directory;
            this.file = new Path(directory, "paimon-splits-" + UUID.randomUUID() + ".bin");
            this.threshold = threshold;
        }

        /** Compatibility adapter for callers which already own a complete Scala sequence. */
        public Encoded encode(Seq<Split> splits) throws IOException {
            return encode(
                    JavaConverters.asJavaIteratorConverter(splits.iterator()).asJava(),
                    ignored -> -1L);
        }

        /** Encodes a single-pass Split iterator without collecting it into another list. */
        public Encoded encode(
                java.util.Iterator<Split> splits, ToLongFunction<Split> dataSizeEstimator)
                throws IOException {
            if (failed) {
                throw new IllegalStateException("The shared split metadata writer has failed");
            }
            if (closed) {
                throw new IllegalStateException("The shared split metadata writer is closed");
            }

            int splitCount = 0;
            long rowCount = 0;
            long estimatedDataBytes = 0;
            boolean dataSizeKnown = true;
            try (SpillingOutput spilling = new SpillingOutput(this, threshold)) {
                DataOutputStream data = new DataOutputStream(spilling);
                data.writeInt(MAGIC);
                data.writeInt(FORMAT_VERSION);
                while (splits.hasNext()) {
                    Split split = splits.next();
                    if (split == null) {
                        throw new IOException("Split iterator returned null");
                    }
                    splitCount = Math.addExact(splitCount, 1);
                    rowCount = Math.addExact(rowCount, split.rowCount());
                    if (dataSizeKnown) {
                        long splitBytes = dataSizeEstimator.applyAsLong(split);
                        if (splitBytes < 0) {
                            dataSizeKnown = false;
                            estimatedDataBytes = -1;
                        } else {
                            estimatedDataBytes = Math.addExact(estimatedDataBytes, splitBytes);
                        }
                    }

                    data.writeInt(SPLIT_MARKER);
                    ChunkedSplitOutputStream splitOutput = new ChunkedSplitOutputStream(data);
                    try (ObjectOutputStream objects = new ObjectOutputStream(splitOutput)) {
                        objects.writeObject(split);
                    }
                }
                data.writeInt(END_MARKER);
                data.writeInt(splitCount);
                data.writeLong(rowCount);
                data.writeLong(estimatedDataBytes);
                data.flush();
                return spilling.finish(FORMAT_VERSION, splitCount, rowCount, estimatedDataBytes);
            } catch (IOException | RuntimeException e) {
                failed = true;
                throw e;
            }
        }

        public FileIO fileIO() {
            return fileIO;
        }

        public Path directory() {
            return directory;
        }

        public boolean hasExternalEntries() {
            return output != null;
        }

        public long externalEntryCount() {
            return externalEntryCount;
        }

        public long externalBytes() {
            return externalBytes;
        }

        private void append(byte[] bytes, int offset, int length) throws IOException {
            ensureOpen();
            output.write(bytes, offset, length);
            size = Math.addExact(size, length);
        }

        private void ensureOpen() throws IOException {
            if (output == null) {
                fileIO.mkdirs(directory);
                output = fileIO.newOutputStream(file, false);
            }
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                if (output != null) {
                    output.close();
                }
            }
        }
    }

    public static List<Split> decode(
            FileIO fileIO,
            String path,
            long offset,
            long length,
            int expectedFormatVersion,
            int expectedSplitCount,
            long expectedRowCount,
            long expectedDataBytes)
            throws IOException, ClassNotFoundException {
        Decoded decoded = decodeInternal(fileIO, path, offset, length);
        if (decoded.formatVersion != expectedFormatVersion
                || decoded.splits.size() != expectedSplitCount
                || decoded.rowCount != expectedRowCount
                || decoded.estimatedDataBytes != expectedDataBytes) {
            throw new IOException(
                    String.format(
                            "Split metadata descriptor mismatch for %s at %d: "
                                    + "expected version/count/rows/bytes %d/%d/%d/%d but found %d/%d/%d/%d",
                            path,
                            offset,
                            expectedFormatVersion,
                            expectedSplitCount,
                            expectedRowCount,
                            expectedDataBytes,
                            decoded.formatVersion,
                            decoded.splits.size(),
                            decoded.rowCount,
                            decoded.estimatedDataBytes));
        }
        return decoded.splits;
    }

    public static List<Split> decode(FileIO fileIO, String path, long offset, long length)
            throws IOException, ClassNotFoundException {
        return decodeInternal(fileIO, path, offset, length).splits;
    }

    private static Decoded decodeInternal(FileIO fileIO, String path, long offset, long length)
            throws IOException, ClassNotFoundException {
        validateRange(fileIO, path, offset, length);
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = SharedSplitMetadataExternalizer.class.getClassLoader();
        }
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (RangeInputStream range =
                        new RangeInputStream(
                                fileIO.newInputStream(new Path(path)), offset, length);
                DataInputStream data = new DataInputStream(range)) {
            Thread.currentThread().setContextClassLoader(loader);
            int magic = data.readInt();
            if (magic != MAGIC) {
                throw new IOException(String.format("Invalid split metadata magic: 0x%08X", magic));
            }
            int version = data.readInt();
            if (version != FORMAT_VERSION) {
                throw new IOException("Unsupported split metadata format version: " + version);
            }

            List<Split> splits = new ArrayList<>();
            while (true) {
                int marker = data.readInt();
                if (marker == END_MARKER) {
                    break;
                }
                if (marker != SPLIT_MARKER) {
                    throw new IOException(
                            String.format("Invalid split metadata frame marker: 0x%08X", marker));
                }
                ChunkedSplitInputStream splitInput = new ChunkedSplitInputStream(data, length);
                try (InstantiationUtil.ClassLoaderObjectInputStream objects =
                        new InstantiationUtil.ClassLoaderObjectInputStream(splitInput, loader)) {
                    Object value = objects.readObject();
                    if (!(value instanceof Split)) {
                        String type = value == null ? "null" : value.getClass().getName();
                        throw new IOException(
                                "Split metadata frame contained " + type + " instead of a Split");
                    }
                    splits.add((Split) value);
                }
            }

            int splitCount = data.readInt();
            long rowCount = data.readLong();
            long estimatedDataBytes = data.readLong();
            if (splitCount != splits.size()) {
                throw new IOException(
                        String.format(
                                "Split metadata count mismatch: footer=%d, decoded=%d",
                                splitCount, splits.size()));
            }
            long decodedRows = 0;
            for (Split split : splits) {
                decodedRows = Math.addExact(decodedRows, split.rowCount());
            }
            if (decodedRows != rowCount) {
                throw new IOException(
                        String.format(
                                "Split metadata row-count mismatch: footer=%d, decoded=%d",
                                rowCount, decodedRows));
            }
            if (data.read() != -1) {
                throw new IOException("Split metadata range contains trailing bytes");
            }
            return new Decoded(version, splits, rowCount, estimatedDataBytes);
        } catch (EOFException e) {
            throw new IOException("Truncated split metadata range", e);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    public static void cleanup(FileIO fileIO, Path directory) throws IOException {
        if (fileIO.exists(directory)) {
            fileIO.delete(directory, true);
        }
    }

    private static void validateRange(FileIO fileIO, String path, long offset, long length) {
        if (fileIO == null) {
            throw new IllegalArgumentException("FileIO must not be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("Path must not be null");
        }
        if (offset < 0 || length <= 0 || offset > Long.MAX_VALUE - length) {
            throw new IllegalArgumentException(
                    String.format("Invalid metadata range: offset=%d, length=%d", offset, length));
        }
    }

    private static final class Decoded {
        private final int formatVersion;
        private final List<Split> splits;
        private final long rowCount;
        private final long estimatedDataBytes;

        private Decoded(
                int formatVersion, List<Split> splits, long rowCount, long estimatedDataBytes) {
            this.formatVersion = formatVersion;
            this.splits = splits;
            this.rowCount = rowCount;
            this.estimatedDataBytes = estimatedDataBytes;
        }
    }

    /** Frames one serialized Split as bounded chunks followed by length and CRC32. */
    private static final class ChunkedSplitOutputStream extends OutputStream {

        private final DataOutputStream output;
        private final byte[] chunk = new byte[CHUNK_SIZE];
        private final CRC32 crc = new CRC32();
        private int used;
        private long length;
        private boolean closed;

        private ChunkedSplitOutputStream(DataOutputStream output) {
            this.output = output;
        }

        @Override
        public void write(int value) throws IOException {
            byte[] single = {(byte) value};
            write(single, 0, 1);
        }

        @Override
        public void write(byte[] bytes, int offset, int requested) throws IOException {
            if (closed) {
                throw new IOException("Split frame is closed");
            }
            while (requested > 0) {
                int copied = Math.min(requested, chunk.length - used);
                System.arraycopy(bytes, offset, chunk, used, copied);
                used += copied;
                offset += copied;
                requested -= copied;
                if (used == chunk.length) {
                    flushChunk();
                }
            }
        }

        private void flushChunk() throws IOException {
            if (used == 0) {
                return;
            }
            output.writeInt(used);
            output.write(chunk, 0, used);
            crc.update(chunk, 0, used);
            length = Math.addExact(length, used);
            used = 0;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                flushChunk();
                output.writeInt(0);
                output.writeLong(length);
                output.writeInt((int) crc.getValue());
            }
        }
    }

    /** Exposes one framed Split payload without closing the shared range stream. */
    private static final class ChunkedSplitInputStream extends InputStream {

        private final DataInputStream input;
        private final long rangeLength;
        private final CRC32 crc = new CRC32();
        private int remainingInChunk;
        private long length;
        private boolean finished;
        private boolean closed;

        private ChunkedSplitInputStream(DataInputStream input, long rangeLength) {
            this.input = input;
            this.rangeLength = rangeLength;
        }

        @Override
        public int read() throws IOException {
            byte[] single = new byte[1];
            int read = read(single, 0, 1);
            return read < 0 ? -1 : single[0] & 0xff;
        }

        @Override
        public int read(byte[] bytes, int offset, int requested) throws IOException {
            if (requested == 0) {
                return 0;
            }
            if (!ensureChunk()) {
                return -1;
            }
            int read = input.read(bytes, offset, Math.min(requested, remainingInChunk));
            if (read < 0) {
                throw new IOException("Truncated split metadata chunk");
            }
            remainingInChunk -= read;
            length = Math.addExact(length, read);
            crc.update(bytes, offset, read);
            return read;
        }

        private boolean ensureChunk() throws IOException {
            if (remainingInChunk > 0) {
                return true;
            }
            if (finished) {
                return false;
            }
            int chunkLength = input.readInt();
            if (chunkLength < 0 || chunkLength > rangeLength) {
                throw new IOException("Invalid split metadata chunk length: " + chunkLength);
            }
            if (chunkLength > 0) {
                remainingInChunk = chunkLength;
                return true;
            }
            long expectedLength = input.readLong();
            long expectedCrc = Integer.toUnsignedLong(input.readInt());
            if (expectedLength != length || expectedCrc != crc.getValue()) {
                throw new IOException(
                        String.format(
                                "Split metadata checksum mismatch: expected length/crc %d/%d but found %d/%d",
                                expectedLength, expectedCrc, length, crc.getValue()));
            }
            finished = true;
            return false;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                byte[] buffer = new byte[CHUNK_SIZE];
                while (read(buffer) >= 0) {
                    // Drain and validate the frame footer without closing the shared stream.
                }
                closed = true;
            }
        }
    }

    private static final class SpillingOutput extends OutputStream {

        private final ScanWriter writer;
        private final long threshold;
        private final List<byte[]> chunks = new ArrayList<>();
        private int tailUsed;
        private long size;
        private long externalOffset = -1;
        private boolean finished;

        private SpillingOutput(ScanWriter writer, long threshold) {
            this.writer = writer;
            this.threshold = threshold;
        }

        @Override
        public void write(int value) throws IOException {
            byte[] single = {(byte) value};
            write(single, 0, 1);
        }

        @Override
        public void write(byte[] bytes, int offset, int requested) throws IOException {
            if (requested == 0) {
                return;
            }
            if (externalOffset < 0 && requested > threshold - size) {
                spill();
            }
            if (externalOffset >= 0) {
                writer.append(bytes, offset, requested);
                size = Math.addExact(size, requested);
                return;
            }
            while (requested > 0) {
                if (chunks.isEmpty() || tailUsed == chunks.get(chunks.size() - 1).length) {
                    chunks.add(new byte[(int) Math.min(CHUNK_SIZE, threshold - size)]);
                    tailUsed = 0;
                }
                byte[] tail = chunks.get(chunks.size() - 1);
                int copied = Math.min(requested, tail.length - tailUsed);
                System.arraycopy(bytes, offset, tail, tailUsed, copied);
                size = Math.addExact(size, copied);
                tailUsed += copied;
                offset += copied;
                requested -= copied;
            }
        }

        private void spill() throws IOException {
            externalOffset = writer.size;
            for (int i = 0; i < chunks.size(); i++) {
                byte[] chunk = chunks.get(i);
                writer.append(chunk, 0, i == chunks.size() - 1 ? tailUsed : chunk.length);
            }
            if (chunks.isEmpty()) {
                writer.ensureOpen();
            }
            chunks.clear();
        }

        private Encoded finish(
                int formatVersion, int splitCount, long rowCount, long estimatedDataBytes) {
            finished = true;
            if (externalOffset >= 0) {
                writer.externalEntryCount++;
                writer.externalBytes = Math.addExact(writer.externalBytes, size);
                return new Encoded(
                        writer.file.toString(),
                        externalOffset,
                        size,
                        formatVersion,
                        splitCount,
                        rowCount,
                        estimatedDataBytes);
            }
            return new Encoded(
                    null, 0, size, formatVersion, splitCount, rowCount, estimatedDataBytes);
        }

        @Override
        public void close() {
            if (!finished && externalOffset >= 0) {
                writer.failed = true;
            }
        }
    }

    private static final class RangeInputStream extends InputStream {

        private final SeekableInputStream input;
        private long remaining;

        private RangeInputStream(SeekableInputStream input, long offset, long length)
                throws IOException {
            this.input = input;
            try {
                this.input.seek(offset);
            } catch (IOException seekError) {
                try {
                    this.input.close();
                } catch (IOException closeError) {
                    seekError.addSuppressed(closeError);
                }
                throw seekError;
            }
            this.remaining = length;
        }

        @Override
        public int read() throws IOException {
            byte[] single = new byte[1];
            int read = read(single, 0, 1);
            return read < 0 ? -1 : single[0] & 0xff;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            if (remaining == 0) {
                return -1;
            }
            int requested = (int) Math.min(length, remaining);
            int read = input.read(bytes, offset, requested);
            if (read < 0) {
                throw new IOException("Truncated split metadata range");
            }
            remaining -= read;
            return read;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }
}
