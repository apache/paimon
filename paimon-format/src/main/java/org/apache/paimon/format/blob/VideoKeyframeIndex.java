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

package org.apache.paimon.format.blob;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/** Streaming validation for paged video seek indexes. */
final class VideoKeyframeIndex {
    private static final long MAGIC = 0x564944454F4B4649L;
    private static final int ENTRY_LENGTH = 24;
    private static final int PAGE_ENTRIES = 4096;
    static final long MAX_METADATA_RANGE_COUNT = 64 * 1024;

    private VideoKeyframeIndex() {}

    static void validate(byte[] data, long payloadLength) {
        try {
            validate(new ByteArrayInputStream(data), data.length, payloadLength);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid video keyframe index.", e);
        }
    }

    static void validate(InputStream source, long length, long payloadLength) throws IOException {
        if (length < 22 || payloadLength <= 0) {
            throw invalid("header or empty video payload");
        }
        DataInputStream in = new DataInputStream(source);
        int version = in.readUnsignedByte();
        long magic = Long.reverseBytes(in.readLong());
        long metadataCount = Integer.toUnsignedLong(Integer.reverseBytes(in.readInt()));
        long count = Long.reverseBytes(in.readLong());
        int codec = in.readUnsignedByte();
        if (version != 1 || magic != MAGIC || codec > 1) {
            throw invalid("version, magic or codec");
        }
        long offset = 22 + metadataCount * 16;
        if (metadataCount > MAX_METADATA_RANGE_COUNT) {
            throw invalid("metadata range count exceeds limit");
        }
        long pages = count > 0 ? (count - 1) / PAGE_ENTRIES + 1 : 0;
        if (count <= 0 || offset >= length || pages > (length - offset) / 69) {
            throw invalid("entries length");
        }
        long previousEnd = 0;
        for (long i = 0; i < metadataCount; i++) {
            long start = Long.reverseBytes(in.readLong());
            long size = Long.reverseBytes(in.readLong());
            if (start < previousEnd || size <= 0 || start > payloadLength - size) {
                throw invalid("metadata range outside video payload");
            }
            previousEnd = start + size;
        }
        Path path = Files.createTempFile("paimon-video-directory-", ".tmp");
        try (RandomAccessFile directory = new RandomAccessFile(path.toFile(), "rw")) {
            byte[] record = new byte[68];
            long nextOffset = offset + pages * record.length;
            long[] previous = null;
            for (long page = 0; page < pages; page++) {
                in.readFully(record);
                ByteBuffer values = ByteBuffer.wrap(record).order(ByteOrder.LITTLE_ENDIAN);
                long start = values.getLong();
                long size = Integer.toUnsignedLong(values.getInt());
                values.getInt();
                long[] first = entry(values);
                long[] last = entry(values);
                if (checksum(record, 64) != Integer.toUnsignedLong(values.getInt())) {
                    throw invalid("directory checksum");
                }
                int entries = (int) Math.min(PAGE_ENTRIES, count - page * PAGE_ENTRIES);
                if (start != nextOffset
                        || size <= 0
                        || size > PAGE_ENTRIES * ENTRY_LENGTH + 64
                        || start > length - size
                        || (codec == 0 && size != entries * ENTRY_LENGTH)) {
                    throw invalid("page offsets or length");
                }
                validateEntry(first, previous, page == 0, payloadLength);
                if (entries == 1) {
                    if (!java.util.Arrays.equals(first, last)) {
                        throw invalid("page endpoints");
                    }
                } else {
                    validateEntry(last, first, false, payloadLength);
                }
                previous = last;
                nextOffset = start + size;
                directory.write(record);
            }
            if (nextOffset != length) {
                throw invalid("entries length");
            }
            directory.seek(0);
            byte[] decoded = new byte[PAGE_ENTRIES * ENTRY_LENGTH + 1];
            byte[] encoded = new byte[PAGE_ENTRIES * ENTRY_LENGTH + 64];
            for (long page = 0; page < pages; page++) {
                directory.readFully(record);
                ByteBuffer descriptor = ByteBuffer.wrap(record).order(ByteOrder.LITTLE_ENDIAN);
                descriptor.getLong();
                int size = descriptor.getInt();
                long crc = Integer.toUnsignedLong(descriptor.getInt());
                long[] first = entry(descriptor);
                long[] last = entry(descriptor);
                int entries = (int) Math.min(PAGE_ENTRIES, count - page * PAGE_ENTRIES);
                int bytes = entries * ENTRY_LENGTH;
                in.readFully(encoded, 0, size);
                if (codec == 0) {
                    System.arraycopy(encoded, 0, decoded, 0, size);
                } else {
                    Inflater inflater = new Inflater();
                    try {
                        inflater.setInput(encoded, 0, size);
                        int written = 0;
                        while (!inflater.finished() && written <= bytes) {
                            int n = inflater.inflate(decoded, written, bytes + 1 - written);
                            if (n == 0) {
                                break;
                            }
                            written += n;
                        }
                        if (written != bytes
                                || !inflater.finished()
                                || inflater.getRemaining() != 0) {
                            throw invalid("compressed page length");
                        }
                    } catch (DataFormatException e) {
                        throw invalid("compressed page");
                    } finally {
                        inflater.end();
                    }
                }
                if (checksum(decoded, bytes) != crc) {
                    throw invalid("page checksum");
                }
                ByteBuffer values =
                        ByteBuffer.wrap(decoded, 0, bytes).order(ByteOrder.LITTLE_ENDIAN);
                previous = null;
                for (int i = 0; i < entries; i++) {
                    long[] value = entry(values);
                    validateEntry(value, previous, page == 0 && i == 0, payloadLength);
                    if ((i == 0 && !java.util.Arrays.equals(value, first))
                            || (i == entries - 1 && !java.util.Arrays.equals(value, last))) {
                        throw invalid("page endpoints");
                    }
                    previous = value;
                }
            }
        } finally {
            Files.deleteIfExists(path);
        }
    }

    private static long[] entry(ByteBuffer values) {
        return new long[] {values.getLong(), values.getLong(), values.getLong()};
    }

    private static long checksum(byte[] bytes, int length) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, length);
        return crc.getValue();
    }

    private static void validateEntry(
            long[] value, long[] previous, boolean initial, long payloadLength) {
        if (value[0] < 0
                || (initial && value[0] != 0)
                || value[2] < 0
                || value[2] >= payloadLength) {
            throw invalid("ordinals or packet positions outside video payload");
        }
        if (previous != null) {
            for (int i = 0; i < 3; i++) {
                if (value[i] <= previous[i]) {
                    throw invalid(
                            "ordinals, timestamps and packet positions must be strictly increasing");
                }
            }
        }
    }

    private static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("Invalid video keyframe index: " + reason + '.');
    }
}
