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

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
            validate(new ByteArraySeekableStream(data), data.length, payloadLength);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid video keyframe index.", e);
        }
    }

    static void validate(SeekableInputStream source, long length, long payloadLength)
            throws IOException {
        long base = source.getPos();
        if (length < 9 || payloadLength <= 0) {
            throw invalid("header or empty video payload");
        }
        DataInputStream in = new DataInputStream(source);
        int version = in.readUnsignedByte();
        long magic = Long.reverseBytes(in.readLong());
        if (magic != MAGIC) {
            throw invalid("magic");
        }
        if (version != 1) {
            throw new UnsupportedIndexException("version " + version);
        }
        if (length < 22) {
            throw invalid("header");
        }
        long metadataCount = Integer.toUnsignedLong(Integer.reverseBytes(in.readInt()));
        long count = Long.reverseBytes(in.readLong());
        int codec = in.readUnsignedByte();
        if (codec > 1) {
            throw new UnsupportedIndexException("codec " + codec);
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
        }
        if (nextOffset != length) {
            throw invalid("entries length");
        }
        byte[] decoded = new byte[PAGE_ENTRIES * ENTRY_LENGTH];
        byte[] encoded = new byte[PAGE_ENTRIES * ENTRY_LENGTH + 64];
        for (long page = 0; page < pages; page++) {
            source.seek(base + offset + page * record.length);
            in.readFully(record);
            ByteBuffer descriptor = ByteBuffer.wrap(record).order(ByteOrder.LITTLE_ENDIAN);
            long start = descriptor.getLong();
            int size = descriptor.getInt();
            long crc = Integer.toUnsignedLong(descriptor.getInt());
            long[] first = entry(descriptor);
            long[] last = entry(descriptor);
            int entries = (int) Math.min(PAGE_ENTRIES, count - page * PAGE_ENTRIES);
            int bytes = entries * ENTRY_LENGTH;
            source.seek(base + start);
            if (codec == 0) {
                in.readFully(decoded, 0, bytes);
            } else {
                in.readFully(encoded, 0, size);
                decodePage(encoded, size, decoded, entries);
            }
            if (checksum(decoded, bytes) != crc) {
                throw invalid("page checksum");
            }
            ByteBuffer values = ByteBuffer.wrap(decoded, 0, bytes).order(ByteOrder.LITTLE_ENDIAN);
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
    }

    private static void decodePage(byte[] encoded, int size, byte[] decoded, int entries) {
        int bytes = entries * ENTRY_LENGTH;
        byte[] deltas = new byte[bytes + 1];
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(encoded, 0, size);
            int total = 0;
            while (!inflater.finished() && total < deltas.length) {
                int n = inflater.inflate(deltas, total, deltas.length - total);
                if (n == 0) {
                    break;
                }
                total += n;
            }
            if (total != bytes || !inflater.finished() || inflater.getRemaining() != 0) {
                throw invalid("compressed page length");
            }
        } catch (DataFormatException e) {
            throw invalid("compressed page");
        } finally {
            inflater.end();
        }
        ByteBuffer input = ByteBuffer.wrap(deltas).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer output = ByteBuffer.wrap(decoded).order(ByteOrder.LITTLE_ENDIAN);
        for (int column = 0; column < 3; column++) {
            long value = 0;
            for (int i = 0; i < entries; i++) {
                // Java long addition supplies the format's modulo-2^64 arithmetic.
                value += input.getLong();
                output.putLong(i * ENTRY_LENGTH + column * Long.BYTES, value);
            }
        }
    }

    static final class UnsupportedIndexException extends IllegalArgumentException {
        UnsupportedIndexException(String reason) {
            super("Unsupported video keyframe index " + reason + '.');
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
