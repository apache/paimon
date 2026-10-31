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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

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
        if (length < 21 || payloadLength <= 0) {
            throw invalid("header or empty video payload");
        }
        DataInputStream in = new DataInputStream(source);
        int version = in.readUnsignedByte();
        long magic = Long.reverseBytes(in.readLong());
        long metadataCount = Integer.toUnsignedLong(Integer.reverseBytes(in.readInt()));
        if (version != 1 || magic != MAGIC) {
            throw invalid("version or magic");
        }
        long count = Long.reverseBytes(in.readLong());
        long offset = 21 + metadataCount * 16;
        if (metadataCount > MAX_METADATA_RANGE_COUNT) {
            throw invalid("metadata range count exceeds limit");
        }
        if (count <= 0 || offset >= length) {
            throw invalid("count or metadata ranges");
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
        long pages = (count - 1) / PAGE_ENTRIES + 1;
        if (count > (length - offset) / ENTRY_LENGTH
                || pages * 4 != length - offset - count * ENTRY_LENGTH) {
            throw invalid("entries length");
        }
        long ordinal = -1;
        long pts = 0;
        long position = -1;
        byte[] page = new byte[PAGE_ENTRIES * ENTRY_LENGTH];
        for (long done = 0; done < count; ) {
            int entries = (int) Math.min(PAGE_ENTRIES, count - done);
            int bytes = entries * ENTRY_LENGTH;
            in.readFully(page, 0, bytes);
            long expected = Integer.toUnsignedLong(Integer.reverseBytes(in.readInt()));
            CRC32 crc = new CRC32();
            crc.update(page, 0, bytes);
            if (crc.getValue() != expected) {
                throw invalid("page checksum");
            }
            ByteBuffer values = ByteBuffer.wrap(page, 0, bytes).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < entries; i++) {
                long nextOrdinal = values.getLong();
                long nextPts = values.getLong();
                long nextPosition = values.getLong();
                if ((ordinal == -1 && nextOrdinal != 0)
                        || nextOrdinal <= ordinal
                        || (ordinal != -1 && nextPts <= pts)
                        || nextPosition <= position
                        || nextPosition >= payloadLength) {
                    throw invalid("ordinals, timestamps or packet positions outside video payload");
                }
                ordinal = nextOrdinal;
                pts = nextPts;
                position = nextPosition;
            }
            done += entries;
        }
    }

    private static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("Invalid video keyframe index: " + reason + '.');
    }
}
