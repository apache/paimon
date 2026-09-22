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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/** Validates persisted video metadata ranges and sparse keyframe entries. */
final class VideoKeyframeIndex {

    private static final int VERSION = 1;
    private static final long MAGIC = 0x564944454F4B4649L; // "VIDEOKFI"
    private static final int HEADER_LENGTH = Byte.BYTES + Long.BYTES + Integer.BYTES * 2;
    private static final int METADATA_RANGE_LENGTH = Long.BYTES * 2;
    private static final int ENTRY_LENGTH = Long.BYTES * 3;

    private VideoKeyframeIndex() {}

    static void validate(byte[] data, long payloadLength) {
        if (data == null || data.length <= HEADER_LENGTH) {
            throw invalid("too short");
        }
        if (payloadLength <= 0) {
            throw invalid("empty video payload");
        }

        ByteBuffer header = ByteBuffer.wrap(data, 0, HEADER_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
        int version = Byte.toUnsignedInt(header.get());
        long magic = header.getLong();
        long metadataRangeCount = Integer.toUnsignedLong(header.getInt());
        long keyframeCount = Integer.toUnsignedLong(header.getInt());
        if (version != VERSION || magic != MAGIC) {
            throw invalid("version or magic");
        }
        if (keyframeCount == 0) {
            throw invalid("empty");
        }

        long entriesOffset = HEADER_LENGTH + metadataRangeCount * METADATA_RANGE_LENGTH;
        if (entriesOffset >= data.length) {
            throw invalid("metadata ranges");
        }
        ByteBuffer ranges = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        ranges.position(HEADER_LENGTH);
        long previousEnd = 0;
        for (long i = 0; i < metadataRangeCount; i++) {
            long offset = ranges.getLong();
            long length = ranges.getLong();
            if (offset < previousEnd || length <= 0 || offset > Long.MAX_VALUE - length) {
                throw invalid("metadata ranges");
            }
            previousEnd = offset + length;
            if (previousEnd > payloadLength) {
                throw invalid("metadata range outside video payload");
            }
        }

        Inflater inflater = new Inflater();
        inflater.setInput(data, (int) entriesOffset, data.length - (int) entriesOffset);
        byte[] entry = new byte[ENTRY_LENGTH];
        long previousOrdinal = -1;
        long previousPts = 0;
        long previousPacketPosition = -1;
        try {
            for (long i = 0; i < keyframeCount; i++) {
                inflateEntry(inflater, entry);
                ByteBuffer values = ByteBuffer.wrap(entry).order(ByteOrder.LITTLE_ENDIAN);
                long ordinal = values.getLong();
                long pts = values.getLong();
                long packetPosition = values.getLong();
                if ((i == 0 && ordinal != 0) || ordinal <= previousOrdinal) {
                    throw invalid("keyframe ordinals must be strictly increasing from zero");
                }
                if (i > 0 && pts <= previousPts) {
                    throw invalid("keyframe timestamps must be strictly increasing");
                }
                if (packetPosition <= previousPacketPosition || packetPosition >= payloadLength) {
                    throw invalid(
                            "keyframe packet positions must be within the video payload and strictly increasing");
                }
                previousOrdinal = ordinal;
                previousPts = pts;
                previousPacketPosition = packetPosition;
            }

            if (inflater.inflate(entry, 0, 1) != 0
                    || !inflater.finished()
                    || inflater.getRemaining() != 0) {
                throw invalid("entry count");
            }
        } catch (DataFormatException e) {
            throw new IllegalArgumentException("Invalid video keyframe index payload.", e);
        } finally {
            inflater.end();
        }
    }

    private static void inflateEntry(Inflater inflater, byte[] entry) throws DataFormatException {
        int offset = 0;
        while (offset < entry.length) {
            int length = inflater.inflate(entry, offset, entry.length - offset);
            if (length == 0) {
                throw invalid("entries");
            }
            offset += length;
        }
    }

    private static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("Invalid video keyframe index: " + reason + '.');
    }
}
