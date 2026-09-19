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

/** Validates the persisted sparse video keyframe index. */
final class VideoKeyframeIndex {

    private static final int VERSION = 1;
    private static final long MAGIC = 0x564944454F4B4649L; // "VIDEOKFI"
    private static final int HEADER_LENGTH = Byte.BYTES + Long.BYTES + Integer.BYTES;
    private static final int ENTRY_LENGTH = Long.BYTES * 2;

    private VideoKeyframeIndex() {}

    static void validate(byte[] data) {
        if (data == null || data.length <= HEADER_LENGTH) {
            throw invalid("too short");
        }

        ByteBuffer header = ByteBuffer.wrap(data, 0, HEADER_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
        int version = Byte.toUnsignedInt(header.get());
        long magic = header.getLong();
        long count = Integer.toUnsignedLong(header.getInt());
        if (version != VERSION || magic != MAGIC) {
            throw invalid("version or magic");
        }
        if (count == 0) {
            throw invalid("empty");
        }

        Inflater inflater = new Inflater();
        inflater.setInput(data, HEADER_LENGTH, data.length - HEADER_LENGTH);
        byte[] entry = new byte[ENTRY_LENGTH];
        long previousOrdinal = -1;
        long previousPts = 0;
        try {
            for (long i = 0; i < count; i++) {
                inflateEntry(inflater, entry);
                ByteBuffer values = ByteBuffer.wrap(entry).order(ByteOrder.LITTLE_ENDIAN);
                long ordinal = values.getLong();
                long pts = values.getLong();
                if ((i == 0 && ordinal != 0) || ordinal <= previousOrdinal) {
                    throw invalid("keyframe ordinals must be strictly increasing from zero");
                }
                if (i > 0 && pts <= previousPts) {
                    throw invalid("keyframe timestamps must be strictly increasing");
                }
                previousOrdinal = ordinal;
                previousPts = pts;
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
