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

package org.apache.paimon.globalindex.rtree;

import org.apache.paimon.geo.BoundingBox;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * An 80-byte big-endian header is followed by fixed-size checksummed pages and two row-ID lists. V1
 * checksums each list independently.
 *
 * <p>Pages are written bottom-up; the root is the final page. Empty geometries are non-null rows
 * without leaf entries. The header is also stored as container metadata.
 */
final class RTreeIndexFile {

    static final int MAGIC = 0x50525431;
    static final int VERSION = 1;
    static final int HEADER_SIZE = 80;
    static final int MAX_PARTITION_ROWS = 1048576;

    private RTreeIndexFile() {}

    static int pageBytes(int pageSize) {
        return 12 + 40 * pageSize;
    }

    static int pageCount(int items, int pageSize) {
        int pages = 0;
        while (items > 0) {
            items = (items + pageSize - 1) / pageSize;
            pages += items;
            if (items == 1) {
                break;
            }
        }
        return pages;
    }

    private static int[] levelStarts(int items, int pageSize) {
        int[] starts = new int[Integer.SIZE]; // The row ceiling bounds the depth to 20 levels.
        int levels = 0;
        int start = 0;
        while (items > 0) {
            starts[levels++] = start;
            items = (items + pageSize - 1) / pageSize;
            start += items;
            if (items == 1) {
                break;
            }
        }
        return Arrays.copyOf(starts, levels);
    }

    static void putBox(ByteBuffer buffer, BoundingBox box) {
        buffer.putDouble(box.minX())
                .putDouble(box.minY())
                .putDouble(box.maxX())
                .putDouble(box.maxY());
    }

    static BoundingBox getBox(ByteBuffer buffer) throws IOException {
        try {
            return BoundingBox.of(
                    buffer.getDouble(), buffer.getDouble(), buffer.getDouble(), buffer.getDouble());
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid R-tree bounding box.", e);
        }
    }

    static void checksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer.wrap(bytes).putInt(bytes.length - 4, (int) crc.getValue());
    }

    static void verify(byte[] bytes) throws IOException {
        verify(bytes, 0, bytes.length);
    }

    static void verify(byte[] bytes, int offset, int length) throws IOException {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, length - 4);
        if ((int) crc.getValue() != ByteBuffer.wrap(bytes).getInt(offset + length - 4)) {
            throw new IOException("R-tree checksum mismatch.");
        }
    }

    static final class Metadata {
        final int pageSize;
        final int items;
        final int pages;
        final int[] levelStarts;
        final int nulls;
        final int nonNulls;
        final long firstRow;
        final long lastRow;
        final BoundingBox bounds;
        final byte[] bytes;

        Metadata(
                int pageSize,
                int items,
                int nulls,
                int nonNulls,
                long firstRow,
                long lastRow,
                BoundingBox bounds) {
            this.pageSize = pageSize;
            this.items = items;
            this.pages = pageCount(items, pageSize);
            this.levelStarts = levelStarts(items, pageSize);
            this.nulls = nulls;
            this.nonNulls = nonNulls;
            this.firstRow = firstRow;
            this.lastRow = lastRow;
            this.bounds = bounds;
            bytes = new byte[HEADER_SIZE];
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            buffer.putInt(MAGIC)
                    .putInt(VERSION)
                    .putInt(pageSize)
                    .putInt(items)
                    .putInt(pages)
                    .putInt(nulls)
                    .putInt(nonNulls)
                    .putLong(firstRow)
                    .putLong(lastRow);
            putBox(buffer, bounds);
            checksum(bytes);
        }

        private Metadata(byte[] bytes) throws IOException {
            if (bytes == null || bytes.length != HEADER_SIZE) {
                throw new IOException("Missing or invalid R-tree header.");
            }
            verify(bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            int magic = buffer.getInt();
            int version = buffer.getInt();
            if (magic != MAGIC || version != VERSION) {
                throw new IOException("Unsupported R-tree file format.");
            }
            pageSize = buffer.getInt();
            items = buffer.getInt();
            pages = buffer.getInt();
            nulls = buffer.getInt();
            nonNulls = buffer.getInt();
            firstRow = buffer.getLong();
            lastRow = buffer.getLong();
            if (pageSize < 2
                    || pageSize > 4096
                    || nulls < 0
                    || nonNulls < 0
                    || (long) nulls + nonNulls > MAX_PARTITION_ROWS
                    || nulls + nonNulls == 0
                    || items < 0
                    || items > nonNulls
                    || firstRow < 0
                    || lastRow < firstRow
                    || (long) nulls + nonNulls - 1 > lastRow - firstRow
                    || (nulls + nonNulls == 1 && firstRow != lastRow)
                    || pages != pageCount(items, pageSize)) {
                throw new IOException("Invalid R-tree header counts or row range.");
            }
            levelStarts = levelStarts(items, pageSize);
            if (items == 0) {
                double minX = buffer.getDouble();
                double minY = buffer.getDouble();
                double maxX = buffer.getDouble();
                double maxY = buffer.getDouble();
                if (minX != Double.POSITIVE_INFINITY
                        || minY != Double.POSITIVE_INFINITY
                        || maxX != Double.NEGATIVE_INFINITY
                        || maxY != Double.NEGATIVE_INFINITY) {
                    throw new IOException("Invalid empty R-tree bounds.");
                }
                bounds = BoundingBox.empty();
            } else {
                bounds = getBox(buffer);
            }
            this.bytes = Arrays.copyOf(bytes, bytes.length);
        }

        static Metadata read(GlobalIndexIOMeta file, long totalRowCount) {
            try {
                Metadata meta = new Metadata(file.metadata());
                if (meta.lastRow >= totalRowCount
                        || file.fileSize() != meta.fileSize()
                        || (file.rowCount() >= 0 && file.rowCount() != meta.rowCount())) {
                    throw new IOException("R-tree file size or source row count mismatch.");
                }
                return meta;
            } catch (IOException e) {
                throw new IllegalArgumentException(
                        "Invalid R-tree metadata for " + file.filePath(), e);
            }
        }

        int rowCount() {
            return nulls + nonNulls;
        }

        long rowsOffset() {
            return HEADER_SIZE + (long) pages * pageBytes(pageSize);
        }

        long fileSize() {
            return rowsOffset() + (long) rowCount() * Long.BYTES + 8;
        }
    }

    static final class Entry {
        final BoundingBox box;
        final long id;

        Entry(BoundingBox box, long id) {
            this.box = box;
            this.id = id;
        }
    }
}
