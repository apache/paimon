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

package org.apache.paimon.fileindex.bitmap;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Helper class for bitmap readers that provides common functionality for reading and querying
 * bitmap indexes using composition pattern.
 *
 * @param <BITMAP> The bitmap type (e.g., RoaringBitmap32 or RoaringBitmap64)
 */
public class BitmapReaderHelper<BITMAP> {

    protected final SeekableInputStream seekableInputStream;
    protected final int headStart;
    protected final Map<Object, BITMAP> bitmaps = new LinkedHashMap<>();
    protected final Options options;
    protected final BitmapOperations<BITMAP> operations;

    protected BitmapFileIndexMeta bitmapFileIndexMeta;
    protected Function<Object, Object> valueMapper;

    public BitmapReaderHelper(
            SeekableInputStream seekableInputStream,
            int headStart,
            Options options,
            BitmapOperations<BITMAP> operations) {
        this.seekableInputStream = seekableInputStream;
        this.headStart = headStart;
        this.options = options;
        this.operations = operations;
    }

    /**
     * Gets the result bitmap for a list of literals by ORing their individual bitmaps.
     *
     * @param literals the list of literal values
     * @return the combined bitmap
     */
    public BITMAP getInListResultBitmap(List<Object> literals) {
        return operations.orBitmaps(
                literals.stream()
                        .map(it -> bitmaps.computeIfAbsent(valueMapper.apply(it), this::readBitmap))
                        .iterator());
    }

    /**
     * Reads a bitmap for the given bitmap ID from the input stream.
     *
     * @param bitmapId the ID of the bitmap to read
     * @return the bitmap
     */
    public BITMAP readBitmap(Object bitmapId) {
        try {
            BitmapFileIndexMeta.Entry entry = bitmapFileIndexMeta.findEntry(bitmapId);
            if (entry == null) {
                return operations.createEmptyBitmap();
            } else {
                int offset = entry.offset;
                if (offset < 0) {
                    return operations.createSingletonBitmap(-1 - offset);
                } else {
                    seekableInputStream.seek(bitmapFileIndexMeta.getBodyStart() + offset);
                    BITMAP bitmap = operations.createEmptyBitmap();
                    int length = entry.length;
                    if (length != -1) {
                        DataInputStream input = new DataInputStream(seekableInputStream);
                        byte[] bytes = new byte[length];
                        input.readFully(bytes);
                        operations.deserializeBitmap(bitmap, bytes);
                        return bitmap;
                    }
                    operations.deserializeBitmap(bitmap, new DataInputStream(seekableInputStream));
                    return bitmap;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads the internal metadata (bitmap file index meta) from the input stream.
     *
     * @param dataType the data type of the indexed field
     */
    public void readInternalMeta(DataType dataType, MetaCreator metaCreator) {
        if (this.bitmapFileIndexMeta == null) {
            this.valueMapper = BitmapIndexUtils.getValueMapper(dataType);
            try {
                seekableInputStream.seek(headStart);
                int version = seekableInputStream.read();
                this.bitmapFileIndexMeta = metaCreator.create(version, dataType, options);
                this.bitmapFileIndexMeta.deserialize(seekableInputStream);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public BitmapFileIndexMeta getBitmapFileIndexMeta() {
        return bitmapFileIndexMeta;
    }

    /** Closes the underlying input stream. */
    public void close() throws IOException {
        this.seekableInputStream.close();
    }

    /** Interface for bitmap-specific operations. */
    public interface BitmapOperations<BITMAP> {
        BITMAP createEmptyBitmap();

        BITMAP createSingletonBitmap(int value);

        BITMAP orBitmaps(java.util.Iterator<BITMAP> bitmaps);

        void deserializeBitmap(BITMAP bitmap, byte[] bytes) throws IOException;

        void deserializeBitmap(BITMAP bitmap, DataInputStream input) throws IOException;
    }

    /** Interface for creating BitmapFileIndexMeta based on version. */
    public interface MetaCreator {
        BitmapFileIndexMeta create(int version, DataType dataType, Options options);
    }

    /** Operations for RoaringBitmap32. */
    public static class Bitmap32Operations
            implements BitmapOperations<org.apache.paimon.utils.RoaringBitmap32> {

        @Override
        public org.apache.paimon.utils.RoaringBitmap32 createEmptyBitmap() {
            return new org.apache.paimon.utils.RoaringBitmap32();
        }

        @Override
        public org.apache.paimon.utils.RoaringBitmap32 createSingletonBitmap(int value) {
            return org.apache.paimon.utils.RoaringBitmap32.bitmapOf(value);
        }

        @Override
        public org.apache.paimon.utils.RoaringBitmap32 orBitmaps(
                java.util.Iterator<org.apache.paimon.utils.RoaringBitmap32> bitmaps) {
            return org.apache.paimon.utils.RoaringBitmap32.or(bitmaps);
        }

        @Override
        public void deserializeBitmap(org.apache.paimon.utils.RoaringBitmap32 bitmap, byte[] bytes)
                throws IOException {
            bitmap.deserialize(java.nio.ByteBuffer.wrap(bytes));
        }

        @Override
        public void deserializeBitmap(
                org.apache.paimon.utils.RoaringBitmap32 bitmap, DataInputStream input)
                throws IOException {
            bitmap.deserialize(input);
        }
    }

    /** Operations for RoaringBitmap64. */
    public static class Bitmap64Operations
            implements BitmapOperations<org.apache.paimon.utils.RoaringBitmap64> {

        @Override
        public org.apache.paimon.utils.RoaringBitmap64 createEmptyBitmap() {
            return new org.apache.paimon.utils.RoaringBitmap64();
        }

        @Override
        public org.apache.paimon.utils.RoaringBitmap64 createSingletonBitmap(int value) {
            return org.apache.paimon.utils.RoaringBitmap64.bitmapOf(value);
        }

        @Override
        public org.apache.paimon.utils.RoaringBitmap64 orBitmaps(
                java.util.Iterator<org.apache.paimon.utils.RoaringBitmap64> bitmaps) {
            return org.apache.paimon.utils.RoaringBitmap64.or(bitmaps);
        }

        @Override
        public void deserializeBitmap(org.apache.paimon.utils.RoaringBitmap64 bitmap, byte[] bytes)
                throws IOException {
            bitmap.deserialize(bytes);
        }

        @Override
        public void deserializeBitmap(
                org.apache.paimon.utils.RoaringBitmap64 bitmap, DataInputStream input)
                throws IOException {
            bitmap.deserialize(input);
        }
    }
}
