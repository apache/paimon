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

import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Helper class for bitmap writers that provides common functionality for building and serializing
 * bitmap indexes using composition pattern.
 *
 * @param <BITMAP> The bitmap type (e.g., RoaringBitmap32 or RoaringBitmap64)
 */
public class BitmapWriterHelper<BITMAP> {

    protected final int version;
    protected final DataType dataType;
    protected final Function<Object, Object> valueMapper;
    protected final Map<Object, BITMAP> id2bitmap = new HashMap<>();
    protected final Options options;
    protected final BitmapOperations<BITMAP> operations;
    protected final BITMAP nullBitmap;

    public BitmapWriterHelper(
            int version, DataType dataType, Options options, BitmapOperations<BITMAP> operations) {
        this.version = version;
        this.dataType = dataType;
        this.valueMapper = BitmapIndexUtils.getValueMapper(dataType);
        this.options = options;
        this.operations = operations;
        this.nullBitmap = operations.createEmptyBitmap();
    }

    /**
     * Adds a value to the bitmap at the specified index. Handles null keys by adding them to the
     * null bitmap.
     *
     * @param key the key to map (can be null)
     * @param index the bitmap index to set
     */
    public void add(Object key, long index) {
        if (key == null) {
            operations.addToBitmap(nullBitmap, index);
        } else {
            Object mappedKey = valueMapper.apply(key);
            id2bitmap.computeIfAbsent(mappedKey, k -> operations.createEmptyBitmap());
            operations.addToBitmap(id2bitmap.get(mappedKey), index);
        }
    }

    /**
     * Serializes the bitmaps and metadata to the output stream. Handles null bitmap serialization
     * internally.
     *
     * @param dos the output stream
     * @param rowCount the total row count
     * @throws Exception if serialization fails
     */
    public void serialize(DataOutputStream dos, int rowCount) throws Exception {

        dos.writeByte(version);

        // 1. Prepare null bitmap data
        byte[] nullBitmapBytes = operations.serializeBitmap(nullBitmap);
        boolean hasNull = !operations.isEmpty(nullBitmap);
        int nullBitmapLength =
                operations.isEmpty(nullBitmap) || operations.getCardinality(nullBitmap) == 1
                        ? 0
                        : nullBitmapBytes.length;
        int nullOffset =
                operations.getCardinality(nullBitmap) == 1
                        ? (int) (-1 - operations.getFirstValue(nullBitmap))
                        : 0;

        // 2. Serialize bitmaps to bytes
        Map<Object, byte[]> id2bitmapBytes =
                id2bitmap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> operations.serializeBitmap(e.getValue())));

        // 3. Build bitmap file index meta
        LinkedHashMap<Object, Integer> bitmapOffsets = new LinkedHashMap<>();
        LinkedList<byte[]> serializeBitmaps = new LinkedList<>();
        int[] offsetRef = {nullBitmapLength};

        id2bitmap.forEach(
                (k, v) -> {
                    if (operations.getCardinality(v) == 1) {
                        bitmapOffsets.put(k, (int) (-1 - operations.getFirstValue(v)));
                    } else {
                        byte[] bytes = id2bitmapBytes.get(k);
                        serializeBitmaps.add(bytes);
                        bitmapOffsets.put(k, offsetRef[0]);
                        offsetRef[0] += bytes.length;
                    }
                });

        // 4. Create and serialize meta based on version
        BitmapFileIndexMeta bitmapFileIndexMeta;
        if (version == 1) {
            // VERSION_1: Use BitmapFileIndexMeta without null bitmap length
            bitmapFileIndexMeta =
                    new BitmapFileIndexMeta(
                            dataType,
                            options,
                            rowCount,
                            id2bitmap.size(),
                            hasNull,
                            nullOffset,
                            bitmapOffsets);
        } else {
            // VERSION_2: Use BitmapFileIndexMetaV2 with null bitmap length
            bitmapFileIndexMeta =
                    new BitmapFileIndexMetaV2(
                            dataType,
                            options,
                            rowCount,
                            id2bitmap.size(),
                            hasNull,
                            nullOffset,
                            nullBitmapLength,
                            bitmapOffsets,
                            offsetRef[0]);
        }

        bitmapFileIndexMeta.serialize(dos);

        // 5. Serialize body
        if (nullBitmapLength > 0) {
            dos.write(nullBitmapBytes);
        }
        for (byte[] bytes : serializeBitmaps) {
            dos.write(bytes);
        }
    }

    public Map<Object, BITMAP> getId2bitmap() {
        return id2bitmap;
    }

    /** Interface for bitmap-specific operations. */
    public interface BitmapOperations<BITMAP> {
        BITMAP createEmptyBitmap();

        void addToBitmap(BITMAP bitmap, long index);

        byte[] serializeBitmap(BITMAP bitmap);

        long getCardinality(BITMAP bitmap);

        long getFirstValue(BITMAP bitmap);

        boolean isEmpty(BITMAP bitmap);
    }

    /** Operations for RoaringBitmap32. */
    public static class Bitmap32Operations
            implements BitmapOperations<org.apache.paimon.utils.RoaringBitmap32> {

        @Override
        public org.apache.paimon.utils.RoaringBitmap32 createEmptyBitmap() {
            return new org.apache.paimon.utils.RoaringBitmap32();
        }

        @Override
        public void addToBitmap(org.apache.paimon.utils.RoaringBitmap32 bitmap, long index) {
            bitmap.add((int) index);
        }

        @Override
        public byte[] serializeBitmap(org.apache.paimon.utils.RoaringBitmap32 bitmap) {
            return bitmap.serialize();
        }

        @Override
        public long getCardinality(org.apache.paimon.utils.RoaringBitmap32 bitmap) {
            return bitmap.getCardinality();
        }

        @Override
        public long getFirstValue(org.apache.paimon.utils.RoaringBitmap32 bitmap) {
            return bitmap.iterator().next();
        }

        @Override
        public boolean isEmpty(org.apache.paimon.utils.RoaringBitmap32 bitmap) {
            return bitmap.isEmpty();
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
        public void addToBitmap(org.apache.paimon.utils.RoaringBitmap64 bitmap, long index) {
            bitmap.add(index);
        }

        @Override
        public byte[] serializeBitmap(org.apache.paimon.utils.RoaringBitmap64 bitmap) {
            return bitmap.serialize();
        }

        @Override
        public long getCardinality(org.apache.paimon.utils.RoaringBitmap64 bitmap) {
            return bitmap.getCardinality();
        }

        @Override
        public long getFirstValue(org.apache.paimon.utils.RoaringBitmap64 bitmap) {
            return bitmap.iterator().next();
        }

        @Override
        public boolean isEmpty(org.apache.paimon.utils.RoaringBitmap64 bitmap) {
            return bitmap.isEmpty();
        }
    }
}
