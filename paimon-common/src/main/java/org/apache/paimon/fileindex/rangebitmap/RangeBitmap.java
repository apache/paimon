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

package org.apache.paimon.fileindex.rangebitmap;

import org.apache.paimon.fileindex.rangebitmap.dictionary.Dictionary;
import org.apache.paimon.fileindex.rangebitmap.dictionary.chunked.ChunkedDictionary;
import org.apache.paimon.fileindex.rangebitmap.dictionary.chunked.KeyFactory;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Implementation of range-bitmap. */
public class RangeBitmap {

    public static final int VERSION_1 = 1;
    public static final byte CURRENT_VERSION = VERSION_1;

    private final int rid;
    @Nullable private final Object min;
    @Nullable private final Object max;
    private final int cardinality;
    private final int dictionaryOffset;
    private final int bsiOffset;

    private final SeekableInputStream in;
    private final KeyFactory factory;
    private final Comparator<Object> comparator;

    private Dictionary dictionary;
    private BitSliceIndexBitmap bsi;

    public RangeBitmap(SeekableInputStream in, int offset, KeyFactory factory) {
        ByteBuffer headers;
        int headerLength;
        try {
            in.seek(offset);
            byte[] headerLengthInBytes = new byte[Integer.BYTES];
            IOUtils.readFully(in, headerLengthInBytes);
            headerLength = ByteBuffer.wrap(headerLengthInBytes).getInt();

            byte[] headerInBytes = new byte[headerLength];
            IOUtils.readFully(in, headerInBytes);
            headers = ByteBuffer.wrap(headerInBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KeyFactory.KeyDeserializer deserializer = factory.createDeserializer();
        byte version = headers.get();
        if (version > CURRENT_VERSION) {
            throw new RuntimeException("Invalid version " + version);
        }
        this.rid = headers.getInt();
        this.cardinality = headers.getInt();
        this.min = cardinality <= 0 ? null : deserializer.deserialize(headers);
        this.max = cardinality <= 0 ? null : deserializer.deserialize(headers);
        int dictionaryLength = headers.getInt();

        this.dictionaryOffset = offset + Integer.BYTES + headerLength;
        this.bsiOffset = dictionaryOffset + dictionaryLength;

        this.in = in;
        this.factory = factory;
        this.comparator = factory.createComparator();
    }

    public RoaringBitmap32 eq(Object key) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMin == 0 && compareMax == 0) {
            return isNotNull();
        } else if (compareMin < 0 || compareMax > 0) {
            return new RoaringBitmap32();
        }

        int code = getDictionary().find(key);
        if (code < 0) {
            return new RoaringBitmap32();
        }
        return getBitSliceIndexBitmap().eq(code);
    }

    public RoaringBitmap32 neq(Object key) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }
        return not(eq(key));
    }

    public RoaringBitmap32 lte(Object key) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMax >= 0) {
            return isNotNull();
        } else if (compareMin < 0) {
            return new RoaringBitmap32();
        }

        return not(gt(key));
    }

    public RoaringBitmap32 lt(Object key) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMax > 0) {
            return isNotNull();
        } else if (compareMin <= 0) {
            return new RoaringBitmap32();
        }

        return not(gte(key));
    }

    public RoaringBitmap32 gte(Object key) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMin <= 0) {
            return isNotNull();
        } else if (compareMax > 0) {
            return new RoaringBitmap32();
        }

        int code = getDictionary().find(key);
        return code < 0
                ? getBitSliceIndexBitmap().gte(-code - 1)
                : getBitSliceIndexBitmap().gte(code);
    }

    public RoaringBitmap32 gt(Object key) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMin < 0) {
            return isNotNull();
        } else if (compareMax >= 0) {
            return new RoaringBitmap32();
        }

        int code = getDictionary().find(key);
        return code < 0
                ? getBitSliceIndexBitmap().gte(-code - 1)
                : getBitSliceIndexBitmap().gt(code);
    }

    public RoaringBitmap32 in(List<Object> keys) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        RoaringBitmap32 bitmap = new RoaringBitmap32();
        for (Object key : keys) {
            bitmap.or(eq(key));
        }
        return bitmap;
    }

    public RoaringBitmap32 notIn(List<Object> keys) {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        return not(in(keys));
    }

    public RoaringBitmap32 isNull() {
        if (cardinality <= 0) {
            return rid > 0 ? RoaringBitmap32.bitmapOf(0, rid - 1) : new RoaringBitmap32();
        }

        RoaringBitmap32 bitmap = isNotNull();
        bitmap.flip(0, rid);
        return bitmap;
    }

    public RoaringBitmap32 isNotNull() {
        if (cardinality <= 0) {
            return new RoaringBitmap32();
        }

        return getBitSliceIndexBitmap().isNotNull();
    }

    public Object get(int position) {
        if (position < 0 || position >= rid) {
            return null;
        }
        Integer code = getBitSliceIndexBitmap().get(position);
        if (code == null) {
            return null;
        }
        return getDictionary().find(code);
    }

    private RoaringBitmap32 not(RoaringBitmap32 bitmap) {
        bitmap.flip(0, rid);
        bitmap.and(isNotNull());
        return bitmap;
    }

    private Dictionary getDictionary() {
        if (dictionary == null) {
            try {
                dictionary = new ChunkedDictionary(in, dictionaryOffset, factory);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dictionary;
    }

    private BitSliceIndexBitmap getBitSliceIndexBitmap() {
        if (bsi == null) {
            try {
                bsi = new BitSliceIndexBitmap(in, bsiOffset);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return bsi;
    }

    /** A Builder for {@link RangeBitmap}. */
    public static class Appender {

        private int rid;
        private final TreeMap<Object, RoaringBitmap32> bitmaps;
        private final KeyFactory factory;
        private final int limitedSerializedSizeInBytes;

        public Appender(KeyFactory factory, int limitedSerializedSizeInBytes) {
            this.rid = 0;
            this.bitmaps = new TreeMap<>(factory.createComparator());
            this.factory = factory;
            this.limitedSerializedSizeInBytes = limitedSerializedSizeInBytes;
        }

        public void append(Object key) {
            if (key != null) {
                bitmaps.computeIfAbsent(key, (x) -> new RoaringBitmap32()).add(rid);
            }
            rid++;
        }

        public byte[] serialize() {
            int code = 0;
            BitSliceIndexBitmap.Appender bsi =
                    new BitSliceIndexBitmap.Appender(0, bitmaps.size() - 1);
            ChunkedDictionary.Appender dictionary =
                    new ChunkedDictionary.Appender(factory, limitedSerializedSizeInBytes);
            for (Map.Entry<Object, RoaringBitmap32> entry : bitmaps.entrySet()) {
                Object key = entry.getKey();
                RoaringBitmap32 bitmap = entry.getValue();

                // build the dictionary
                dictionary.sortedAppend(key, code);

                // build the relationship between position and code by the bsi
                Iterator<Integer> iterator = bitmap.iterator();
                while (iterator.hasNext()) {
                    bsi.append(iterator.next(), code);
                }

                code++;
            }

            // serializer
            KeyFactory.KeySerializer serializer = factory.createSerializer();

            // min & max
            Object min = bitmaps.isEmpty() ? null : bitmaps.firstKey();
            Object max = bitmaps.isEmpty() ? null : bitmaps.lastKey();

            int headerSize = 0;
            headerSize += Byte.BYTES; // version
            headerSize += Integer.BYTES; // rid
            headerSize += Integer.BYTES; // cardinality
            headerSize += min == null ? 0 : serializer.serializedSizeInBytes(min); // min
            headerSize += max == null ? 0 : serializer.serializedSizeInBytes(max); // max
            headerSize += Integer.BYTES; // dictionary length

            // dictionary
            byte[] dictionarySerializeInBytes = dictionary.serialize();
            int dictionaryLength = dictionarySerializeInBytes.length;

            // bsi
            ByteBuffer bsiBuffer = bsi.serialize();
            int bsiLength = bsiBuffer.array().length;

            ByteBuffer buffer =
                    ByteBuffer.allocate(Integer.BYTES + headerSize + dictionaryLength + bsiLength);
            // write header length
            buffer.putInt(headerSize);

            // write header
            buffer.put(CURRENT_VERSION);
            buffer.putInt(rid);
            buffer.putInt(bitmaps.size());
            if (min != null) {
                serializer.serialize(buffer, min);
            }
            if (max != null) {
                serializer.serialize(buffer, max);
            }
            buffer.putInt(dictionaryLength);

            // write dictionary
            buffer.put(dictionarySerializeInBytes);

            // write bsi
            buffer.put(bsiBuffer.array());

            return buffer.array();
        }
    }
}
