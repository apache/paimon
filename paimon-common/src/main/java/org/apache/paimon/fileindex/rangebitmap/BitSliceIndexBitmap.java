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

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.paimon.utils.IOUtils.readFully;

/** Implementation of bit-slice index bitmap. */
public class BitSliceIndexBitmap {

    public static final byte VERSION_1 = 1;
    public static final byte CURRENT_VERSION = VERSION_1;

    private final int ebmLength;
    private final ByteBuffer indexes;
    private final RoaringBitmap32[] slices;
    private final SeekableInputStream in;
    private final int bodyOffset;

    private RoaringBitmap32 ebm;
    private boolean initialized = false;

    public BitSliceIndexBitmap(SeekableInputStream in, int offset) throws IOException {
        in.seek(offset);
        byte[] headerLengthInBytes = new byte[Integer.BYTES];
        readFully(in, headerLengthInBytes);
        int headerLength = ByteBuffer.wrap(headerLengthInBytes).getInt();

        byte[] headerInBytes = new byte[headerLength];
        readFully(in, headerInBytes);
        ByteBuffer headers = ByteBuffer.wrap(headerInBytes);

        byte version = headers.get();
        if (version > CURRENT_VERSION) {
            throw new RuntimeException(
                    String.format(
                            "deserialize bsi index fail, " + "current version is lower than %d",
                            version));
        }

        // slices size
        slices = new RoaringBitmap32[headers.get()];

        // ebm length
        ebmLength = headers.getInt();

        // read indexes
        int indexesLength = headers.getInt();
        indexes = (ByteBuffer) headers.slice().limit(indexesLength);

        this.in = in;
        this.bodyOffset = offset + Integer.BYTES + headerLength;
    }

    @Nullable
    public Integer get(int key) {
        if (!isNotNull().contains(key)) {
            return null;
        }
        int value = 0;
        for (int i = 0; i < slices.length; i++) {
            if (getSlice(i).contains(key)) {
                value |= (1 << i);
            }
        }
        return value;
    }

    public RoaringBitmap32 eq(int code) {
        RoaringBitmap32 state = isNotNull();
        if (state.isEmpty()) {
            return new RoaringBitmap32();
        }

        loadSlices(0, slices.length);
        for (int i = 0; i < slices.length; i++) {
            int bit = (code >> i) & 1;
            if (bit == 1) {
                state.and(getSlice(i));
            } else {
                state.andNot(getSlice(i));
            }
        }
        return state;
    }

    public RoaringBitmap32 gt(int code) {
        if (code < 0) {
            return isNotNull();
        }

        RoaringBitmap32 foundSet = isNotNull();
        if (foundSet.isEmpty()) {
            return new RoaringBitmap32();
        }

        // the state is always start from the empty bitmap
        RoaringBitmap32 state = null;

        // if there is a run of k set bits starting from 0, [0, k] operations can be eliminated.
        int start = Long.numberOfTrailingZeros(~code);
        loadSlices(start, slices.length);
        for (int i = start; i < slices.length; i++) {
            if (state == null) {
                state = getSlice(i).clone();
                continue;
            }

            long bit = (code >> i) & 1;
            if (bit == 1) {
                state.and(getSlice(i));
            } else {
                state.or(getSlice(i));
            }
        }

        if (state == null) {
            return new RoaringBitmap32();
        }

        state.and(foundSet);
        return state;
    }

    public RoaringBitmap32 gte(int code) {
        return gt(code - 1);
    }

    /**
     * Find k rows with largest values in a BSI.
     *
     * <p>Refer to algorithm 4.1 in the paper <a
     * href="https://www.cs.umb.edu/~poneil/SIGBSTMH.pdf">Bit-Sliced Index Arithmetic</a>
     *
     * @param k K largest values.
     * @param foundSet the selection.
     * @param strict if true, the result will be trimmed; otherwise, it will not be.
     */
    public RoaringBitmap32 topK(int k, @Nullable RoaringBitmap32 foundSet, boolean strict) {
        if (k == 0 || (foundSet != null && foundSet.isEmpty())) {
            return new RoaringBitmap32();
        }

        if (k < 0) {
            throw new IllegalArgumentException("the k param can not be negative in topK, k=" + k);
        }

        RoaringBitmap32 g = new RoaringBitmap32();
        RoaringBitmap32 e = isNotNull(foundSet);
        if (e.getCardinality() <= k) {
            return e;
        }

        loadSlices(0, slices.length);
        for (int i = slices.length - 1; i >= 0; i--) {
            RoaringBitmap32 x = RoaringBitmap32.or(g, RoaringBitmap32.and(e, getSlice(i)));
            long n = x.getCardinality();
            if (n > k) {
                e = RoaringBitmap32.and(e, getSlice(i));
            } else if (n < k) {
                g = x;
                e = RoaringBitmap32.andNot(e, getSlice(i));
            } else {
                e = RoaringBitmap32.and(e, getSlice(i));
                break;
            }
        }

        RoaringBitmap32 f = RoaringBitmap32.or(g, e);
        if (!strict) {
            return f;
        }

        // return k rows
        long n = f.getCardinality() - k;
        if (n > 0) {
            Iterator<Integer> iterator = e.iterator();
            while (iterator.hasNext() && n > 0) {
                f.remove(iterator.next());
                n--;
            }
        }
        return f;
    }

    /**
     * Find k rows with smallest values in a BSI.
     *
     * @param k K smallest values.
     * @param foundSet the selection.
     * @param strict if true, the result will be trimmed; otherwise, it will not be.
     */
    public RoaringBitmap32 bottomK(int k, @Nullable RoaringBitmap32 foundSet, boolean strict) {
        if (k == 0 || (foundSet != null && foundSet.isEmpty())) {
            return new RoaringBitmap32();
        }

        if (k < 0) {
            throw new IllegalArgumentException(
                    "the k param can not be negative in bottomK, k=" + k);
        }

        RoaringBitmap32 g = new RoaringBitmap32();
        RoaringBitmap32 e = isNotNull(foundSet);
        if (e.getCardinality() <= k) {
            return e;
        }

        loadSlices(0, slices.length);
        for (int i = slices.length - 1; i >= 0; i--) {
            RoaringBitmap32 x = RoaringBitmap32.or(g, RoaringBitmap32.andNot(e, getSlice(i)));
            long n = x.getCardinality();
            if (n > k) {
                e = RoaringBitmap32.andNot(e, getSlice(i));
            } else if (n < k) {
                g = x;
                e = RoaringBitmap32.and(e, getSlice(i));
            } else {
                e = RoaringBitmap32.andNot(e, getSlice(i));
                break;
            }
        }

        RoaringBitmap32 f = RoaringBitmap32.or(g, e);
        if (!strict) {
            return f;
        }

        // return k rows
        long n = f.getCardinality() - k;
        if (n > 0) {
            Iterator<Integer> iterator = e.iterator();
            while (iterator.hasNext() && n > 0) {
                f.remove(iterator.next());
                n--;
            }
        }
        return f;
    }

    public RoaringBitmap32 isNotNull() {
        return isNotNull(null);
    }

    private RoaringBitmap32 isNotNull(@Nullable RoaringBitmap32 foundSet) {
        if (ebm == null) {
            try {
                in.seek(bodyOffset);
                byte[] bytes = new byte[ebmLength];
                readFully(in, bytes);
                RoaringBitmap32 bitmap = new RoaringBitmap32();
                bitmap.deserialize(ByteBuffer.wrap(bytes));
                ebm = bitmap;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return foundSet == null ? ebm.clone() : RoaringBitmap32.and(ebm, foundSet);
    }

    private void loadSlices(int begin, int end) {
        if (initialized) {
            return;
        }

        indexes.position(2 * Integer.BYTES * begin);
        int offset = indexes.getInt();
        int length = indexes.getInt();
        int[] lengths = new int[end];
        lengths[begin] = length;
        for (int i = begin + 1; i < end; i++) {
            indexes.getInt();
            lengths[i] = indexes.getInt();
            length += lengths[i];
        }

        try {
            in.seek(bodyOffset + ebmLength + offset);
            byte[] bytes = new byte[length];
            readFully(in, bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            int position = 0;
            for (int i = begin; i < end; i++) {
                buffer.position(position);
                RoaringBitmap32 slice = new RoaringBitmap32();
                slice.deserialize((ByteBuffer) buffer.slice().limit(lengths[i]));
                slices[i] = slice;
                position += lengths[i];
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        initialized = true;
    }

    private RoaringBitmap32 getSlice(int index) {
        if (slices[index] == null) {
            indexes.position(2 * Integer.BYTES * index);
            int offset = indexes.getInt();
            int length = indexes.getInt();

            try {
                in.seek(bodyOffset + ebmLength + offset);
                byte[] bytes = new byte[length];
                readFully(in, bytes);
                RoaringBitmap32 slice = new RoaringBitmap32();
                slice.deserialize(ByteBuffer.wrap(bytes));
                slices[index] = slice;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return slices[index];
    }

    /** A Builder for {@link BitSliceIndexBitmap}. */
    public static final class Appender {

        private final int min;
        private final int max;
        private final RoaringBitmap32 ebm;
        private final RoaringBitmap32[] slices;

        public Appender(int min, int max) {
            this.min = min;
            this.max = max;
            this.ebm = new RoaringBitmap32();
            this.slices =
                    new RoaringBitmap32[Math.max(Long.SIZE - Long.numberOfLeadingZeros(max), 1)];
            for (int i = 0; i < slices.length; i++) {
                slices[i] = new RoaringBitmap32();
            }
        }

        public void append(int key, int value) {
            if (value < 0) {
                throw new UnsupportedOperationException("value can not be negative");
            }

            if (value < min || value > max) {
                throw new IllegalArgumentException("value not in range [" + min + "," + max + "]");
            }

            // only bit=1 need to set
            long bits = value;
            while (bits != 0) {
                slices[Long.numberOfTrailingZeros(bits)].add(key);
                bits &= (bits - 1);
            }
            ebm.add(key);
        }

        public ByteBuffer serialize() {
            int indexesLength = 2 * Integer.BYTES * slices.length;

            byte[] ebmSerializeInBytes = ebm.serialize();
            int ebmLength = ebmSerializeInBytes.length;

            int headerSize = 0;
            headerSize += Byte.BYTES; // version
            headerSize += Byte.BYTES; // slices size
            headerSize += Integer.BYTES; // ebm length
            headerSize += Integer.BYTES; // indexes length
            headerSize += indexesLength; // indexes size in bytes

            int bodySize = 0;
            bodySize += ebmLength;

            int offset = 0;
            ByteBuffer indexes = ByteBuffer.allocate(indexesLength);
            List<byte[]> slicesSerializeInBytes = new ArrayList<>();
            for (RoaringBitmap32 slice : slices) {
                byte[] sliceInBytes = slice.serialize();
                slicesSerializeInBytes.add(sliceInBytes);

                int length = sliceInBytes.length;
                indexes.putInt(offset);
                indexes.putInt(length);

                offset += length;
                bodySize += length;
            }

            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + headerSize + bodySize);
            buffer.putInt(headerSize);

            // write header
            buffer.put(CURRENT_VERSION);
            buffer.put((byte) slices.length);
            buffer.putInt(ebmLength);
            buffer.putInt(indexesLength);
            buffer.put(indexes.array());

            // write body
            buffer.put(ebmSerializeInBytes);
            for (byte[] slice : slicesSerializeInBytes) {
                buffer.put(slice);
            }

            return buffer;
        }
    }
}
