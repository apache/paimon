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

package org.apache.paimon.utils;

import org.apache.paimon.annotation.VisibleForTesting;

import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;

/** A compressed bitmap for 32-bit integer. */
public class RoaringBitmap32 {

    public static final int MAX_VALUE = Integer.MAX_VALUE;

    private final RoaringBitmap roaringBitmap;

    public RoaringBitmap32() {
        this.roaringBitmap = new RoaringBitmap();
    }

    private RoaringBitmap32(RoaringBitmap roaringBitmap) {
        this.roaringBitmap = roaringBitmap;
    }

    public void add(int x) {
        roaringBitmap.add(x);
    }

    public void or(RoaringBitmap32 other) {
        roaringBitmap.or(other.roaringBitmap);
    }

    public boolean checkedAdd(int x) {
        return roaringBitmap.checkedAdd(x);
    }

    public boolean contains(int x) {
        return roaringBitmap.contains(x);
    }

    public boolean isEmpty() {
        return roaringBitmap.isEmpty();
    }

    public long getCardinality() {
        return roaringBitmap.getLongCardinality();
    }

    public long rangeCardinality(long start, long end) {
        return roaringBitmap.rangeCardinality(start, end);
    }

    public void serialize(DataOutput out) throws IOException {
        roaringBitmap.runOptimize();
        roaringBitmap.serialize(out);
    }

    public void deserialize(DataInput in) throws IOException {
        roaringBitmap.deserialize(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RoaringBitmap32 that = (RoaringBitmap32) o;
        return Objects.equals(this.roaringBitmap, that.roaringBitmap);
    }

    public void clear() {
        roaringBitmap.clear();
    }

    public byte[] serialize() {
        roaringBitmap.runOptimize();
        ByteBuffer buffer = ByteBuffer.allocate(roaringBitmap.serializedSizeInBytes());
        roaringBitmap.serialize(buffer);
        return buffer.array();
    }

    public void deserialize(byte[] rbmBytes) throws IOException {
        roaringBitmap.deserialize(ByteBuffer.wrap(rbmBytes));
    }

    public void flip(final long rangeStart, final long rangeEnd) {
        roaringBitmap.flip(rangeStart, rangeEnd);
    }

    public Iterator<Integer> iterator() {
        return roaringBitmap.iterator();
    }

    @Override
    public String toString() {
        return roaringBitmap.toString();
    }

    @VisibleForTesting
    public static RoaringBitmap32 bitmapOf(int... dat) {
        RoaringBitmap32 roaringBitmap32 = new RoaringBitmap32();
        for (int ele : dat) {
            roaringBitmap32.add(ele);
        }
        return roaringBitmap32;
    }

    public static RoaringBitmap32 and(final RoaringBitmap32 x1, final RoaringBitmap32 x2) {
        return new RoaringBitmap32(RoaringBitmap.and(x1.roaringBitmap, x2.roaringBitmap));
    }

    public static RoaringBitmap32 or(final RoaringBitmap32 x1, final RoaringBitmap32 x2) {
        return new RoaringBitmap32(RoaringBitmap.or(x1.roaringBitmap, x2.roaringBitmap));
    }

    public static RoaringBitmap32 or(Iterator<RoaringBitmap32> iterator) {
        return new RoaringBitmap32(
                RoaringBitmap.or(
                        new Iterator<RoaringBitmap>() {
                            @Override
                            public boolean hasNext() {
                                return iterator.hasNext();
                            }

                            @Override
                            public RoaringBitmap next() {
                                return iterator.next().roaringBitmap;
                            }
                        }));
    }
}
