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

import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

/** A compressed bitmap for 64-bit integer. */
public class RoaringBitmap64 {

    private final Roaring64Bitmap roaringBitmap;

    public RoaringBitmap64() {
        this.roaringBitmap = new Roaring64Bitmap();
    }

    public void add(long x) {
        roaringBitmap.add(x);
    }

    public void or(RoaringBitmap64 other) {
        roaringBitmap.or(other.roaringBitmap);
    }

    public long getCardinality() {
        return roaringBitmap.getLongCardinality();
    }

    public boolean isEmpty() {
        return roaringBitmap.isEmpty();
    }

    public void flip(final long rangeStart, final long rangeEnd) {
        roaringBitmap.flip(rangeStart, rangeEnd);
    }

    public Iterator<Long> iterator() {
        return roaringBitmap.iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RoaringBitmap64 that = (RoaringBitmap64) o;
        return Objects.equals(this.roaringBitmap, that.roaringBitmap);
    }

    public void clear() {
        roaringBitmap.clear();
    }

    public byte[] serialize() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos)) {
            roaringBitmap.runOptimize();
            roaringBitmap.serialize(dos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize RoaringBitmap64", e);
        }
    }

    public void deserialize(byte[] rbmBytes) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(rbmBytes);
                DataInputStream dis = new DataInputStream(bis)) {
            roaringBitmap.deserialize(dis);
        }
    }

    public void deserialize(DataInput in) throws IOException {
        roaringBitmap.deserialize(in);
    }

    @VisibleForTesting
    public static RoaringBitmap64 bitmapOf(long... dat) {
        RoaringBitmap64 roaringBitmap64 = new RoaringBitmap64();
        for (long ele : dat) {
            roaringBitmap64.add(ele);
        }
        return roaringBitmap64;
    }

    public static RoaringBitmap64 or(RoaringBitmap64 left, RoaringBitmap64 right) {
        RoaringBitmap64 result = new RoaringBitmap64();
        result.roaringBitmap.or(left.roaringBitmap);
        result.roaringBitmap.or(right.roaringBitmap);
        return result;
    }

    public static RoaringBitmap64 or(Iterator<RoaringBitmap64> iterator) {
        RoaringBitmap64 result = new RoaringBitmap64();
        while (iterator.hasNext()) {
            RoaringBitmap64 next = iterator.next();
            result.roaringBitmap.or(next.roaringBitmap);
        }
        return result;
    }

    public static RoaringBitmap64 and(RoaringBitmap64 left, RoaringBitmap64 right) {
        RoaringBitmap64 result = new RoaringBitmap64();
        result.roaringBitmap.and(left.roaringBitmap);
        result.roaringBitmap.and(right.roaringBitmap);
        return result;
    }
}
