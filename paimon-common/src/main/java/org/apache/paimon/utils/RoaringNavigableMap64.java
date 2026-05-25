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

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/** A compressed bitmap for 64-bit integer aggregated by tree. */
public class RoaringNavigableMap64 implements Iterable<Long> {

    private final Roaring64NavigableMap roaring64NavigableMap;

    public RoaringNavigableMap64() {
        this.roaring64NavigableMap = new Roaring64NavigableMap();
    }

    private RoaringNavigableMap64(Roaring64NavigableMap bitmap) {
        this.roaring64NavigableMap = bitmap;
    }

    public void addRange(Range range) {
        roaring64NavigableMap.addRange(range.from, range.to + 1);
    }

    public boolean contains(long x) {
        return roaring64NavigableMap.contains(x);
    }

    public void add(long x) {
        roaring64NavigableMap.add(x);
    }

    public void or(RoaringNavigableMap64 other) {
        roaring64NavigableMap.or(other.roaring64NavigableMap);
    }

    public void and(RoaringNavigableMap64 other) {
        roaring64NavigableMap.and(other.roaring64NavigableMap);
    }

    public void andNot(RoaringNavigableMap64 other) {
        roaring64NavigableMap.andNot(other.roaring64NavigableMap);
    }

    public boolean isEmpty() {
        return roaring64NavigableMap.isEmpty();
    }

    public boolean runOptimize() {
        return roaring64NavigableMap.runOptimize();
    }

    public long getLongCardinality() {
        return roaring64NavigableMap.getLongCardinality();
    }

    public int getIntCardinality() {
        return roaring64NavigableMap.getIntCardinality();
    }

    public Iterator<Long> iterator() {
        return roaring64NavigableMap.iterator();
    }

    public static RoaringNavigableMap64 and(RoaringNavigableMap64 x1, RoaringNavigableMap64 x2) {
        Roaring64NavigableMap result = new Roaring64NavigableMap();
        result.or(x1.roaring64NavigableMap);
        result.and(x2.roaring64NavigableMap);
        return new RoaringNavigableMap64(result);
    }

    public static RoaringNavigableMap64 or(RoaringNavigableMap64 x1, RoaringNavigableMap64 x2) {
        Roaring64NavigableMap result = new Roaring64NavigableMap();
        result.or(x1.roaring64NavigableMap);
        result.or(x2.roaring64NavigableMap);
        return new RoaringNavigableMap64(result);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RoaringNavigableMap64 that = (RoaringNavigableMap64) o;
        return Objects.equals(this.roaring64NavigableMap, that.roaring64NavigableMap);
    }

    public void clear() {
        roaring64NavigableMap.clear();
    }

    public byte[] serialize() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos)) {
            roaring64NavigableMap.runOptimize();
            roaring64NavigableMap.serializePortable(dos);
            return bos.toByteArray();
        }
    }

    public void deserialize(byte[] rbmBytes) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(rbmBytes);
                DataInputStream dis = new DataInputStream(bis)) {
            roaring64NavigableMap.deserializePortable(dis);
        }
    }

    /**
     * Converts this bitmap to a list of contiguous ranges.
     *
     * <p>This is useful for interoperability with APIs that expect List&lt;Range&gt;.
     */
    public List<Range> toRangeList() {
        // TODO Optimize this to avoid iterator all ids
        return Range.toRanges(roaring64NavigableMap::iterator);
    }

    public static RoaringNavigableMap64 bitmapOf(long... dat) {
        RoaringNavigableMap64 roaringBitmap64 = new RoaringNavigableMap64();
        for (long ele : dat) {
            roaringBitmap64.add(ele);
        }
        return roaringBitmap64;
    }
}
