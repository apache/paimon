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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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

    public byte[] serialize() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos)) {
            roaringBitmap.runOptimize();
            roaringBitmap.serialize(dos);
            return bos.toByteArray();
        }
    }

    public void deserialize(byte[] rbmBytes) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(rbmBytes);
                DataInputStream dis = new DataInputStream(bis)) {
            roaringBitmap.deserialize(dis);
        }
    }

    @VisibleForTesting
    public static RoaringBitmap64 bitmapOf(long... dat) {
        RoaringBitmap64 roaringBitmap64 = new RoaringBitmap64();
        for (long ele : dat) {
            roaringBitmap64.add(ele);
        }
        return roaringBitmap64;
    }
}
