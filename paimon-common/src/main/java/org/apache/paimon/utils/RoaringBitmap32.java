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

import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** A compressed bitmap for 32-bit integer. */
public class RoaringBitmap32 {

    public static final int MAX_VALUE = Integer.MAX_VALUE;

    private final RoaringBitmap roaringBitmap;

    public RoaringBitmap32() {
        this.roaringBitmap = new RoaringBitmap();
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

    public void serialize(DataOutput out) throws IOException {
        roaringBitmap.runOptimize();
        roaringBitmap.serialize(out);
    }

    public void deserialize(DataInput in) throws IOException {
        roaringBitmap.deserialize(in);
    }
}
