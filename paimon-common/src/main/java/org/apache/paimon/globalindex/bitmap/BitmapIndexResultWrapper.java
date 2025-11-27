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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap32;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.util.Iterator;

import static org.apache.paimon.utils.RoaringBitmap32.bitmapOfRange;

/** A {@link GlobalIndexResult} wrapper for {@link BitmapIndexResult}. */
public class BitmapIndexResultWrapper implements GlobalIndexResult {

    private final BitmapIndexResult result;
    private final long start;
    private RoaringNavigableMap64 lazyResult;

    public BitmapIndexResultWrapper(BitmapIndexResult result, long start) {
        this.result = result;
        this.start = start;
    }

    public BitmapIndexResultWrapper(RoaringNavigableMap64 finalResult) {
        this.result = null;
        this.start = -1;
        this.lazyResult = finalResult;
    }

    @Override
    public RoaringNavigableMap64 results() {
        if (lazyResult != null) {
            return lazyResult;
        }
        if (result == null) {
            throw new IllegalStateException("No results available");
        }
        RoaringBitmap32 bitmap = result.get();
        RoaringNavigableMap64 result64 = new RoaringNavigableMap64();

        // Convert 32-bit bitmap to 64-bit bitmap with offset
        Iterator<Integer> iterator = bitmap.iterator();
        while (iterator.hasNext()) {
            result64.add(iterator.next() + start);
        }
        result64.runOptimize();
        this.lazyResult = result64;
        return result64;
    }

    public static BitmapIndexResultWrapper fromRange(Range range) {
        RoaringNavigableMap64 result64 = new RoaringNavigableMap64();
        result64.addRange(range);
        return new BitmapIndexResultWrapper(result64);
    }

    public BitmapIndexResult getBitmapIndexResult() {
        return result;
    }
}
