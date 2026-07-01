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

package org.apache.paimon.fileindex.rtree;

import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.utils.RoaringBitmap32;

import java.util.function.Supplier;

/** Index result for R-Tree index. */
public class RTreeIndexResult implements FileIndexResult {
    private final Supplier<RoaringBitmap32> bitmapSupplier;
    private RoaringBitmap32 bitmap;
    private int rowCount;

    public RTreeIndexResult(Supplier<RoaringBitmap32> bitmapSupplier) {
        this.bitmapSupplier = bitmapSupplier;
    }

    public RTreeIndexResult(Supplier<RoaringBitmap32> bitmapSupplier, int rowCount) {
        this.bitmapSupplier = bitmapSupplier;
        this.rowCount = rowCount;
    }

    public RoaringBitmap32 getBitmap() {
        if (bitmap == null) {
            bitmap = bitmapSupplier.get();
        }
        return bitmap;
    }

    @Override
    public boolean remain() {
        return !getBitmap().isEmpty();
    }

    @Override
    public FileIndexResult and(FileIndexResult fileIndexResult) {
        if (fileIndexResult instanceof RTreeIndexResult) {
            RoaringBitmap32 other = ((RTreeIndexResult) fileIndexResult).getBitmap();
            RoaringBitmap32 result = RoaringBitmap32.and(getBitmap(), other);
            return new RTreeIndexResult(() -> result, rowCount);
        }
        return FileIndexResult.super.and(fileIndexResult);
    }

    @Override
    public FileIndexResult or(FileIndexResult fileIndexResult) {
        if (fileIndexResult instanceof RTreeIndexResult) {
            RoaringBitmap32 other = ((RTreeIndexResult) fileIndexResult).getBitmap();
            RoaringBitmap32 result = RoaringBitmap32.or(getBitmap(), other);
            return new RTreeIndexResult(() -> result, rowCount);
        }
        return FileIndexResult.super.or(fileIndexResult);
    }
}
