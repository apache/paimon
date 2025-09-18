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

import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.predicate.LimitDirection;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.RoaringBitmap32;

import java.util.Objects;
import java.util.function.Supplier;

/** bitmap file index result. */
public class BitmapIndexResult extends LazyField<RoaringBitmap32> implements FileIndexResult {

    public BitmapIndexResult(Supplier<RoaringBitmap32> supplier) {
        super(supplier);
    }

    @Override
    public boolean remain() {
        return !get().isEmpty();
    }

    @Override
    public FileIndexResult and(FileIndexResult fileIndexResult) {
        if (fileIndexResult instanceof BitmapIndexResult) {
            return new BitmapIndexResult(
                    () -> RoaringBitmap32.and(get(), ((BitmapIndexResult) fileIndexResult).get()));
        }
        return FileIndexResult.super.and(fileIndexResult);
    }

    @Override
    public FileIndexResult or(FileIndexResult fileIndexResult) {
        if (fileIndexResult instanceof BitmapIndexResult) {
            return new BitmapIndexResult(
                    () -> RoaringBitmap32.or(get(), ((BitmapIndexResult) fileIndexResult).get()));
        }
        return FileIndexResult.super.or(fileIndexResult);
    }

    public BitmapIndexResult andNot(RoaringBitmap32 deletion) {
        return new BitmapIndexResult(() -> RoaringBitmap32.andNot(get(), deletion));
    }

    public FileIndexResult limit(int limit, LimitDirection direction) {
        return new BitmapIndexResult(() -> get().limit(limit, direction));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BitmapIndexResult that = (BitmapIndexResult) o;
        return Objects.equals(this.get(), that.get());
    }
}
