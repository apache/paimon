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

package org.apache.paimon.index.bitmap;

import org.apache.paimon.index.globalindex.GlobalIndexResult;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap64;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Supplier;

/** bitmap file index result. */
public class GlobalBitmapIndexResult extends LazyField<RoaringBitmap64>
        implements GlobalIndexResult {

    public GlobalBitmapIndexResult(Supplier<RoaringBitmap64> supplier) {
        super(supplier);
    }

    @Override
    public GlobalIndexResult and(GlobalIndexResult fileIndexResult) {
        if (fileIndexResult instanceof GlobalBitmapIndexResult) {
            return new GlobalBitmapIndexResult(
                    () ->
                            RoaringBitmap64.and(
                                    get(), ((GlobalBitmapIndexResult) fileIndexResult).get()));
        }
        return GlobalIndexResult.super.and(fileIndexResult);
    }

    @Override
    public GlobalIndexResult or(GlobalIndexResult fileIndexResult) {
        if (fileIndexResult instanceof GlobalBitmapIndexResult) {
            return new GlobalBitmapIndexResult(
                    () ->
                            RoaringBitmap64.or(
                                    get(), ((GlobalBitmapIndexResult) fileIndexResult).get()));
        }
        return GlobalIndexResult.super.or(fileIndexResult);
    }

    @Override
    public boolean empty() {
        return get().isEmpty();
    }

    @Override
    public Iterator<Range> results() {
        // Convert row IDs from bitmap to Range objects
        // Each row ID becomes a range [id, id+1)
        Iterator<Long> rowIds = get().iterator();
        return new Iterator<Range>() {
            @Override
            public boolean hasNext() {
                return rowIds.hasNext();
            }

            @Override
            public Range next() {
                long rowId = rowIds.next();
                return new Range(rowId, rowId + 1);
            }
        };
    }

    /**
     * Gets the row IDs as an iterator.
     *
     * @return iterator of row IDs
     */
    public Iterator<Long> rowIds() {
        return get().iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GlobalBitmapIndexResult that = (GlobalBitmapIndexResult) o;
        return Objects.equals(this.get(), that.get());
    }
}
