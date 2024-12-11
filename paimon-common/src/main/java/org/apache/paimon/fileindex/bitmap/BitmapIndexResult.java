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
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.RoaringBitmap32;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/** bitmap file index result. */
public class BitmapIndexResult extends LazyField<RoaringBitmap32> implements FileIndexResult {

    private final Set<FieldRef> fields;

    public BitmapIndexResult(Set<FieldRef> fields, Supplier<RoaringBitmap32> supplier) {
        super(supplier);
        this.fields = new HashSet<>(fields);
    }

    @Override
    public boolean remain() {
        return !get().isEmpty();
    }

    @Override
    public Set<FieldRef> applyIndexes() {
        return fields;
    }

    @Override
    public FileIndexResult and(FileIndexResult fileIndexResult) {
        if (fileIndexResult instanceof BitmapIndexResult) {
            fields.addAll(fileIndexResult.applyIndexes());
            return new BitmapIndexResult(
                    fields,
                    () -> RoaringBitmap32.and(get(), ((BitmapIndexResult) fileIndexResult).get()));
        }
        return FileIndexResult.super.and(fileIndexResult);
    }

    @Override
    public FileIndexResult or(FileIndexResult fileIndexResult) {
        if (fileIndexResult instanceof BitmapIndexResult) {
            fields.addAll(fileIndexResult.applyIndexes());
            return new BitmapIndexResult(
                    fields,
                    () -> RoaringBitmap32.or(get(), ((BitmapIndexResult) fileIndexResult).get()));
        }
        return FileIndexResult.super.and(fileIndexResult);
    }
}
