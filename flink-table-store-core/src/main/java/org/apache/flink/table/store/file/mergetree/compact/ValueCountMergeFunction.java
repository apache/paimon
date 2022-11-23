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

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@link MergeFunction} where key is the full record and value is a count which represents number
 * of records of the exact same fields.
 */
public class ValueCountMergeFunction implements MergeFunction<KeyValue> {

    private KeyValue latestKv;
    private long total;
    private KeyValue reused;

    protected ValueCountMergeFunction() {}

    @Override
    public void reset() {
        latestKv = null;
        total = 0;
    }

    @Override
    public void add(KeyValue kv) {
        checkArgument(
                kv.valueKind() == RowKind.INSERT,
                "In value count mode, only insert records come. This is a bug. Please file an issue.");
        latestKv = kv;
        total += count(kv.value());
    }

    @Override
    @Nullable
    public KeyValue getResult() {
        if (total == 0) {
            return null;
        }

        if (reused == null) {
            reused = new KeyValue();
        }
        return reused.replace(
                latestKv.key(),
                latestKv.sequenceNumber(),
                RowKind.INSERT,
                GenericRowData.of(total));
    }

    private long count(RowData value) {
        checkArgument(!value.isNullAt(0), "Value count should not be null.");
        return value.getLong(0);
    }

    public static MergeFunctionFactory<KeyValue> factory() {
        return new Factory();
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            return new ValueCountMergeFunction();
        }
    }
}
