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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * A {@link MergeFunction} for lookup, this wrapper only considers the latest high level record,
 * because each merge will query the old merged record, so the latest high level record should be
 * the final merged value.
 */
public class LookupMergeFunction implements MergeFunction<KeyValue> {

    private final MergeFunction<KeyValue> mergeFunction;
    private final LinkedList<KeyValue> candidates = new LinkedList<>();
    private final InternalRowSerializer keySerializer;
    private final InternalRowSerializer valueSerializer;

    public LookupMergeFunction(
            MergeFunction<KeyValue> mergeFunction, RowType keyType, RowType valueType) {
        this.mergeFunction = mergeFunction;
        this.keySerializer = new InternalRowSerializer(keyType);
        this.valueSerializer = new InternalRowSerializer(valueType);
    }

    @Override
    public void reset() {
        candidates.clear();
    }

    @Override
    public void add(KeyValue kv) {
        candidates.add(kv.copy(keySerializer, valueSerializer));
    }

    @Override
    public KeyValue getResult() {
        // 1. Find the latest high level record
        Iterator<KeyValue> descending = candidates.descendingIterator();
        KeyValue highLevel = null;
        while (descending.hasNext()) {
            KeyValue kv = descending.next();
            if (kv.level() > 0) {
                if (highLevel != null) {
                    descending.remove();
                } else {
                    highLevel = kv;
                }
            }
        }

        // 2. Do the merge for inputs
        mergeFunction.reset();
        candidates.forEach(mergeFunction::add);
        return mergeFunction.getResult();
    }

    LinkedList<KeyValue> candidates() {
        return candidates;
    }

    public static MergeFunctionFactory<KeyValue> wrap(
            MergeFunctionFactory<KeyValue> wrapped, RowType keyType, RowType valueType) {
        if (wrapped.create() instanceof FirstRowMergeFunction) {
            // don't wrap first row, it is already OK
            return wrapped;
        }

        return new Factory(wrapped, keyType, valueType);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final MergeFunctionFactory<KeyValue> wrapped;
        private final RowType keyType;
        private final RowType rowType;

        private Factory(MergeFunctionFactory<KeyValue> wrapped, RowType keyType, RowType rowType) {
            this.wrapped = wrapped;
            this.keyType = keyType;
            this.rowType = rowType;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            RowType valueType =
                    projection == null ? rowType : Projection.of(projection).project(rowType);
            return new LookupMergeFunction(wrapped.create(projection), keyType, valueType);
        }

        @Override
        public AdjustedProjection adjustProjection(@Nullable int[][] projection) {
            return wrapped.adjustProjection(projection);
        }
    }
}
