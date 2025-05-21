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
import org.apache.paimon.data.InternalRow;

import javax.annotation.Nullable;

import java.util.LinkedList;

/**
 * A {@link MergeFunction} for lookup, this wrapper only considers the latest high level record,
 * because each merge will query the old merged record, so the latest high level record should be
 * the final merged value.
 */
public class LookupMergeFunction implements MergeFunction<KeyValue> {

    private final MergeFunction<KeyValue> mergeFunction;
    private final LinkedList<KeyValue> candidates = new LinkedList<>();

    public LookupMergeFunction(MergeFunction<KeyValue> mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    @Override
    public void reset() {
        candidates.clear();
    }

    @Override
    public void add(KeyValue kv) {
        candidates.add(kv);
    }

    @Nullable
    public KeyValue pickHighLevel() {
        KeyValue highLevel = null;
        for (KeyValue kv : candidates) {
            // records that has not been stored on the disk yet, such as the data in the write
            // buffer being at level -1
            if (kv.level() <= 0) {
                continue;
            }
            // For high-level comparison logic (not involving Level 0), only the value of the
            // minimum Level should be selected
            if (highLevel == null || kv.level() < highLevel.level()) {
                highLevel = kv;
            }
        }
        return highLevel;
    }

    public InternalRow key() {
        return candidates.get(0).key();
    }

    public LinkedList<KeyValue> candidates() {
        return candidates;
    }

    @Override
    public KeyValue getResult() {
        mergeFunction.reset();
        KeyValue highLevel = pickHighLevel();
        for (KeyValue kv : candidates) {
            // records that has not been stored on the disk yet, such as the data in the write
            // buffer being at level -1
            if (kv.level() <= 0 || kv == highLevel) {
                mergeFunction.add(kv);
            }
        }
        return mergeFunction.getResult();
    }

    @Override
    public boolean requireCopy() {
        return true;
    }

    public static MergeFunctionFactory<KeyValue> wrap(MergeFunctionFactory<KeyValue> wrapped) {
        if (wrapped.create() instanceof FirstRowMergeFunction) {
            // don't wrap first row, it is already OK
            return wrapped;
        }

        return new Factory(wrapped);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final MergeFunctionFactory<KeyValue> wrapped;

        private Factory(MergeFunctionFactory<KeyValue> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            return new LookupMergeFunction(wrapped.create(projection));
        }

        @Override
        public AdjustedProjection adjustProjection(@Nullable int[][] projection) {
            return wrapped.adjustProjection(projection);
        }
    }
}
