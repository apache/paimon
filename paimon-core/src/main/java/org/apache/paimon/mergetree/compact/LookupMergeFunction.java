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
    private final LevelOrder levelOrder;

    public LookupMergeFunction(
            MergeFunction<KeyValue> mergeFunction,
            RowType keyType,
            RowType valueType,
            LevelOrder levelOrder) {
        this.mergeFunction = mergeFunction;
        this.keySerializer = new InternalRowSerializer(keyType);
        this.valueSerializer = new InternalRowSerializer(valueType);
        this.levelOrder = levelOrder;
    }

    /**
     * The relationship between the high-level order and the sequence order.
     * <li>For the first row, as the level increases, so does the sequence number.
     * <li>For all other merge engine, as the level increases, the sequence number decreases.
     */
    enum LevelOrder {
        DESCENDING,
        ASCENDING
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
        Iterator<KeyValue> iterator =
                levelOrder == LevelOrder.DESCENDING
                        ? candidates.descendingIterator()
                        : candidates().iterator();
        KeyValue highLevel = null;
        while (iterator.hasNext()) {
            KeyValue kv = iterator.next();
            if (kv.level() > 0) {
                if (highLevel != null) {
                    iterator.remove();
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

    LevelOrder getLevelOrder() {
        return levelOrder;
    }

    public static MergeFunctionFactory<KeyValue> wrap(
            MergeFunctionFactory<KeyValue> wrapped, RowType keyType, RowType valueType) {
        if (wrapped.create().getClass() == FirstRowMergeFunction.class) {
            // don't wrap first row, it is already OK
            return wrapped;
        }
        return new Factory(
                wrapped,
                keyType,
                valueType,
                wrapped.create() instanceof UnOrderedFirstRowMergeFunction
                        ? LevelOrder.ASCENDING
                        : LevelOrder.DESCENDING);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final MergeFunctionFactory<KeyValue> wrapped;
        private final RowType keyType;
        private final RowType rowType;
        private final LevelOrder levelOrder;

        private Factory(
                MergeFunctionFactory<KeyValue> wrapped,
                RowType keyType,
                RowType rowType,
                LevelOrder levelOrder) {
            this.wrapped = wrapped;
            this.keyType = keyType;
            this.rowType = rowType;
            this.levelOrder = levelOrder;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            RowType valueType =
                    projection == null ? rowType : Projection.of(projection).project(rowType);
            return new LookupMergeFunction(
                    wrapped.create(projection), keyType, valueType, levelOrder);
        }

        @Override
        public AdjustedProjection adjustProjection(@Nullable int[][] projection) {
            return wrapped.adjustProjection(projection);
        }
    }
}
