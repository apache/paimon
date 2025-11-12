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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.util.Comparator;

/**
 * A {@link MergeFunction} for lookup, this wrapper only considers the latest high level record,
 * because each merge will query the old merged record, so the latest high level record should be
 * the final merged value.
 */
public class LookupMergeFunction implements MergeFunction<KeyValue> {

    private final MergeFunction<KeyValue> mergeFunction;

    private final KeyValueBuffer candidates;
    private boolean containLevel0;
    private InternalRow currentKey;

    public LookupMergeFunction(
            MergeFunction<KeyValue> mergeFunction,
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            @Nullable IOManager ioManager) {
        this.mergeFunction = mergeFunction;
        this.candidates = KeyValueBuffer.createHybridBuffer(options, keyType, valueType, ioManager);
    }

    @Override
    public void reset() {
        candidates.reset();
        currentKey = null;
        containLevel0 = false;
    }

    @Override
    public void add(KeyValue kv) {
        currentKey = kv.key();
        if (kv.level() == 0) {
            containLevel0 = true;
        }
        candidates.put(kv);
    }

    public boolean containLevel0() {
        return containLevel0;
    }

    @Nullable
    public KeyValue pickHighLevel() {
        KeyValue highLevel = null;
        try (CloseableIterator<KeyValue> iterator = candidates.iterator()) {
            while (iterator.hasNext()) {
                KeyValue kv = iterator.next();
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return highLevel;
    }

    public InternalRow key() {
        return currentKey;
    }

    public void insertInto(KeyValue highLevel, Comparator<KeyValue> comparator) {
        KeyValueBuffer.insertInto(candidates, highLevel, comparator);
    }

    @Override
    public KeyValue getResult() {
        mergeFunction.reset();
        KeyValue highLevel = pickHighLevel();
        try (CloseableIterator<KeyValue> iterator = candidates.iterator()) {
            while (iterator.hasNext()) {
                KeyValue kv = iterator.next();
                // records that has not been stored on the disk yet, such as the data in the write
                // buffer being at level -1
                if (kv.level() <= 0 || kv == highLevel) {
                    mergeFunction.add(kv);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return mergeFunction.getResult();
    }

    @Override
    public boolean requireCopy() {
        return true;
    }

    public static MergeFunctionFactory<KeyValue> wrap(
            MergeFunctionFactory<KeyValue> wrapped,
            CoreOptions options,
            RowType keyType,
            RowType valueType) {
        if (wrapped.create() instanceof FirstRowMergeFunction) {
            // don't wrap first row, it is already OK
            return wrapped;
        }

        return new Factory(wrapped, options, keyType, valueType);
    }

    /** Factory to create {@link LookupMergeFunction}. */
    public static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final MergeFunctionFactory<KeyValue> wrapped;
        private final CoreOptions options;
        private final RowType keyType;
        private final RowType valueType;

        private @Nullable IOManager ioManager;

        private Factory(
                MergeFunctionFactory<KeyValue> wrapped,
                CoreOptions options,
                RowType keyType,
                RowType valueType) {
            this.wrapped = wrapped;
            this.options = options;
            this.keyType = keyType;
            this.valueType = valueType;
        }

        public void withIOManager(@Nullable IOManager ioManager) {
            this.ioManager = ioManager;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            return new LookupMergeFunction(
                    wrapped.create(projection), options, keyType, valueType, ioManager);
        }

        @Override
        public AdjustedProjection adjustProjection(@Nullable int[][] projection) {
            return wrapped.adjustProjection(projection);
        }
    }
}
