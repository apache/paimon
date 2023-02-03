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

import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.utils.Preconditions;

import javax.annotation.Nullable;

/**
 * Wrapper for {@link MergeFunction}s to produce changelog during a full compaction.
 *
 * <p>This wrapper can only be used in {@link SortMergeReader} because
 *
 * <ul>
 *   <li>This wrapper does not copy {@link KeyValue}s. As {@link KeyValue}s are reused by readers
 *       this may cause issues in other readers.
 *   <li>{@link KeyValue}s with the same key come from different inner readers in {@link
 *       SortMergeReader}, so there is no issue related to object reuse.
 * </ul>
 */
public class FullChangelogMergeFunctionWrapper
        implements MergeFunctionWrapper<FullChangelogMergeFunctionWrapper.Result> {

    private final MergeFunction<KeyValue> mergeFunction;
    private final int maxLevel;

    // only full compaction will write files into maxLevel, see UniversalCompaction class
    private transient KeyValue topLevelKv;
    private transient KeyValue initialKv;
    private transient boolean isInitialized;

    private transient Result reusedResult;
    private transient KeyValue reusedBefore;
    private transient KeyValue reusedAfter;

    public FullChangelogMergeFunctionWrapper(MergeFunction<KeyValue> mergeFunction, int maxLevel) {
        Preconditions.checkArgument(
                !(mergeFunction instanceof ValueCountMergeFunction),
                "Value count merge function does not need to produce changelog from full compaction. "
                        + "Please set changelog producer to 'input'.");
        this.mergeFunction = mergeFunction;
        this.maxLevel = maxLevel;
    }

    @Override
    public void reset() {
        mergeFunction.reset();

        topLevelKv = null;
        initialKv = null;
        isInitialized = false;
    }

    @Override
    public void add(KeyValue kv) {
        if (maxLevel == kv.level()) {
            Preconditions.checkState(
                    topLevelKv == null, "Top level key-value already exists! This is unexpected.");
            topLevelKv = kv;
        }

        if (initialKv == null) {
            initialKv = kv;
        } else {
            if (!isInitialized) {
                merge(initialKv);
                isInitialized = true;
            }
            merge(kv);
        }
    }

    private void merge(KeyValue kv) {
        mergeFunction.add(kv);
    }

    @Override
    public Result getResult() {
        if (reusedResult == null) {
            reusedResult = new Result();
            reusedBefore = new KeyValue();
            reusedAfter = new KeyValue();
        }

        if (isInitialized) {
            KeyValue merged = mergeFunction.getResult();
            if (topLevelKv == null) {
                if (merged != null && isAdd(merged)) {
                    reusedResult.setChangelog(null, replace(reusedAfter, RowKind.INSERT, merged));
                } else {
                    reusedResult.setChangelog(null, null);
                }
            } else {
                if (merged != null && isAdd(merged)) {
                    reusedResult.setChangelog(
                            replace(reusedBefore, RowKind.UPDATE_BEFORE, topLevelKv),
                            replace(reusedAfter, RowKind.UPDATE_AFTER, merged));
                } else {
                    reusedResult.setChangelog(
                            replace(reusedBefore, RowKind.DELETE, topLevelKv), null);
                }
            }
            return reusedResult.setResult(merged);
        } else {
            if (topLevelKv == null && isAdd(initialKv)) {
                reusedResult.setChangelog(null, replace(reusedAfter, RowKind.INSERT, initialKv));
            } else {
                // either topLevelKv is not null, but there is only one kv,
                // so topLevelKv must be the only kv, which means there is no change
                //
                // or initialKv is not an ADD kv, so no new key is added
                reusedResult.setChangelog(null, null);
            }
            return reusedResult.setResult(initialKv);
        }
    }

    private KeyValue replace(KeyValue reused, RowKind valueKind, KeyValue from) {
        return reused.replace(from.key(), from.sequenceNumber(), valueKind, from.value());
    }

    private boolean isAdd(KeyValue kv) {
        return kv.valueKind() == RowKind.INSERT || kv.valueKind() == RowKind.UPDATE_AFTER;
    }

    /** Changelog and final result for the same key. */
    public static class Result {

        @Nullable private KeyValue before;
        @Nullable private KeyValue after;
        @Nullable private KeyValue result;

        private Result() {}

        private void setChangelog(@Nullable KeyValue before, @Nullable KeyValue after) {
            this.before = before;
            this.after = after;
        }

        private Result setResult(@Nullable KeyValue result) {
            if (result != null && result.valueKind() != RowKind.DELETE) {
                this.result = result;
            } else {
                this.result = null;
            }
            return this;
        }

        /**
         * Previous full compaction result for this key. Null if this key does not appear in the
         * previous result or its value remains unchanged.
         */
        @Nullable
        public KeyValue before() {
            return before;
        }

        /**
         * Latest full compaction result for this key. Null if this key does not appear in the
         * latest result or its value remains unchanged.
         */
        @Nullable
        public KeyValue after() {
            return after;
        }

        /**
         * Latest full compaction result (result of merge function) for this key. Null if the merged
         * result is null or is of DELETE kind.
         */
        @Nullable
        public KeyValue result() {
            return result;
        }
    }
}
