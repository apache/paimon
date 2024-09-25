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
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Preconditions;

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
public class FullChangelogMergeFunctionWrapper implements MergeFunctionWrapper<ChangelogResult> {

    private final MergeFunction<KeyValue> mergeFunction;
    private final int maxLevel;
    @Nullable private final RecordEqualiser valueEqualiser;

    // only full compaction will write files into maxLevel, see UniversalCompaction class
    private KeyValue topLevelKv;
    private KeyValue initialKv;
    private boolean isInitialized;

    private final ChangelogResult reusedResult = new ChangelogResult();
    private final KeyValue reusedBefore = new KeyValue();
    private final KeyValue reusedAfter = new KeyValue();

    public FullChangelogMergeFunctionWrapper(
            MergeFunction<KeyValue> mergeFunction,
            int maxLevel,
            @Nullable RecordEqualiser valueEqualiser) {
        this.mergeFunction = mergeFunction;
        this.maxLevel = maxLevel;
        this.valueEqualiser = valueEqualiser;
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
    public ChangelogResult getResult() {
        reusedResult.reset();
        if (isInitialized) {
            KeyValue merged = mergeFunction.getResult();
            if (topLevelKv == null) {
                if (merged.isAdd()) {
                    reusedResult.addChangelog(replace(reusedAfter, RowKind.INSERT, merged));
                }
            } else {
                if (!merged.isAdd()) {
                    reusedResult.addChangelog(replace(reusedBefore, RowKind.DELETE, topLevelKv));
                } else if (valueEqualiser == null
                        || !valueEqualiser.equals(topLevelKv.value(), merged.value())) {
                    reusedResult
                            .addChangelog(replace(reusedBefore, RowKind.UPDATE_BEFORE, topLevelKv))
                            .addChangelog(replace(reusedAfter, RowKind.UPDATE_AFTER, merged));
                }
            }
            return reusedResult.setResultIfNotRetract(merged);
        } else {
            if (topLevelKv == null && initialKv.isAdd()) {
                reusedResult.addChangelog(replace(reusedAfter, RowKind.INSERT, initialKv));
            }
            // either topLevelKv is not null, but there is only one kv,
            // so topLevelKv must be the only kv, which means there is no change
            //
            // or initialKv is not an ADD kv, so no new key is added
            return reusedResult.setResultIfNotRetract(initialKv);
        }
    }

    private KeyValue replace(KeyValue reused, RowKind valueKind, KeyValue from) {
        return reused.replace(from.key(), from.sequenceNumber(), valueKind, from.value());
    }
}
