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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.types.RowKind;


import javax.annotation.Nullable;

import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.types.RowKind.UPDATE_BEFORE;
import static org.apache.flink.util.Preconditions.checkArgument;

/** A changelog {@link MergeFunction}. */
public class ChangelogMergeFunction implements MergeFunction {

    private final MergeFunction mergeFunction;

    private final ChangelogConsumer changelogConsumer;

    private final KeyValue reuseChangelogKV;

    private KeyValue firstKV;
    private KeyValue topLevelKV;
    private KeyValue lastKV;
    private boolean isInitialized;

    public ChangelogMergeFunction(MergeFunction mergeFunction, ChangelogConsumer changelogConsumer) {
        this.mergeFunction = mergeFunction;
        this.changelogConsumer = changelogConsumer;
        this.reuseChangelogKV = new KeyValue();
    }

    @Override
    public void reset() {
        firstKV = null;
        topLevelKV = null;
        lastKV = null;
        mergeFunction.reset();
        isInitialized = false;
    }

    @Override
    public void add(KeyValue kv) {
        if (firstKV == null) {
            firstKV = kv;
        } else {
            if (!isInitialized) {
                merge(firstKV);
                isInitialized = true;
            }
            merge(kv);
        }
    }

    private void merge(KeyValue kv) {
        mergeFunction.add(kv);
        if (kv.fromTopLevel()) {
            checkArgument(isAdd(kv), "Only insert/update_after in top level.");
            topLevelKV = kv;
        }
        lastKV = kv;
    }

    @Override
    public RowData getValue() {
        if (isInitialized) {
            RowData merged = mergeFunction.getValue();
            if (isAdd(lastKV)) {
                if (topLevelKV != null) {
                    outputLog(UPDATE_BEFORE, topLevelKV.value());
                    outputLog(UPDATE_AFTER, merged);
                } else {
                    outputLog(INSERT, merged);
                }
            } else {
                if (topLevelKV != null) {
                    outputLog(DELETE, topLevelKV.value());
                }
            }
            return merged;
        } else {
            if (!firstKV.fromTopLevel() && isAdd(firstKV)) {
                outputLog(INSERT, firstKV.value());
            }
            return firstKV.value();
        }
    }

    @Override
    public MergeFunction copy() {
        return new ChangelogMergeFunction(mergeFunction, changelogConsumer);
    }

    private void outputLog(RowKind rowKind, RowData value) {
        changelogConsumer.consume(reuseChangelogKV.replace(firstKV.key(), rowKind, value));
    }

    private boolean isAdd(KeyValue kv) {
        return kv.valueKind() == INSERT || kv.valueKind() == UPDATE_AFTER;
    }
}
