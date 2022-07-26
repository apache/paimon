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


import javax.annotation.Nullable;

/** A lazy {@link MergeFunction}. */
public class LazyMergeFunction implements MergeFunction {

    private final MergeFunction mergeFunction;

    private KeyValue firstKV;
    private boolean isInitialized;

    public LazyMergeFunction(MergeFunction mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    @Override
    public void reset() {
        firstKV = null;
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

    protected void merge(KeyValue kv) {
        mergeFunction.add(kv);
    }

    @Nullable
    @Override
    public RowData getValue() {
        return isInitialized ? mergeFunction.getValue() : firstKV.value();
    }

    @Override
    public MergeFunction copy() {
        return new LazyMergeFunction(mergeFunction.copy());
    }
}
