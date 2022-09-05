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

/** Helper functions for the interaction with {@link MergeFunction}. */
public class MergeFunctionHelper {

    private final MergeFunction mergeFunction;

    private KeyValue initialKV;
    private boolean isInitialized;

    public MergeFunctionHelper(MergeFunction mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    /** Resets the {@link MergeFunction} helper to its default state. */
    public void reset() {
        initialKV = null;
        mergeFunction.reset();
        isInitialized = false;
    }

    /** Adds the given {@link KeyValue} to the {@link MergeFunction} helper. */
    public void add(KeyValue kv) {
        if (initialKV == null) {
            initialKV = kv;
        } else {
            if (!isInitialized) {
                merge(initialKV);
                isInitialized = true;
            }
            merge(kv);
        }
    }

    protected void merge(KeyValue kv) {
        mergeFunction.add(kv);
    }

    /** Get current value of the {@link MergeFunction} helper. */
    public RowData getValue() {
        return isInitialized ? mergeFunction.getValue() : initialKV.value();
    }
}
