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

/** Helper functions for the interaction with {@link MergeFunction}. */
public class MergeFunctionHelper {

    private final MergeFunction mergeFunction;

    private RowData rowData;
    private boolean isInitialized;

    public MergeFunctionHelper(MergeFunction mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    /**
     * Resets the {@link MergeFunction} helper to its default state: 1. Clears the one record which
     * the helper maintains. 2. Resets the {@link MergeFunction} to its default state. 3. Clears the
     * initialized state of the {@link MergeFunction}.
     */
    public void reset() {
        rowData = null;
        mergeFunction.reset();
        isInitialized = false;
    }

    /** Adds the given {@link RowData} to the {@link MergeFunction} helper. */
    public void add(RowData value) {
        if (rowData == null) {
            rowData = value;
        } else {
            if (!isInitialized) {
                mergeFunction.add(rowData);
                isInitialized = true;
            }
            mergeFunction.add(value);
        }
    }

    /**
     * Get current value of the {@link MergeFunction} helper. Return null if the value should be
     * skipped.
     */
    public RowData getValue() {
        return isInitialized ? mergeFunction.getValue() : rowData;
    }
}
