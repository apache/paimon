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

/**
 * Wrapper for {@link MergeFunction}s which works like a reducer.
 *
 * <p>A reducer is a type of function. If there is only one input the result is equal to that input;
 * Otherwise the result is calculated by merging all the inputs in some way.
 *
 * <p>This wrapper optimize the wrapped {@link MergeFunction}. If there is only one input, the input
 * will be stored and the inner merge function will not be called, thus saving some computing time.
 */
public class ReducerMergeFunctionWrapper implements MergeFunctionWrapper<KeyValue> {

    private final MergeFunction<KeyValue> mergeFunction;

    private KeyValue initialKv;
    private boolean isInitialized;

    public ReducerMergeFunctionWrapper(MergeFunction<KeyValue> mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    /** Resets the {@link MergeFunction} helper to its default state. */
    @Override
    public void reset() {
        initialKv = null;
        mergeFunction.reset();
        isInitialized = false;
    }

    /** Adds the given {@link KeyValue} to the {@link MergeFunction} helper. */
    @Override
    public void add(KeyValue kv) {
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

    /** Get current value of the {@link MergeFunction} helper. */
    @Override
    public KeyValue getResult() {
        return isInitialized ? mergeFunction.getResult() : initialKv;
    }
}
