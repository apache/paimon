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
import org.apache.paimon.utils.Filter;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Wrapper for {@link MergeFunction}s to produce changelog by lookup for first row. */
public class FirstRowMergeFunctionWrapper implements MergeFunctionWrapper<ChangelogResult> {

    private final Filter<InternalRow> contains;
    private final FirstRowMergeFunction mergeFunction;
    private final ChangelogResult reusedResult = new ChangelogResult();

    public FirstRowMergeFunctionWrapper(
            MergeFunctionFactory<KeyValue> mergeFunctionFactory, Filter<InternalRow> contains) {
        this.contains = contains;
        MergeFunction<KeyValue> mergeFunction = mergeFunctionFactory.create();
        checkArgument(
                mergeFunction instanceof FirstRowMergeFunction,
                "Merge function should be a FirstRowMergeFunction, but is %s, there is a bug.",
                mergeFunction.getClass().getName());
        this.mergeFunction = (FirstRowMergeFunction) mergeFunction;
    }

    @Override
    public void reset() {
        mergeFunction.reset();
    }

    @Override
    public void add(KeyValue kv) {
        mergeFunction.add(kv);
    }

    @Override
    public ChangelogResult getResult() {
        reusedResult.reset();
        KeyValue result = mergeFunction.getResult();
        if (contains.test(result.key())) {
            // empty
            return reusedResult;
        }

        reusedResult.setResult(result);
        if (result.level() == 0) {
            // new record, output changelog
            return reusedResult.addChangelog(result);
        }
        return reusedResult;
    }
}
