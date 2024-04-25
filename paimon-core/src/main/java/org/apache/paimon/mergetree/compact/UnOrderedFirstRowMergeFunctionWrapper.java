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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.util.function.Function;

/** Wrapper for {@link MergeFunction}s to produce changelog by lookup for first row. */
public class UnOrderedFirstRowMergeFunctionWrapper<T>
        extends LookupChangelogMergeFunctionWrapper<T> {

    private final KeyValue reusedBefore = new KeyValue();
    private final KeyValue reusedAfter = new KeyValue();

    public UnOrderedFirstRowMergeFunctionWrapper(
            MergeFunctionFactory<KeyValue> mergeFunctionFactory,
            Function<InternalRow, T> lookup,
            RecordEqualiser valueEqualiser,
            boolean changelogRowDeduplicate,
            LookupStrategy lookupStrategy,
            @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer,
            @Nullable UserDefinedSeqComparator userDefinedSeqComparator) {
        super(
                mergeFunctionFactory,
                lookup,
                valueEqualiser,
                changelogRowDeduplicate,
                lookupStrategy,
                deletionVectorsMaintainer,
                userDefinedSeqComparator);
    }

    @Override
    protected void setChangelog(
            @Nullable KeyValue before, KeyValue after, ChangelogResult reusedResult) {
        if (after.level() == 0) {
            if (before == null) {
                reusedResult.addChangelog(after);
            } else {
                reusedResult
                        .addChangelog(replace(reusedBefore, RowKind.UPDATE_BEFORE, before))
                        .addChangelog(replace(reusedAfter, RowKind.UPDATE_AFTER, after));
            }
        }
    }
}
