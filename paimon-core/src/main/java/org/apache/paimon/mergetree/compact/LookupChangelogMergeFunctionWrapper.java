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
import org.apache.paimon.types.RowKind;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Wrapper for {@link MergeFunction}s to produce changelog by lookup during the compaction involving
 * level 0 files.
 *
 * <p>Changelog records are generated in the process of the level-0 file participating in the
 * compaction, if during the compaction processing:
 *
 * <ul>
 *   <li>Without level-0 records, no changelog.
 *   <li>With level-0 record, with level-x (x > 0) record, level-x record should be BEFORE, level-0
 *       should be AFTER.
 *   <li>With level-0 record, without level-x record, need to lookup the history value of the upper
 *       level as BEFORE.
 * </ul>
 */
public class LookupChangelogMergeFunctionWrapper implements MergeFunctionWrapper<ChangelogResult> {

    private final LookupMergeFunction mergeFunction;
    private final MergeFunction<KeyValue> mergeFunction2;
    private final Function<InternalRow, KeyValue> lookup;

    private final ChangelogResult reusedResult = new ChangelogResult();
    private final KeyValue reusedBefore = new KeyValue();
    private final KeyValue reusedAfter = new KeyValue();
    private final RecordEqualiser valueEqualiser;
    private final boolean changelogRowDeduplicate;

    public LookupChangelogMergeFunctionWrapper(
            MergeFunctionFactory<KeyValue> mergeFunctionFactory,
            Function<InternalRow, KeyValue> lookup,
            RecordEqualiser valueEqualiser,
            boolean changelogRowDeduplicate) {
        MergeFunction<KeyValue> mergeFunction = mergeFunctionFactory.create();
        checkArgument(
                mergeFunction instanceof LookupMergeFunction,
                "Merge function should be a LookupMergeFunction, but is %s, there is a bug.",
                mergeFunction.getClass().getName());
        this.mergeFunction = (LookupMergeFunction) mergeFunction;
        this.mergeFunction2 = mergeFunctionFactory.create();
        this.lookup = lookup;
        this.valueEqualiser = valueEqualiser;
        this.changelogRowDeduplicate = changelogRowDeduplicate;
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

        // 1. Compute the latest high level record and containLevel0 of candidates
        LinkedList<KeyValue> candidates = mergeFunction.candidates();
        Iterator<KeyValue> descending = candidates.descendingIterator();
        KeyValue highLevel = null;
        boolean containLevel0 = false;
        while (descending.hasNext()) {
            KeyValue kv = descending.next();
            if (kv.level() > 0) {
                if (highLevel != null) {
                    descending.remove();
                } else {
                    highLevel = kv;
                }
            } else {
                containLevel0 = true;
            }
        }

        // 2. Lookup if latest high level record is absent
        if (highLevel == null) {
            InternalRow lookupKey = candidates.get(0).key();
            highLevel = lookup.apply(lookupKey);
        }

        // 3. Calculate result
        mergeFunction2.reset();
        if (highLevel != null) {
            mergeFunction2.add(highLevel);
        }
        candidates.forEach(mergeFunction2::add);
        KeyValue result = mergeFunction2.getResult();

        // 4. Set changelog when there's level-0 records
        if (containLevel0) {
            setChangelog(highLevel, result);
        }

        return reusedResult.setResult(result);
    }

    private void setChangelog(KeyValue before, KeyValue after) {
        if (before == null || !before.isAdd()) {
            if (after.isAdd()) {
                reusedResult.addChangelog(replaceAfter(RowKind.INSERT, after));
            }
        } else {
            if (!after.isAdd()) {
                reusedResult.addChangelog(replaceBefore(RowKind.DELETE, before));
            } else if (!changelogRowDeduplicate
                    || !valueEqualiser.equals(before.value(), after.value())) {
                reusedResult
                        .addChangelog(replaceBefore(RowKind.UPDATE_BEFORE, before))
                        .addChangelog(replaceAfter(RowKind.UPDATE_AFTER, after));
            }
        }
    }

    private KeyValue replaceBefore(RowKind valueKind, KeyValue from) {
        return replace(reusedBefore, valueKind, from);
    }

    private KeyValue replaceAfter(RowKind valueKind, KeyValue from) {
        return replace(reusedAfter, valueKind, from);
    }

    private KeyValue replace(KeyValue reused, RowKind valueKind, KeyValue from) {
        return reused.replace(from.key(), from.sequenceNumber(), valueKind, from.value());
    }
}
