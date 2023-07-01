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
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.io.DataFileTestUtils.row;

/** Tests for {@link FullChangelogMergeFunctionWrapper}. */
public abstract class FullChangelogMergeFunctionWrapperTestBase {

    private static final int MAX_LEVEL = 3;

    private static final RecordEqualiser EQUALISER =
            (RecordEqualiser) (row1, row2) -> row1.getInt(0) == row2.getInt(0);

    protected FullChangelogMergeFunctionWrapper wrapper;

    protected abstract MergeFunction<KeyValue> createMergeFunction();

    protected abstract boolean changelogRowDeduplicate();

    protected List<List<KeyValue>> getInputKvs() {
        return INPUT_KVS;
    }

    @BeforeEach
    public void beforeEach() {
        wrapper =
                new FullChangelogMergeFunctionWrapper(
                        createMergeFunction(), MAX_LEVEL, EQUALISER, changelogRowDeduplicate());
    }

    private static final List<List<KeyValue>> INPUT_KVS =
            Arrays.asList(
                    // only 1 insert record, not from top level
                    Collections.singletonList(
                            new KeyValue().replace(row(1), 1, RowKind.INSERT, row(1)).setLevel(0)),
                    // only 1 delete record, not from top level
                    Collections.singletonList(
                            new KeyValue().replace(row(2), 2, RowKind.DELETE, row(0)).setLevel(0)),
                    // only 1 insert record, from top level
                    Collections.singletonList(
                            new KeyValue()
                                    .replace(row(3), 3, RowKind.INSERT, row(3))
                                    .setLevel(MAX_LEVEL)),
                    // multiple records, none from top level
                    Arrays.asList(
                            new KeyValue().replace(row(4), 4, RowKind.INSERT, row(3)).setLevel(0),
                            new KeyValue().replace(row(4), 5, RowKind.INSERT, row(-3)).setLevel(0)),
                    Arrays.asList(
                            new KeyValue().replace(row(5), 6, RowKind.INSERT, row(3)).setLevel(0),
                            new KeyValue().replace(row(5), 7, RowKind.DELETE, row(3)).setLevel(0)),
                    // multiple records, one from top level
                    Arrays.asList(
                            new KeyValue()
                                    .replace(row(6), 8, RowKind.INSERT, row(3))
                                    .setLevel(MAX_LEVEL),
                            new KeyValue().replace(row(6), 9, RowKind.INSERT, row(-3)).setLevel(0)),
                    Arrays.asList(
                            new KeyValue()
                                    .replace(row(7), 10, RowKind.INSERT, row(3))
                                    .setLevel(MAX_LEVEL),
                            new KeyValue().replace(row(7), 11, RowKind.DELETE, row(3)).setLevel(0)),
                    Arrays.asList(
                            new KeyValue()
                                    .replace(row(7), 12, RowKind.INSERT, row(3))
                                    .setLevel(MAX_LEVEL),
                            new KeyValue()
                                    .replace(row(7), 13, RowKind.UPDATE_BEFORE, row(3))
                                    .setLevel(0)),
                    Arrays.asList(
                            new KeyValue()
                                    .replace(row(8), 14, RowKind.INSERT, row(3))
                                    .setLevel(MAX_LEVEL),
                            new KeyValue()
                                    .replace(row(8), 15, RowKind.INSERT, row(3))
                                    .setLevel(0)));

    protected abstract KeyValue getExpectedBefore(int idx);

    protected abstract KeyValue getExpectedAfter(int idx);

    protected abstract KeyValue getExpectedResult(int idx);

    @Test
    public void testFullChangelogMergeFunctionWrapper() {
        List<List<KeyValue>> inputs = getInputKvs();
        for (int i = 0; i < inputs.size(); i++) {
            wrapper.reset();
            List<KeyValue> kvs = inputs.get(i);
            kvs.forEach(kv -> wrapper.add(kv));
            ChangelogResult actualResult = wrapper.getResult();
            List<KeyValue> expectedChangelogs = new ArrayList<>();
            if (getExpectedBefore(i) != null) {
                expectedChangelogs.add(getExpectedBefore(i));
            }
            if (getExpectedAfter(i) != null) {
                expectedChangelogs.add(getExpectedAfter(i));
            }
            MergeFunctionTestUtils.assertKvsEquals(expectedChangelogs, actualResult.changelogs());
            MergeFunctionTestUtils.assertKvEquals(getExpectedResult(i), actualResult.result());
        }
    }

    /**
     * Tests for {@link FullChangelogMergeFunctionWrapper} with {@link DeduplicateMergeFunction}.
     */
    public abstract static class WithDeduplicateMergeFunctionTestBase
            extends FullChangelogMergeFunctionWrapperTestBase {

        private final List<KeyValue> expectedBefore =
                Arrays.asList(
                        null,
                        null,
                        null,
                        null,
                        null,
                        new KeyValue().replace(row(6), 8, RowKind.UPDATE_BEFORE, row(3)),
                        new KeyValue().replace(row(7), 10, RowKind.DELETE, row(3)),
                        new KeyValue().replace(row(7), 12, RowKind.DELETE, row(3)),
                        changelogRowDeduplicate()
                                ? null
                                : new KeyValue()
                                        .replace(row(8), 14, RowKind.UPDATE_BEFORE, row(3)));

        private final List<KeyValue> expectedAfter =
                Arrays.asList(
                        new KeyValue().replace(row(1), 1, RowKind.INSERT, row(1)),
                        null,
                        null,
                        new KeyValue().replace(row(4), 5, RowKind.INSERT, row(-3)),
                        null,
                        new KeyValue().replace(row(6), 9, RowKind.UPDATE_AFTER, row(-3)),
                        null,
                        null,
                        changelogRowDeduplicate()
                                ? null
                                : new KeyValue().replace(row(8), 15, RowKind.UPDATE_AFTER, row(3)));

        private final List<KeyValue> expectedResult =
                Arrays.asList(
                        new KeyValue().replace(row(1), 1, RowKind.INSERT, row(1)),
                        null,
                        new KeyValue().replace(row(3), 3, RowKind.INSERT, row(3)),
                        new KeyValue().replace(row(4), 5, RowKind.INSERT, row(-3)),
                        null,
                        new KeyValue().replace(row(6), 9, RowKind.INSERT, row(-3)),
                        null,
                        null,
                        new KeyValue().replace(row(8), 15, RowKind.INSERT, row(3)));

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            return new DeduplicateMergeFunction();
        }

        @Override
        protected KeyValue getExpectedBefore(int idx) {
            return expectedBefore.get(idx);
        }

        @Override
        protected KeyValue getExpectedAfter(int idx) {
            return expectedAfter.get(idx);
        }

        @Override
        protected KeyValue getExpectedResult(int idx) {
            return expectedResult.get(idx);
        }
    }

    /**
     * Tests for {@link WithDeduplicateMergeFunctionTestBase} with changelog deduplication disabled.
     */
    public static class WithoutChangelogRowDeduplicateMergeFunctionTest
            extends WithDeduplicateMergeFunctionTestBase {

        @Override
        protected boolean changelogRowDeduplicate() {
            return false;
        }
    }

    /**
     * Tests for {@link WithDeduplicateMergeFunctionTestBase} with changelog deduplication enabled.
     */
    public static class WithChangelogRowDeduplicateMergeFunctionTest
            extends WithDeduplicateMergeFunctionTestBase {

        @Override
        protected boolean changelogRowDeduplicate() {
            return true;
        }
    }

    /** Test for {@link FirstRowMergeFunction} with {@link FullChangelogMergeFunctionWrapper}. */
    public static class FirstRowMergeFunctionTest
            extends FullChangelogMergeFunctionWrapperTestBase {

        private static final List<List<KeyValue>> INPUT_KVS =
                Arrays.asList(
                        // only 1 insert record, not from top level
                        Collections.singletonList(
                                new KeyValue()
                                        .replace(row(1), 1, RowKind.INSERT, row(1))
                                        .setLevel(0)),
                        // only 1 delete record, not from top level
                        Collections.singletonList(
                                new KeyValue()
                                        .replace(row(2), 2, RowKind.DELETE, row(0))
                                        .setLevel(0)),
                        // only 1 insert record, from top level
                        Collections.singletonList(
                                new KeyValue()
                                        .replace(row(3), 3, RowKind.INSERT, row(3))
                                        .setLevel(MAX_LEVEL)),
                        // multiple records, none from top level
                        Arrays.asList(
                                new KeyValue()
                                        .replace(row(4), 4, RowKind.INSERT, row(3))
                                        .setLevel(0),
                                new KeyValue()
                                        .replace(row(4), 5, RowKind.INSERT, row(-3))
                                        .setLevel(0)),
                        // multiple records, one from top level
                        Arrays.asList(
                                new KeyValue()
                                        .replace(row(6), 8, RowKind.INSERT, row(3))
                                        .setLevel(MAX_LEVEL),
                                new KeyValue()
                                        .replace(row(6), 9, RowKind.INSERT, row(-3))
                                        .setLevel(0)),
                        Arrays.asList(
                                new KeyValue()
                                        .replace(row(8), 14, RowKind.INSERT, row(3))
                                        .setLevel(MAX_LEVEL),
                                new KeyValue()
                                        .replace(row(8), 15, RowKind.INSERT, row(3))
                                        .setLevel(0)));

        @Override
        protected List<List<KeyValue>> getInputKvs() {
            return INPUT_KVS;
        }

        private final List<KeyValue> expectedBefore =
                Arrays.asList(
                        null,
                        null,
                        null,
                        null,
                        null,
                        changelogRowDeduplicate()
                                ? null
                                : new KeyValue()
                                        .replace(row(8), 14, RowKind.UPDATE_BEFORE, row(3)));

        private final List<KeyValue> expectedAfter =
                Arrays.asList(
                        new KeyValue().replace(row(1), 1, RowKind.INSERT, row(1)),
                        null,
                        null,
                        new KeyValue().replace(row(4), 4, RowKind.INSERT, row(3)),
                        null,
                        changelogRowDeduplicate()
                                ? null
                                : new KeyValue().replace(row(8), 15, RowKind.UPDATE_AFTER, row(3)));

        private final List<KeyValue> expectedResult =
                Arrays.asList(
                        new KeyValue().replace(row(1), 1, RowKind.INSERT, row(1)),
                        null,
                        new KeyValue().replace(row(3), 3, RowKind.INSERT, row(3)),
                        new KeyValue().replace(row(4), 4, RowKind.INSERT, row(3)),
                        new KeyValue().replace(row(6), 8, RowKind.INSERT, row(3)),
                        new KeyValue().replace(row(8), 14, RowKind.INSERT, row(3)));

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            return new FirstRowMergeFunction(
                    RowType.of(DataTypes.INT()), RowType.of(DataTypes.INT()));
        }

        @Override
        protected boolean changelogRowDeduplicate() {
            return true;
        }

        @Override
        protected KeyValue getExpectedBefore(int idx) {
            return expectedBefore.get(idx);
        }

        @Override
        protected KeyValue getExpectedAfter(int idx) {
            return expectedAfter.get(idx);
        }

        @Override
        protected KeyValue getExpectedResult(int idx) {
            return expectedResult.get(idx);
        }
    }
}
