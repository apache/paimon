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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ReusingTestData;
import org.apache.paimon.utils.TestReusingRecordReader;

import org.assertj.core.util.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Tests for {@link SortMergeReader}. */
public abstract class SortMergeReaderTestBase extends CombiningRecordReaderTestBase {

    protected abstract MergeFunction<KeyValue> createMergeFunction();

    @Override
    protected RecordReader<KeyValue> createRecordReader(
            List<TestReusingRecordReader> readers, CoreOptions.SortEngine sortEngine) {
        return SortMergeReader.createSortMergeReader(
                new ArrayList<>(readers),
                KEY_COMPARATOR,
                null,
                new ReducerMergeFunctionWrapper(createMergeFunction()),
                sortEngine);
    }

    @ParameterizedTest
    @EnumSource(SortEngine.class)
    public void testEmpty(SortEngine sortEngine) throws IOException {
        runTest(parseData(""), sortEngine);
        runTest(parseData("", "", ""), sortEngine);
    }

    @ParameterizedTest
    @EnumSource(SortEngine.class)
    public void testAlternateKeys(SortEngine sortEngine) throws IOException {
        runTest(
                parseData(
                        "1, 1, +, 100 | 3, 2, +, 300 | 5, 3, +, 200 | 7, 4, +, 600 | 9, 20, +, 400",
                        "0, 5, +, 0",
                        "0, 10, +, 0",
                        "",
                        "2, 6, +, 200 | 4, 7, +, 400 | 6, 8, +, 600 | 8, 9, +, 800"),
                sortEngine);
    }

    @ParameterizedTest
    @EnumSource(SortEngine.class)
    public void testDuplicateKeys(SortEngine sortEngine) throws IOException {
        runTest(
                parseData("1, 1, +, 100 | 3, 3, +, 300", "1, 4, +, 200 | 3, 5, +, 300"),
                sortEngine);
    }

    @ParameterizedTest
    @EnumSource(SortEngine.class)
    public void testLongTailRecords(SortEngine sortEngine) throws IOException {
        runTest(
                parseData(
                        "1, 1, +, 100 | 2, 500, +, 200",
                        "1, 3, +, 100 | 3, 4, +, 300 | 5, 501, +, 500 | 7, 503, +, 700 | "
                                + "8, 504, +, 800 | 9, 505, +, 900 | 10, 506, +, 1000 | "
                                + "11, 507, +, 1100 | 12, 508, +, 1200 | 13, 509, +, 1300"),
                sortEngine);
    }

    /** Tests for {@link SortMergeReader} with {@link DeduplicateMergeFunction}. */
    public static class WithDeduplicateMergeFunction extends SortMergeReaderTestBase {

        @Override
        protected boolean addOnly() {
            return false;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return MergeFunctionTestUtils.getExpectedForDeduplicate(input);
        }

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            return DeduplicateMergeFunction.factory().create();
        }
    }

    /** Test for {@link SortMergeReader} with {@link FirstRowMergeFunction}. */
    public static class WithFirstRowMergeFunctionTest extends SortMergeReaderTestBase {

        @Override
        protected boolean addOnly() {
            return true;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return MergeFunctionTestUtils.getExpectedForFirstRow(input);
        }

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            RowType keyType = new RowType(Lists.list(new DataField(0, "f0", new IntType())));
            RowType valueType = new RowType(Lists.list(new DataField(1, "f1", new BigIntType())));
            return new LookupMergeFunction(
                    new FirstRowMergeFunction(keyType, valueType, false), keyType, valueType);
        }
    }
}
