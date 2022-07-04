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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.store.file.data.DataFileTestUtils.row;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link MergeFunctionHelper}. */
public abstract class MergeFunctionHelperTestBase {

    protected MergeFunctionHelper mergeFunctionHelper;

    protected abstract MergeFunction createMergeFunction();

    protected abstract RowData getExpected(List<RowData> rows);

    @BeforeEach
    void setUp() {
        mergeFunctionHelper = new MergeFunctionHelper(createMergeFunction());
    }

    @MethodSource("provideMergedRowData")
    @ParameterizedTest
    public void testMergeFunctionHelper(List<RowData> rows) {
        rows.forEach(r -> mergeFunctionHelper.add(r));
        assertEquals(getExpected(rows), mergeFunctionHelper.getValue());
    }

    public static Stream<Arguments> provideMergedRowData() {
        return Stream.of(
                Arguments.of(Collections.singletonList(row(1))),
                Arguments.of(Arrays.asList(row(-1), row(1))),
                Arguments.of(Arrays.asList(row(1), row(2))));
    }

    /** Tests for {@link MergeFunctionHelper} with {@link DeduplicateMergeFunction}. */
    public static class WithDeduplicateMergeFunctionTest extends MergeFunctionHelperTestBase {

        @Override
        protected MergeFunction createMergeFunction() {
            return new DeduplicateMergeFunction();
        }

        @Override
        protected RowData getExpected(List<RowData> rows) {
            return rows.get(rows.size() - 1);
        }
    }

    /** Tests for {@link MergeFunctionHelper} with {@link ValueCountMergeFunction}. */
    public static class WithValueRecordMergeFunctionTest extends MergeFunctionHelperTestBase {

        @Override
        protected MergeFunction createMergeFunction() {
            return new ValueCountMergeFunction();
        }

        @Override
        protected RowData getExpected(List<RowData> rows) {
            if (rows.size() == 1) {
                return rows.get(0);
            } else {
                long total = rows.stream().mapToLong(r -> r.getLong(0)).sum();
                return total == 0 ? null : GenericRowData.of(total);
            }
        }
    }
}
