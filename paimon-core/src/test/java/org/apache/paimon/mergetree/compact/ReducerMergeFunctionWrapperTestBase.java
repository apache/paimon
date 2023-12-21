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
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.paimon.io.DataFileTestUtils.row;

/** Tests for {@link ReducerMergeFunctionWrapper}. */
public abstract class ReducerMergeFunctionWrapperTestBase {

    protected ReducerMergeFunctionWrapper wrapper;

    protected abstract MergeFunction<KeyValue> createMergeFunction();

    protected abstract KeyValue getExpected(List<KeyValue> kvs);

    @BeforeEach
    void setUp() {
        wrapper = new ReducerMergeFunctionWrapper(createMergeFunction());
    }

    @MethodSource("provideKeyValuesToMerge")
    @ParameterizedTest
    public void testReducerMergeFunctionWrapper(List<KeyValue> kvs) {
        KeyValue expectedKv = getExpected(kvs);
        kvs.forEach(kv -> wrapper.add(kv));
        KeyValue mergedKv = wrapper.getResult();
        MergeFunctionTestUtils.assertKvEquals(expectedKv, mergedKv);
    }

    public static Stream<Arguments> provideKeyValuesToMerge() {
        return Stream.of(
                Arguments.of(
                        Collections.singletonList(
                                new KeyValue().replace(row(1), 1, RowKind.INSERT, row(1)))),
                Arguments.of(
                        Arrays.asList(
                                new KeyValue().replace(row(2), 2, RowKind.INSERT, row(-1)),
                                new KeyValue().replace(row(2), 3, RowKind.INSERT, row(1)))),
                Arguments.of(
                        Arrays.asList(
                                new KeyValue().replace(row(3), 4, RowKind.INSERT, row(1)),
                                new KeyValue().replace(row(3), 5, RowKind.INSERT, row(2)))));
    }

    /** Tests for {@link ReducerMergeFunctionWrapper} with {@link DeduplicateMergeFunction}. */
    public static class WithDeduplicateMergeFunctionTest
            extends ReducerMergeFunctionWrapperTestBase {

        @Override
        protected MergeFunction<KeyValue> createMergeFunction() {
            return DeduplicateMergeFunction.factory().create();
        }

        @Override
        protected KeyValue getExpected(List<KeyValue> kvs) {
            return kvs.get(kvs.size() - 1);
        }
    }
}
