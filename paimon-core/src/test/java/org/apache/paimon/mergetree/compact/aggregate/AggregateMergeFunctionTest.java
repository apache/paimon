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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.paimon.CoreOptions.FIELDS_DEFAULT_AGG_FUNC;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for aggregate merge function. */
class AggregateMergeFunctionTest {

    @Test
    void testDefaultAggFunc() {
        Options options = new Options();
        options.set(FIELDS_DEFAULT_AGG_FUNC, "first_non_null_value");
        options.set("fields.b.aggregate-function", "sum");
        MergeFunction<KeyValue> aggregateFunction =
                AggregateMergeFunction.factory(
                                options,
                                Arrays.asList("k", "a", "b", "c", "d"),
                                Arrays.asList(
                                        DataTypes.INT(),
                                        DataTypes.INT(),
                                        DataTypes.INT(),
                                        DataTypes.INT(),
                                        DataTypes.INT()),
                                Collections.singletonList("k"))
                        .create();
        aggregateFunction.reset();

        aggregateFunction.add(value(1, null, 1, 1, 1));
        aggregateFunction.add(value(1, 2, null, 2, 2));
        aggregateFunction.add(value(1, 3, 3, null, 3));
        aggregateFunction.add(value(1, 4, 4, 4, null));
        aggregateFunction.add(value(1, 5, 5, 5, 5));
        assertThat(aggregateFunction.getResult().value()).isEqualTo(GenericRow.of(1, 2, 13, 1, 1));
    }

    @Test
    void tesListAggFunc() {
        Options options = new Options();
        options.set("fields.a.aggregate-function", "listagg");
        options.set("fields.b.aggregate-function", "listagg");
        options.set("fields.b.list-agg-delimiter", "-");
        options.set("fields.d.aggregate-function", "listagg");
        options.set("fields.d.list-agg-delimiter", "/");

        MergeFunction<KeyValue> aggregateFunction =
                AggregateMergeFunction.factory(
                                options,
                                Arrays.asList("k", "a", "b", "c", "d"),
                                Arrays.asList(
                                        DataTypes.INT(),
                                        DataTypes.STRING(),
                                        DataTypes.STRING(),
                                        DataTypes.INT(),
                                        DataTypes.STRING()),
                                Collections.singletonList("k"))
                        .create();
        aggregateFunction.reset();

        aggregateFunction.add(
                value(
                        1,
                        BinaryString.fromString("1"),
                        BinaryString.fromString("1"),
                        1,
                        BinaryString.fromString("1")));
        aggregateFunction.add(
                value(
                        1,
                        BinaryString.fromString("2"),
                        BinaryString.fromString("2"),
                        2,
                        BinaryString.fromString("2")));
        aggregateFunction.add(
                value(
                        1,
                        BinaryString.fromString("3"),
                        BinaryString.fromString("3"),
                        3,
                        BinaryString.fromString("3")));
        aggregateFunction.add(
                value(
                        1,
                        BinaryString.fromString("4"),
                        BinaryString.fromString("4"),
                        4,
                        BinaryString.fromString("4")));
        aggregateFunction.add(
                value(
                        1,
                        BinaryString.fromString("5"),
                        BinaryString.fromString("5"),
                        5,
                        BinaryString.fromString("5")));
        assertThat(aggregateFunction.getResult().value())
                .isEqualTo(
                        GenericRow.of(
                                1,
                                BinaryString.fromString("1,2,3,4,5"),
                                BinaryString.fromString("1-2-3-4-5"),
                                5,
                                BinaryString.fromString("1/2/3/4/5")));
    }

    private KeyValue value(Integer... values) {
        return new KeyValue()
                .replace(GenericRow.of(values[0]), RowKind.INSERT, GenericRow.of(values));
    }

    private KeyValue value(
            Integer value1,
            BinaryString value2,
            BinaryString value3,
            Integer value4,
            BinaryString value5) {
        return new KeyValue()
                .replace(
                        GenericRow.of(value1),
                        RowKind.INSERT,
                        GenericRow.of(value1, value2, value3, value4, value5));
    }
}
