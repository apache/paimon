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

package org.apache.flink.table.store.file.mergetree.compact.aggregate;

import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.types.BooleanType;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.VarCharType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** test whether {@link FieldAggregator}' subclasses behaviors are expected. */
public class FieldAggregatorTest {

    @Test
    public void testFieldBoolAndAgg() {
        FieldBoolAndAgg fieldBoolAndAgg = new FieldBoolAndAgg(new BooleanType());
        Boolean accumulator = false;
        Boolean inputField = true;
        assertThat(fieldBoolAndAgg.agg(accumulator, inputField)).isEqualTo(false);

        accumulator = true;
        inputField = true;
        assertThat(fieldBoolAndAgg.agg(accumulator, inputField)).isEqualTo(true);
    }

    @Test
    public void testFieldBoolOrAgg() {
        FieldBoolOrAgg fieldBoolOrAgg = new FieldBoolOrAgg(new BooleanType());
        Boolean accumulator = false;
        Boolean inputField = true;
        assertThat(fieldBoolOrAgg.agg(accumulator, inputField)).isEqualTo(true);

        accumulator = false;
        inputField = false;
        assertThat(fieldBoolOrAgg.agg(accumulator, inputField)).isEqualTo(false);
    }

    @Test
    public void testFieldLastNonNullValueAgg() {
        FieldLastNonNullValueAgg fieldLastNonNullValueAgg =
                new FieldLastNonNullValueAgg(new IntType());
        Integer accumulator = null;
        Integer inputField = 1;
        assertThat(fieldLastNonNullValueAgg.agg(accumulator, inputField)).isEqualTo(1);

        accumulator = 1;
        inputField = null;
        assertThat(fieldLastNonNullValueAgg.agg(accumulator, inputField)).isEqualTo(1);
    }

    @Test
    public void testFieldLastValueAgg() {
        FieldLastValueAgg fieldLastValueAgg = new FieldLastValueAgg(new IntType());
        Integer accumulator = null;
        Integer inputField = 1;
        assertThat(fieldLastValueAgg.agg(accumulator, inputField)).isEqualTo(1);

        accumulator = 1;
        inputField = null;
        assertThat(fieldLastValueAgg.agg(accumulator, inputField)).isEqualTo(null);
    }

    @Test
    public void testFieldListaggAgg() {
        FieldListaggAgg fieldListaggAgg = new FieldListaggAgg(new VarCharType());
        BinaryString accumulator = BinaryString.fromString("user1");
        BinaryString inputField = BinaryString.fromString("user2");
        assertThat(fieldListaggAgg.agg(accumulator, inputField).toString())
                .isEqualTo("user1,user2");
    }

    @Test
    public void testFieldMaxAgg() {
        FieldMaxAgg fieldMaxAgg = new FieldMaxAgg(new IntType());
        Integer accumulator = 1;
        Integer inputField = 10;
        assertThat(fieldMaxAgg.agg(accumulator, inputField)).isEqualTo(10);
    }

    @Test
    public void testFieldMinAgg() {
        FieldMinAgg fieldMinAgg = new FieldMinAgg(new IntType());
        Integer accumulator = 1;
        Integer inputField = 10;
        assertThat(fieldMinAgg.agg(accumulator, inputField)).isEqualTo(1);
    }

    @Test
    public void testFieldSumAgg() {
        FieldSumAgg fieldSumAgg = new FieldSumAgg(new IntType());
        Integer accumulator = 1;
        Integer inputField = 10;
        assertThat(fieldSumAgg.agg(accumulator, inputField)).isEqualTo(11);
    }
}
