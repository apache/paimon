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
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Regression tests for partial-update buffers that start with a retraction. */
class PartialUpdateFirstRetractTest {

    private static final int PRECISION = 18;
    private static final int SCALE = 6;

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"UPDATE_BEFORE", "DELETE"})
    void testFirstRetractionDoesNotAddExtraAmount(RowKind retractKind) {
        ReducerMergeFunctionWrapper buffer = newReducer();
        buffer.add(record(retractKind, 2, "90"));
        buffer.add(record(RowKind.UPDATE_AFTER, 3, "90"));
        KeyValue delta = buffer.getResult();
        assertThat(delta.value().getDecimal(1, PRECISION, SCALE)).isEqualTo(decimal("0"));
        assertThat(delta.value().getLong(2)).isZero();

        ReducerMergeFunctionWrapper compact = newReducer();
        compact.add(record(RowKind.INSERT, 1, "90"));
        compact.add(delta);
        assertThat(compact.getResult().value())
                .isEqualTo(GenericRow.of(1, decimal("90"), 1L, Timestamp.fromEpochMillis(3)));
    }

    @Test
    void testNotNullAggregatesStartWithNegativeContribution() {
        ReducerMergeFunctionWrapper buffer = newReducer(true);
        buffer.add(record(RowKind.UPDATE_BEFORE, 2, "90"));
        buffer.add(record(RowKind.UPDATE_AFTER, 3, "60"));
        KeyValue delta = buffer.getResult();
        assertThat(delta.value().getDecimal(1, PRECISION, SCALE)).isEqualTo(decimal("-30"));
        assertThat(delta.value().getLong(2)).isZero();
    }

    @Test
    void testConsecutiveRetractionsAreAllCounted() {
        ReducerMergeFunctionWrapper buffer = newReducer();
        buffer.add(record(RowKind.UPDATE_BEFORE, 2, "90"));
        buffer.add(record(RowKind.UPDATE_BEFORE, 3, "40"));
        buffer.add(record(RowKind.UPDATE_AFTER, 4, "90"));
        assertThat(buffer.getResult().value().getDecimal(1, PRECISION, SCALE))
                .isEqualTo(decimal("-40"));
        assertThat(buffer.getResult().value().getLong(2)).isEqualTo(-1L);
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"UPDATE_BEFORE", "DELETE"})
    void testRetractionOnlyResultCanBeMergedAgain(RowKind retractKind) {
        ReducerMergeFunctionWrapper buffer = newReducer();
        buffer.add(record(retractKind, 2, "-50"));
        buffer.add(record(retractKind, 3, "10"));

        KeyValue result = buffer.getResult();
        assertThat(result.valueKind()).isEqualTo(RowKind.DELETE);
        assertThat(result.value())
                .isEqualTo(GenericRow.of(1, decimal("-40"), 2L, Timestamp.fromEpochMillis(3)));

        ReducerMergeFunctionWrapper compact = newReducer();
        compact.add(record(RowKind.INSERT, 1, "-40"));
        compact.add(buffer.getResult());
        assertThat(compact.getResult().value().getDecimal(1, PRECISION, SCALE))
                .isEqualTo(decimal("0"));

        buffer.add(record(RowKind.UPDATE_AFTER, 4, "60"));
        assertThat(buffer.getResult().value())
                .isEqualTo(GenericRow.of(1, decimal("100"), -1L, Timestamp.fromEpochMillis(4)));
    }

    @Test
    void testNullSequenceGroupDoesNotRetractAmount() {
        ReducerMergeFunctionWrapper buffer = newReducer();
        buffer.add(
                new KeyValue()
                        .replace(
                                GenericRow.of(1),
                                2,
                                RowKind.UPDATE_BEFORE,
                                GenericRow.of(1, decimal("90"), 1L, null)));
        buffer.add(record(RowKind.UPDATE_AFTER, 3, "60"));
        assertThat(buffer.getResult().value().getDecimal(1, PRECISION, SCALE))
                .isEqualTo(decimal("60"));
        assertThat(buffer.getResult().value().getLong(2)).isEqualTo(1L);
    }

    @Test
    void testNotNullIgnoreRetractFieldIsPreserved() {
        Options options = sumOptions();
        options.set("fields.f2.aggregate-function", "max");
        options.set("fields.f2.ignore-retract", "true");
        ReducerMergeFunctionWrapper buffer = newReducer(true, options);
        buffer.add(record(RowKind.UPDATE_BEFORE, 2, "90"));
        buffer.add(record(RowKind.UPDATE_BEFORE, 3, "40"));
        assertThat(buffer.getResult().valueKind()).isEqualTo(RowKind.DELETE);
        assertThat(buffer.getResult().value().getDecimal(1, PRECISION, SCALE))
                .isEqualTo(decimal("130"));
        assertThat(buffer.getResult().value().getLong(2)).isEqualTo(1L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIgnoredHistoricalRetractionDoesNotSeedSum(boolean notNullAggregates) {
        Options options = sumOptions();
        options.set("ignore-delete", "true");
        ReducerMergeFunctionWrapper buffer = newReducer(notNullAggregates, options);
        buffer.add(record(RowKind.UPDATE_BEFORE, 2, "90"));
        buffer.add(record(RowKind.UPDATE_AFTER, 3, "90"));
        assertThat(buffer.getResult().value())
                .isEqualTo(GenericRow.of(1, decimal("90"), 1L, Timestamp.fromEpochMillis(3)));

        buffer.add(record(RowKind.DELETE, 4, "40"));
        buffer.add(record(RowKind.UPDATE_AFTER, 5, "60"));
        assertThat(buffer.getResult().value())
                .isEqualTo(GenericRow.of(1, decimal("150"), 2L, Timestamp.fromEpochMillis(5)));
    }

    @Test
    void testOnlyIgnoredHistoricalRetractionsKeepDeletePayload() {
        Options options = sumOptions();
        options.set("ignore-delete", "true");
        ReducerMergeFunctionWrapper buffer = newReducer(true, options);
        buffer.add(record(RowKind.DELETE, 1, "90"));
        buffer.add(record(RowKind.UPDATE_BEFORE, 2, "40"));
        KeyValue result = buffer.getResult();
        assertThat(result.valueKind()).isEqualTo(RowKind.DELETE);
        assertThat(result.value())
                .isEqualTo(GenericRow.of(1, decimal("90"), 1L, Timestamp.fromEpochMillis(1)));
    }

    @Test
    void testRemovingDeleteModeKeepsNullablePayload() {
        Options options = new Options();
        options.set("partial-update.remove-record-on-delete", "true");
        MergeFunction<KeyValue> function =
                PartialUpdateMergeFunction.factory(
                                options,
                                RowType.of(
                                        DataTypes.INT().notNull(),
                                        DataTypes.INT(),
                                        DataTypes.INT()),
                                Collections.singletonList("f0"))
                        .create();
        function.reset();
        function.add(
                new KeyValue()
                        .replace(
                                GenericRow.of(1),
                                1,
                                RowKind.UPDATE_BEFORE,
                                GenericRow.of(1, 10, 20)));
        function.add(
                new KeyValue()
                        .replace(
                                GenericRow.of(1),
                                2,
                                RowKind.UPDATE_AFTER,
                                GenericRow.of(1, 11, null)));
        assertThat(function.getResult().value()).isEqualTo(GenericRow.of(1, 11, 20));
    }

    private ReducerMergeFunctionWrapper newReducer() {
        return newReducer(false);
    }

    private ReducerMergeFunctionWrapper newReducer(boolean notNullAggregates) {
        return newReducer(notNullAggregates, sumOptions());
    }

    private Options sumOptions() {
        Options options = new Options();
        options.set("fields.f3.sequence-group", "f1,f2");
        options.set("fields.f1.aggregate-function", "sum");
        options.set("fields.f2.aggregate-function", "sum");
        return options;
    }

    private ReducerMergeFunctionWrapper newReducer(boolean notNullAggregates, Options options) {
        RowType type =
                RowType.of(
                        DataTypes.INT().notNull(),
                        notNullAggregates
                                ? DataTypes.DECIMAL(PRECISION, SCALE).notNull()
                                : DataTypes.DECIMAL(PRECISION, SCALE),
                        notNullAggregates ? DataTypes.BIGINT().notNull() : DataTypes.BIGINT(),
                        DataTypes.TIMESTAMP(3));
        ReducerMergeFunctionWrapper reducer =
                new ReducerMergeFunctionWrapper(
                        PartialUpdateMergeFunction.factory(
                                        options, type, Collections.singletonList("f0"))
                                .create());
        reducer.reset();
        return reducer;
    }

    private KeyValue record(RowKind kind, long sequence, String amount) {
        return new KeyValue()
                .replace(
                        GenericRow.of(1),
                        sequence,
                        kind,
                        GenericRow.of(1, decimal(amount), 1L, Timestamp.fromEpochMillis(sequence)));
    }

    private Decimal decimal(String amount) {
        return Decimal.fromBigDecimal(new BigDecimal(amount), PRECISION, SCALE);
    }
}
