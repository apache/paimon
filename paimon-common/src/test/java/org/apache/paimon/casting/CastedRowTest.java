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

package org.apache.paimon.casting;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link CastedRow}. */
class CastedRowTest {

    @Test
    void testNullabilityAfterDecimalOverflow() {
        GenericRow source = GenericRow.of(1000);
        CastedRow row = createCastedRow(DataTypes.INT(), DataTypes.DECIMAL(3, 0));
        row.replaceRow(source);

        assertThat(source.isNullAt(0)).isFalse();
        assertThat(row.getDecimal(0, 3, 0)).isNull();
        assertThat(row.isNullAt(0)).isTrue();
    }

    @Test
    void testCopyAfterDecimalOverflow() {
        GenericRow source = GenericRow.of(1000);
        source.setRowKind(RowKind.UPDATE_AFTER);
        DataType targetType = DataTypes.DECIMAL(3, 0);
        CastedRow row = createCastedRow(DataTypes.INT(), targetType).replaceRow(source);
        InternalRowSerializer serializer = new InternalRowSerializer(targetType);

        assertThatCode(
                        () -> {
                            InternalRow copy = serializer.copy(row);
                            assertThat(copy.isNullAt(0)).isTrue();
                            assertThat(copy.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);
                        })
                .doesNotThrowAnyException();
    }

    @Test
    void testNullabilityAfterDecimalRoundingOverflow() {
        Decimal value = Decimal.fromBigDecimal(new BigDecimal("99.99"), 4, 2);
        CastedRow row =
                createCastedRow(DataTypes.DECIMAL(4, 2), DataTypes.DECIMAL(3, 1))
                        .replaceRow(GenericRow.of(value));

        assertThat(row.getDecimal(0, 3, 1)).isNull();
        assertThat(row.isNullAt(0)).isTrue();
    }

    @Test
    void testNullabilityAfterNonCompactDecimalOverflow() {
        Decimal value = Decimal.fromBigDecimal(new BigDecimal("100000000000000000000"), 21, 0);
        CastedRow row =
                createCastedRow(DataTypes.DECIMAL(21, 0), DataTypes.DECIMAL(19, 0))
                        .replaceRow(GenericRow.of(value));

        assertThat(row.getDecimal(0, 19, 0)).isNull();
        assertThat(row.isNullAt(0)).isTrue();
    }

    @Test
    void testNullabilityWhenReplacingRow() {
        CastedRow row = createCastedRow(DataTypes.INT(), DataTypes.DECIMAL(3, 0));

        row.replaceRow(new GenericRow(1));
        assertThat(row.isNullAt(0)).isTrue();

        row.replaceRow(GenericRow.of(42));
        assertThat(row.isNullAt(0)).isFalse();
        assertThat(row.getDecimal(0, 3, 0).toBigDecimal()).isEqualByComparingTo("42");

        row.replaceRow(GenericRow.of(1000));
        assertThat(row.isNullAt(0)).isTrue();

        row.replaceRow(GenericRow.of(-42));
        assertThat(row.isNullAt(0)).isFalse();
        assertThat(row.getDecimal(0, 3, 0).toBigDecimal()).isEqualByComparingTo("-42");
    }

    private static CastedRow createCastedRow(DataType sourceType, DataType targetType) {
        return CastedRow.from(
                new CastFieldGetter[] {
                    new CastFieldGetter(
                            InternalRow.createFieldGetter(sourceType, 0),
                            CastExecutors.resolve(sourceType, targetType),
                            "amount")
                });
    }
}
