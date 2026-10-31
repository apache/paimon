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

package org.apache.paimon.utils;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests generation and NULL-safe validation before partition routing. */
class GeneratedColumnRowTest {

    private final RowType rowType =
            RowType.of(
                    new DataField(0, "ts", DataTypes.TIMESTAMP(9)),
                    new DataField(1, "dt", DataTypes.DATE())
                            .newGenerationExpression("CAST(ts AS DATE)"),
                    new DataField(2, "id", DataTypes.INT()));

    @Test
    void testOmittedPartitionAndReorderedInput() {
        RowType inputType = RowType.of(rowType.getField("id"), rowType.getField("ts"));
        GeneratedColumnRow generator = new GeneratedColumnRow(rowType, inputType);
        InternalRow row =
                generator.replaceRow(
                        GenericRow.of(
                                1,
                                Timestamp.fromLocalDateTime(
                                        LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999999))));
        assertThat(row.getInt(1)).isEqualTo(-1);
        assertThat(row.getInt(2)).isEqualTo(1);
        assertThat(generator.replaceRow(GenericRow.of(2, null)).isNullAt(1)).isTrue();
    }

    @Test
    void testExplicitValuesAndNulls() {
        GeneratedColumnRow generator = new GeneratedColumnRow(rowType, rowType);
        Timestamp timestamp = Timestamp.fromEpochMillis(0);
        assertThat(generator.replaceRow(GenericRow.of(timestamp, 0, 1)).getInt(1)).isZero();
        assertThatThrownBy(() -> generator.replaceRow(GenericRow.of(timestamp, 1, 1)))
                .hasMessageContaining("does not match generation expression");
        assertThatThrownBy(() -> generator.replaceRow(GenericRow.of(timestamp, null, 1)))
                .hasMessageContaining("does not match generation expression");
        assertThatThrownBy(() -> generator.replaceRow(GenericRow.of(null, 0, 1)))
                .hasMessageContaining("does not match generation expression");
        assertThat(generator.replaceRow(GenericRow.of(null, null, 1)).isNullAt(1)).isTrue();
    }

    @Test
    void testPreservesDeletePartitionAndRowKind() {
        GenericRow delete = GenericRow.of(null, 42, 1);
        delete.setRowKind(RowKind.DELETE);
        InternalRow result = new GeneratedColumnRow(rowType, rowType).replaceRow(delete);
        assertThat(result.getRowKind()).isEqualTo(RowKind.DELETE);
        assertThat(result.getInt(1)).isEqualTo(42);
    }

    @Test
    void testOnlyGeneratedFieldsMayBeOmitted() {
        assertThatThrownBy(
                        () -> new GeneratedColumnRow(rowType, RowType.of(rowType.getField("ts"))))
                .hasMessageContaining("Missing input column 'id'");
    }

    @Test
    void testUnsupportedExpressionsAndTypes() {
        for (String expression :
                Arrays.asList("current_date", "CAST(id AS DATE)", "CAST(missing AS DATE)")) {
            RowType invalid =
                    rowType.copy(
                            Arrays.asList(
                                    rowType.getField("ts"),
                                    rowType.getField("dt").newGenerationExpression(expression),
                                    rowType.getField("id")));
            assertThatThrownBy(() -> new GeneratedColumnRow(invalid, invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        RowType zoned =
                rowType.copy(
                        Arrays.asList(
                                rowType.getField("ts").newType(DataTypes.TIMESTAMP_LTZ_MILLIS()),
                                rowType.getField("dt"),
                                rowType.getField("id")));
        assertThatThrownBy(() -> new GeneratedColumnRow(zoned, zoned))
                .hasMessageContaining("TIMESTAMP WITHOUT TIME ZONE");
    }
}
