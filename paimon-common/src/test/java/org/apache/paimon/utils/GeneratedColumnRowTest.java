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

import org.apache.paimon.data.BinaryString;
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
    void testTransformsAndNestedCallsOnTopLevelFields() {
        RowType type =
                RowType.of(
                        new DataField(0, "name", DataTypes.STRING()),
                        new DataField(1, "ts", DataTypes.TIMESTAMP(9)),
                        new DataField(2, "days", DataTypes.INT()),
                        new DataField(3, "label", DataTypes.STRING())
                                .newGenerationExpression(
                                        "CONCAT(UPPER(name), '-', SUBSTRING(name, 2, 2))"),
                        new DataField(4, "year", DataTypes.INT())
                                .newGenerationExpression("YEAR(ts)"),
                        new DataField(5, "date", DataTypes.DATE())
                                .newGenerationExpression("DATE_ADD(CAST(ts AS DATE), days)"),
                        new DataField(6, "wide", DataTypes.BIGINT())
                                .newGenerationExpression("CAST(days AS BIGINT)"));
        GeneratedColumnRow generator =
                new GeneratedColumnRow(type, type.project("days", "ts", "name"));
        InternalRow result =
                generator.replaceRow(
                        GenericRow.of(
                                2, Timestamp.fromEpochMillis(-1), BinaryString.fromString("abc")));
        assertThat(result.getString(3).toString()).isEqualTo("ABC-bc");
        assertThat(result.getInt(4)).isEqualTo(1969);
        assertThat(result.getInt(5)).isEqualTo(1);
        assertThat(result.getLong(6)).isEqualTo(2L);
        assertThat(new GeneratedColumnRow(type, type).replaceRow(result).getInt(5)).isEqualTo(1);
        GenericRow wrong =
                GenericRow.of(
                        BinaryString.fromString("abc"),
                        Timestamp.fromEpochMillis(-1),
                        2,
                        BinaryString.fromString("wrong"),
                        1969,
                        1,
                        2L);
        assertThatThrownBy(() -> new GeneratedColumnRow(type, type).replaceRow(wrong))
                .hasMessageContaining("does not match generation expression");
        InternalRow nulls = generator.replaceRow(GenericRow.of(null, null, null));
        assertThat(nulls.isNullAt(3)).isTrue();
        assertThat(nulls.isNullAt(4)).isTrue();
        assertThat(nulls.isNullAt(5)).isTrue();
        assertThat(nulls.isNullAt(6)).isTrue();
    }

    @Test
    void testUnsupportedExpressionsAndTypes() {
        for (String expression :
                Arrays.asList(
                        "current_date",
                        "CAST(id AS DATE)",
                        "CAST(missing AS DATE)",
                        "RAND()",
                        "CAST(ts AS DATE) trailing",
                        "ts.value",
                        "UPPER(ts)",
                        "YEAR(ts, id)",
                        "CONCAT(id, 'x')",
                        "YEAR(dt)",
                        "SUBSTRING(ts, 1)",
                        "SUBSTRING('x', 'bad')")) {
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
                .hasMessageContaining("TIMESTAMP WITH LOCAL TIME ZONE");
    }

    @Test
    void testNormalizeAllReferencesWithoutChangingLiterals() {
        RowType type = RowType.of(new DataField(0, "name", DataTypes.STRING()));
        String expression = "CONCAT(UPPER(`NAME`), 'NAME''s', NAME) ";
        String normalized =
                GeneratedColumn.normalizeExpression(expression, type, String::toLowerCase);
        RowType generated =
                RowType.of(
                        type.getField("name"),
                        new DataField(1, "label", DataTypes.STRING())
                                .newGenerationExpression(normalized));
        InternalRow row =
                new GeneratedColumnRow(generated, type)
                        .replaceRow(GenericRow.of(BinaryString.fromString("ab")));
        assertThat(row.getString(1).toString()).isEqualTo("ABNAME'sab");
        assertThat(GeneratedColumn.create(generated, generated.getField("label")).sourceNames())
                .containsExactly("name");
    }

    @Test
    void testIdentityUsesTypedEqualityForBinaryValues() {
        RowType type =
                RowType.of(
                        new DataField(0, "bytes", DataTypes.BYTES()),
                        new DataField(1, "copy", DataTypes.BYTES())
                                .newGenerationExpression("bytes"));
        GeneratedColumnRow generator = new GeneratedColumnRow(type, type);
        assertThat(
                        generator
                                .replaceRow(GenericRow.of(new byte[] {1, 2}, new byte[] {1, 2}))
                                .getBinary(1))
                .containsExactly(1, 2);
        assertThatThrownBy(
                        () -> generator.replaceRow(GenericRow.of(new byte[] {1}, new byte[] {2})))
                .hasMessageContaining("does not match generation expression");
    }

    @Test
    void testDateAndStringLiteralArguments() {
        RowType type =
                RowType.of(
                        new DataField(0, "days", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.STRING()),
                        new DataField(2, "date", DataTypes.DATE())
                                .newGenerationExpression("DATE_ADD(DATE '1970-01-01', days)"),
                        new DataField(3, "label", DataTypes.STRING())
                                .newGenerationExpression("LPAD(SUBSTRING(name, 1, 2), 4, '_')"));
        InternalRow row =
                new GeneratedColumnRow(type, type.project("days", "name"))
                        .replaceRow(GenericRow.of(2, BinaryString.fromString("abc")));
        assertThat(row.getInt(2)).isEqualTo(2);
        assertThat(row.getString(3).toString()).isEqualTo("__ab");
    }

    @Test
    void testRejectGeneratedDependenciesWithMatchingTypes() {
        RowType type =
                RowType.of(
                        rowType.getField("ts"),
                        rowType.getField("dt"),
                        new DataField(3, "year", DataTypes.INT())
                                .newGenerationExpression("YEAR(dt)"));
        assertThatThrownBy(() -> new GeneratedColumnRow(type, type))
                .hasMessageContaining("cannot reference generated column 'dt'");
        RowType self =
                RowType.of(
                        new DataField(0, "copy", DataTypes.INT()).newGenerationExpression("copy"));
        assertThatThrownBy(() -> new GeneratedColumnRow(self, self))
                .hasMessageContaining("cannot reference generated column 'copy'");
    }
}
