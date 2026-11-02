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
import org.apache.paimon.data.GenericArray;
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

/** Tests omitted-column generation and preservation of supplied values before partition routing. */
class GeneratedColumnRowTest {

    @Test
    void testRejectTimeZoneDependentCastsInsideTopLevelComplexFields() {
        for (org.apache.paimon.types.DataType sourceType :
                Arrays.asList(
                        DataTypes.ROW(new DataField(0, "ts", DataTypes.TIMESTAMP_LTZ_MILLIS())),
                        DataTypes.ARRAY(DataTypes.TIMESTAMP_LTZ_MILLIS()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.TIMESTAMP_LTZ_MILLIS()),
                        DataTypes.MULTISET(DataTypes.TIMESTAMP_LTZ_MILLIS()))) {
            RowType type =
                    RowType.of(
                            new DataField(0, "source", sourceType),
                            new DataField(1, "text", DataTypes.STRING())
                                    .newGenerationExpression("CAST(source AS STRING)"));
            assertThatThrownBy(() -> GeneratedColumn.create(type, type.getField("text")))
                    .hasMessageContaining("TIMESTAMP WITH LOCAL TIME ZONE");
        }
    }

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
        assertThat(generator.replaceRow(GenericRow.of(timestamp, 1, 1)).getInt(1)).isEqualTo(1);
        assertThat(generator.replaceRow(GenericRow.of(timestamp, null, 1)).isNullAt(1)).isTrue();
        assertThat(generator.replaceRow(GenericRow.of(null, 0, 1)).getInt(1)).isZero();
        assertThat(generator.replaceRow(GenericRow.of(null, null, 1)).isNullAt(1)).isTrue();
    }

    @Test
    void testGenerateOnlyOmittedColumnsWithoutEvaluatingSuppliedExpressions() {
        RowType type =
                RowType.of(
                        new DataField(0, "text", DataTypes.STRING()),
                        new DataField(1, "number", DataTypes.INT())
                                .newGenerationExpression("CAST(text AS INT)"),
                        new DataField(2, "label", DataTypes.STRING())
                                .newGenerationExpression("UPPER(text)"));
        InternalRow row =
                new GeneratedColumnRow(type, type.project("number", "text"))
                        .replaceRow(GenericRow.of(42, BinaryString.fromString("invalid")));
        assertThat(row.getInt(1)).isEqualTo(42);
        assertThat(row.getString(2).toString()).isEqualTo("INVALID");
        assertThat(
                        new GeneratedColumnRow(type, type)
                                .replaceRow(
                                        GenericRow.of(
                                                BinaryString.fromString("invalid"), null, null))
                                .isNullAt(1))
                .isTrue();
        assertThatThrownBy(
                        () ->
                                new GeneratedColumnRow(type, type.project("text"))
                                        .replaceRow(
                                                GenericRow.of(BinaryString.fromString("invalid"))))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void testComplexIdentityAndSuppliedValuesWithDifferentFieldIds() {
        RowType sourceType =
                RowType.of(new DataField(10, "values", DataTypes.ARRAY(DataTypes.INT())));
        RowType targetType =
                RowType.of(new DataField(20, "values", DataTypes.ARRAY(DataTypes.INT())));
        RowType type =
                RowType.of(
                        new DataField(0, "source", sourceType),
                        new DataField(1, "copy", targetType).newGenerationExpression("source"));
        GenericRow source = GenericRow.of(new GenericArray(new int[] {1, 2}));
        InternalRow generated =
                new GeneratedColumnRow(type, type.project("source"))
                        .replaceRow(GenericRow.of(source));
        assertThat(generated.getRow(1, 1).getArray(0).getInt(1)).isEqualTo(2);
        GenericRow supplied = GenericRow.of(new GenericArray(new int[] {7}));
        InternalRow explicit =
                new GeneratedColumnRow(type, type).replaceRow(GenericRow.of(source, supplied));
        assertThat(explicit.getRow(1, 1).getArray(0).getInt(0)).isEqualTo(7);
        assertThat(explicit.getRow(1, 1).getArray(0).size()).isEqualTo(1);
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
        assertThat(new GeneratedColumnRow(type, type).replaceRow(wrong).getString(3).toString())
                .isEqualTo("wrong");
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
            assertThatThrownBy(() -> GeneratedColumn.create(invalid, invalid.getField("dt")))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        RowType zoned =
                rowType.copy(
                        Arrays.asList(
                                rowType.getField("ts").newType(DataTypes.TIMESTAMP_LTZ_MILLIS()),
                                rowType.getField("dt"),
                                rowType.getField("id")));
        assertThatThrownBy(() -> GeneratedColumn.create(zoned, zoned.getField("dt")))
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
    void testIdentityGeneratesOmittedBinaryValuesAndPreservesSuppliedValues() {
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
        assertThat(generator.replaceRow(GenericRow.of(new byte[] {1}, new byte[] {2})).getBinary(1))
                .containsExactly(2);
        assertThat(
                        new GeneratedColumnRow(type, type.project("bytes"))
                                .replaceRow(GenericRow.of(new byte[] {1, 2}))
                                .getBinary(1))
                .containsExactly(1, 2);
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
        assertThatThrownBy(() -> GeneratedColumn.create(type, type.getField("year")))
                .hasMessageContaining("cannot reference generated column 'dt'");
        RowType self =
                RowType.of(
                        new DataField(0, "copy", DataTypes.INT()).newGenerationExpression("copy"));
        assertThatThrownBy(() -> GeneratedColumn.create(self, self.getField("copy")))
                .hasMessageContaining("cannot reference generated column 'copy'");
    }

    @Test
    void testSqlTwoArgumentTrimOrder() {
        RowType type =
                RowType.of(
                        new DataField(0, "name", DataTypes.STRING()),
                        new DataField(1, "trimmed", DataTypes.STRING())
                                .newGenerationExpression("TRIM('x', name)"),
                        new DataField(2, "left_trimmed", DataTypes.STRING())
                                .newGenerationExpression("LTRIM('x', name)"),
                        new DataField(3, "right_trimmed", DataTypes.STRING())
                                .newGenerationExpression("RTRIM('x', name)"));
        GeneratedColumnRow generator = new GeneratedColumnRow(type, type.project("name"));
        InternalRow row = generator.replaceRow(GenericRow.of(BinaryString.fromString("xabcxx")));
        assertThat(row.getString(1).toString()).isEqualTo("abc");
        assertThat(row.getString(2).toString()).isEqualTo("abcxx");
        assertThat(row.getString(3).toString()).isEqualTo("xabc");
        assertThat(new GeneratedColumnRow(type, type).replaceRow(row).getString(1).toString())
                .isEqualTo("abc");
        InternalRow nulls = generator.replaceRow(GenericRow.of((Object) null));
        assertThat(nulls.isNullAt(1)).isTrue();
        assertThat(nulls.isNullAt(2)).isTrue();
        assertThat(nulls.isNullAt(3)).isTrue();
    }

    @Test
    void testTimestampNtzCastAlias() {
        RowType type =
                RowType.of(
                        new DataField(0, "event_date", DataTypes.DATE()),
                        new DataField(1, "event_time", DataTypes.TIMESTAMP(6))
                                .newGenerationExpression("cast(event_date as timestamp_ntz)"));
        GeneratedColumnRow generator = new GeneratedColumnRow(type, type.project("event_date"));
        assertThat(generator.replaceRow(GenericRow.of(0)).getTimestamp(1, 6))
                .isEqualTo(Timestamp.fromEpochMillis(0));
        assertThat(generator.replaceRow(GenericRow.of(-1)).getTimestamp(1, 6))
                .isEqualTo(Timestamp.fromEpochMillis(-86400000L));
        assertThat(generator.replaceRow(GenericRow.of((Object) null)).isNullAt(1)).isTrue();
        assertThat(
                        new GeneratedColumnRow(type, type)
                                .replaceRow(GenericRow.of(0, Timestamp.fromEpochMillis(0)))
                                .getTimestamp(1, 6))
                .isEqualTo(Timestamp.fromEpochMillis(0));
        assertThat(
                        new GeneratedColumnRow(type, type)
                                .replaceRow(GenericRow.of(0, Timestamp.fromEpochMillis(1)))
                                .getTimestamp(1, 6))
                .isEqualTo(Timestamp.fromEpochMillis(1));
    }
}
