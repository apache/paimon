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
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
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
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests generation and NULL-safe validation before partition routing. */
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
            assertThatThrownBy(() -> new GeneratedColumnRow(type, type))
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

    @Test
    void testExplicitFloatingValuesUseSqlEquality() {
        RowType type =
                RowType.of(
                        new DataField(0, "float_value", DataTypes.FLOAT()),
                        new DataField(1, "float_copy", DataTypes.FLOAT())
                                .newGenerationExpression("float_value"),
                        new DataField(2, "double_value", DataTypes.DOUBLE()),
                        new DataField(3, "double_copy", DataTypes.DOUBLE())
                                .newGenerationExpression("double_value"));
        GeneratedColumnRow generator = new GeneratedColumnRow(type, type);
        InternalRow negativeZero = generator.replaceRow(GenericRow.of(-0.0f, 0.0f, -0.0d, 0.0d));
        assertThat(Float.floatToRawIntBits(negativeZero.getFloat(1)))
                .isEqualTo(Float.floatToRawIntBits(-0.0f));
        assertThat(Double.doubleToRawLongBits(negativeZero.getDouble(3)))
                .isEqualTo(Double.doubleToRawLongBits(-0.0d));
        InternalRow positiveZero = generator.replaceRow(GenericRow.of(0.0f, -0.0f, 0.0d, -0.0d));
        assertThat(Float.floatToRawIntBits(positiveZero.getFloat(1)))
                .isEqualTo(Float.floatToRawIntBits(0.0f));
        assertThat(Double.doubleToRawLongBits(positiveZero.getDouble(3)))
                .isEqualTo(Double.doubleToRawLongBits(0.0d));

        InternalRow nan =
                generator.replaceRow(
                        GenericRow.of(
                                Float.intBitsToFloat(0x7fc00001),
                                Float.NaN,
                                Double.longBitsToDouble(0x7ff8000000000001L),
                                Double.NaN));
        assertThat(nan.getFloat(1)).isNaN();
        assertThat(nan.getDouble(3)).isNaN();
        assertThatThrownBy(() -> generator.replaceRow(GenericRow.of(0.0f, 1.0f, 0.0d, 0.0d)))
                .hasMessageContaining("float_copy");
        assertThatThrownBy(() -> generator.replaceRow(GenericRow.of(0.0f, 0.0f, 0.0d, 1.0d)))
                .hasMessageContaining("double_copy");
        assertThatThrownBy(() -> generator.replaceRow(GenericRow.of(Float.NaN, 1.0f, 0.0d, 0.0d)))
                .hasMessageContaining("float_copy");
        assertThatThrownBy(() -> generator.replaceRow(GenericRow.of(0.0f, 0.0f, 1.0d, Double.NaN)))
                .hasMessageContaining("double_copy");
    }

    @Test
    void testExplicitArrayUsesRecursiveSqlEquality() {
        RowType type =
                RowType.of(
                        new DataField(0, "samples", DataTypes.ARRAY(DataTypes.DOUBLE())),
                        new DataField(1, "copy", DataTypes.ARRAY(DataTypes.DOUBLE()))
                                .newGenerationExpression("samples"));
        GeneratedColumnRow generator = new GeneratedColumnRow(type, type);
        GenericArray source =
                new GenericArray(
                        new Object[] {
                            -0.0d, Double.longBitsToDouble(0x7ff8000000000001L), null, 7.0d
                        });
        GenericArray supplied = new GenericArray(new Object[] {0.0d, Double.NaN, null, 7.0d});
        InternalRow row = generator.replaceRow(GenericRow.of(source, supplied));
        assertThat(Double.doubleToRawLongBits(row.getArray(1).getDouble(0)))
                .isEqualTo(Double.doubleToRawLongBits(-0.0d));
        assertThat(row.getArray(1).getDouble(1)).isNaN();
        assertThat(row.getArray(1).isNullAt(2)).isTrue();
        assertThat(row.getArray(1).getDouble(3)).isEqualTo(7.0d);
        for (GenericArray wrong :
                Arrays.asList(
                        new GenericArray(new Object[] {0.0d, Double.NaN, null, 8.0d}),
                        new GenericArray(new Object[] {0.0d, Double.NaN, 0.0d, 7.0d}),
                        new GenericArray(new Object[] {0.0d, Double.NaN, null}))) {
            assertThatThrownBy(() -> generator.replaceRow(GenericRow.of(source, wrong)))
                    .hasMessageContaining("does not match generation expression");
        }
        assertThat(generator.replaceRow(GenericRow.of(null, null)).isNullAt(1)).isTrue();
        assertThatThrownBy(() -> generator.replaceRow(GenericRow.of(source, null)))
                .hasMessageContaining("does not match generation expression");
        assertThatThrownBy(() -> generator.replaceRow(GenericRow.of(null, supplied)))
                .hasMessageContaining("does not match generation expression");
    }

    @Test
    void testExplicitRowUsesRecursiveSqlEquality() {
        RowType sourceType =
                RowType.of(
                        new DataField(10, "rate", DataTypes.FLOAT()),
                        new DataField(11, "offset", DataTypes.DOUBLE()),
                        new DataField(12, "samples", DataTypes.ARRAY(DataTypes.DOUBLE())));
        RowType targetType =
                RowType.of(
                        new DataField(20, "rate", DataTypes.FLOAT()),
                        new DataField(21, "offset", DataTypes.DOUBLE()),
                        new DataField(22, "samples", DataTypes.ARRAY(DataTypes.DOUBLE())));
        RowType type =
                RowType.of(
                        new DataField(0, "reading", sourceType),
                        new DataField(1, "copy", targetType).newGenerationExpression("reading"));
        GeneratedColumnRow generator = new GeneratedColumnRow(type, type);
        GenericRow source =
                GenericRow.of(-0.0f, Double.NaN, new GenericArray(new Object[] {-0.0d, null}));
        GenericRow supplied =
                GenericRow.of(
                        0.0f,
                        Double.longBitsToDouble(0x7ff8000000000001L),
                        new GenericArray(new Object[] {0.0d, null}));
        InternalRow row = generator.replaceRow(GenericRow.of(source, supplied));
        assertThat(Float.floatToRawIntBits(row.getRow(1, 3).getFloat(0)))
                .isEqualTo(Float.floatToRawIntBits(-0.0f));
        assertThat(row.getRow(1, 3).getDouble(1)).isNaN();
        assertThat(row.getRow(1, 3).getArray(2).isNullAt(1)).isTrue();
        assertThatThrownBy(
                        () ->
                                generator.replaceRow(
                                        GenericRow.of(
                                                source,
                                                GenericRow.of(
                                                        0.0f,
                                                        1.0d,
                                                        new GenericArray(
                                                                new Object[] {0.0d, null})))))
                .hasMessageContaining("does not match generation expression");
        assertThatThrownBy(
                        () ->
                                generator.replaceRow(
                                        GenericRow.of(
                                                source,
                                                GenericRow.of(
                                                        0.0f,
                                                        Double.NaN,
                                                        new GenericArray(
                                                                new Object[] {0.0d, 1.0d})))))
                .hasMessageContaining("does not match generation expression");
    }

    @Test
    void testExplicitMapUsesRecursiveSqlEqualityAndIgnoresEntryOrder() {
        RowType type =
                RowType.of(
                        new DataField(
                                0, "weights", DataTypes.MAP(DataTypes.DOUBLE(), DataTypes.FLOAT())),
                        new DataField(
                                        1,
                                        "copy",
                                        DataTypes.MAP(DataTypes.DOUBLE(), DataTypes.FLOAT()))
                                .newGenerationExpression("weights"));
        GeneratedColumnRow generator = new GeneratedColumnRow(type, type);
        Map<Double, Float> source = new LinkedHashMap<>();
        source.put(-0.0d, Float.intBitsToFloat(0x7fc00001));
        source.put(2.0d, -0.0f);
        source.put(3.0d, null);
        Map<Double, Float> supplied = new LinkedHashMap<>();
        supplied.put(3.0d, null);
        supplied.put(2.0d, 0.0f);
        supplied.put(0.0d, Float.NaN);
        InternalRow row =
                generator.replaceRow(
                        GenericRow.of(new GenericMap(source), new GenericMap(supplied)));
        assertThat(row.getMap(1).size()).isEqualTo(3);
        assertThat(Double.doubleToRawLongBits(row.getMap(1).keyArray().getDouble(0)))
                .isEqualTo(Double.doubleToRawLongBits(-0.0d));
        assertThat(row.getMap(1).valueArray().getFloat(0)).isNaN();
        assertThat(row.getMap(1).valueArray().isNullAt(2)).isTrue();
        Map<Double, Float> wrongValue = new LinkedHashMap<>(supplied);
        wrongValue.put(2.0d, 1.0f);
        Map<Double, Float> wrongKey = new LinkedHashMap<>(supplied);
        wrongKey.remove(0.0d);
        wrongKey.put(1.0d, Float.NaN);
        Map<Double, Float> missingEntry = new LinkedHashMap<>(supplied);
        missingEntry.remove(3.0d);
        for (Map<Double, Float> wrong : Arrays.asList(wrongValue, wrongKey, missingEntry)) {
            assertThatThrownBy(
                            () ->
                                    generator.replaceRow(
                                            GenericRow.of(
                                                    new GenericMap(source), new GenericMap(wrong))))
                    .hasMessageContaining("does not match generation expression");
        }
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
    void testExplicitVectorUsesRecursiveSqlEquality() {
        RowType type =
                RowType.of(
                        new DataField(0, "embedding", DataTypes.VECTOR(3, DataTypes.DOUBLE())),
                        new DataField(1, "copy", DataTypes.VECTOR(3, DataTypes.DOUBLE()))
                                .newGenerationExpression("embedding"));
        GeneratedColumnRow generator = new GeneratedColumnRow(type, type);
        BinaryVector source =
                BinaryVector.fromPrimitiveArray(
                        new double[] {-0.0d, Double.longBitsToDouble(0x7ff8000000000001L), 2.0d});
        BinaryVector supplied =
                BinaryVector.fromPrimitiveArray(new double[] {0.0d, Double.NaN, 2.0d});
        InternalRow row = generator.replaceRow(GenericRow.of(source, supplied));
        assertThat(Double.doubleToRawLongBits(row.getVector(1).getDouble(0)))
                .isEqualTo(Double.doubleToRawLongBits(-0.0d));
        assertThat(row.getVector(1).getDouble(1)).isNaN();
        assertThat(row.getVector(1).getDouble(2)).isEqualTo(2.0d);
        assertThatThrownBy(
                        () ->
                                generator.replaceRow(
                                        GenericRow.of(
                                                source,
                                                BinaryVector.fromPrimitiveArray(
                                                        new double[] {0.0d, Double.NaN, 3.0d}))))
                .hasMessageContaining("does not match generation expression");
        assertThatThrownBy(
                        () ->
                                generator.replaceRow(
                                        GenericRow.of(
                                                source,
                                                BinaryVector.fromPrimitiveArray(
                                                        new double[] {0.0d, 1.0d, 2.0d}))))
                .hasMessageContaining("does not match generation expression");
        assertThat(generator.replaceRow(GenericRow.of(null, null)).isNullAt(1)).isTrue();
    }

    @Test
    void testExplicitMultisetUsesRecursiveSqlEquality() {
        RowType type =
                RowType.of(
                        new DataField(0, "measurements", DataTypes.MULTISET(DataTypes.DOUBLE())),
                        new DataField(1, "copy", DataTypes.MULTISET(DataTypes.DOUBLE()))
                                .newGenerationExpression("measurements"));
        GeneratedColumnRow generator = new GeneratedColumnRow(type, type);
        Map<Double, Integer> source = new LinkedHashMap<>();
        source.put(-0.0d, 2);
        source.put(Double.longBitsToDouble(0x7ff8000000000001L), 1);
        Map<Double, Integer> supplied = new LinkedHashMap<>();
        supplied.put(Double.NaN, 1);
        supplied.put(0.0d, 2);
        InternalRow row =
                generator.replaceRow(
                        GenericRow.of(new GenericMap(source), new GenericMap(supplied)));
        assertThat(row.getMap(1).size()).isEqualTo(2);
        assertThat(Double.doubleToRawLongBits(row.getMap(1).keyArray().getDouble(0)))
                .isEqualTo(Double.doubleToRawLongBits(-0.0d));
        assertThat(row.getMap(1).valueArray().getInt(0)).isEqualTo(2);
        Map<Double, Integer> wrongCount = new LinkedHashMap<>(supplied);
        wrongCount.put(0.0d, 3);
        Map<Double, Integer> wrongElement = new LinkedHashMap<>();
        wrongElement.put(Double.NaN, 1);
        wrongElement.put(1.0d, 2);
        for (Map<Double, Integer> wrong : Arrays.asList(wrongCount, wrongElement)) {
            assertThatThrownBy(
                            () ->
                                    generator.replaceRow(
                                            GenericRow.of(
                                                    new GenericMap(source), new GenericMap(wrong))))
                    .hasMessageContaining("does not match generation expression");
        }
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
        assertThatThrownBy(
                        () ->
                                new GeneratedColumnRow(type, type)
                                        .replaceRow(GenericRow.of(0, Timestamp.fromEpochMillis(1))))
                .hasMessageContaining("does not match generation expression");
    }
}
