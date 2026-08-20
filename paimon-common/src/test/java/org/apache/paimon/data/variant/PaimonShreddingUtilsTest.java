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

package org.apache.paimon.data.variant;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.PaimonShreddingUtils.FieldToExtract;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.paimon.data.variant.PaimonShreddingUtils.assembleVariant;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.assembleVariantStruct;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.buildFieldsToExtract;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.buildVariantSchema;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.castShredded;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.variantShreddingSchema;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for PaimonShreddingUtils. */
public class PaimonShreddingUtilsTest {

    @Test
    void testAssembleAllTypes() {
        VariantCastArgs castArgs = new VariantCastArgs(true, ZoneOffset.UTC);

        DataField f1 =
                new DataField(
                        1,
                        "object",
                        RowType.of(
                                new DataType[] {DataTypes.STRING(), DataTypes.INT()},
                                new String[] {"name", "age"}));
        DataField f2 = new DataField(2, "array", DataTypes.ARRAY(DataTypes.INT()));
        DataField f3 = new DataField(3, "string", DataTypes.STRING());
        DataField f4 = new DataField(4, "tinyint", DataTypes.TINYINT());
        DataField f5 = new DataField(5, "smallint", DataTypes.SMALLINT());
        DataField f6 = new DataField(6, "int", DataTypes.INT());
        DataField f7 = new DataField(7, "long", DataTypes.BIGINT());
        DataField f8 = new DataField(8, "double", DataTypes.DOUBLE());
        DataField f9 = new DataField(9, "decimal", DataTypes.DECIMAL(5, 2));
        DataField f10 = new DataField(10, "boolean", DataTypes.BOOLEAN());
        DataField f11 = new DataField(11, "nullField", DataTypes.INT());
        DataField f12 = new DataField(12, "date", DataTypes.DATE());
        DataField f13 = new DataField(13, "timestamp", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        DataField f14 = new DataField(14, "timestampntz", DataTypes.TIMESTAMP());
        DataField f15 = new DataField(15, "float", DataTypes.FLOAT());
        DataField f16 = new DataField(16, "binary", DataTypes.BYTES());
        RowType allTypes =
                RowType.of(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16);

        Map<String, Object> values = new HashMap<>();
        values.put("object", ImmutableMap.of("name", "Apache Paimon", "age", 3));
        values.put("array", Arrays.asList(1, 2, 3, 4, 5));
        values.put("string", "Hello, World!");
        values.put("tinyint", (byte) 1);
        values.put("smallint", (short) 3000);
        values.put("int", 400000);
        values.put("long", 12345678901234L);
        values.put("double", 1.0123456789012345678901234567890123456789D);
        values.put("decimal", new BigDecimal("100.99"));
        values.put("boolean", true);
        values.put("nullField", null);
        values.put("date", 20000);
        values.put("timestamp", 1_234_567_890_123_456L);
        values.put("timestampntz", 9_876_543_210_123_456L);
        values.put("float", 3.14f);
        values.put("binary", "bytes".getBytes(StandardCharsets.UTF_8));
        GenericVariant v = GenericVariantBuilderHelper.build(allTypes, values);
        GenericRow expected =
                GenericRow.of(
                        GenericRow.of(BinaryString.fromString("Apache Paimon"), 3),
                        new GenericArray(new Integer[] {1, 2, 3, 4, 5}),
                        BinaryString.fromString("Hello, World!"),
                        (byte) 1,
                        (short) 3000,
                        400000,
                        12345678901234L,
                        1.0123456789012345678901234567890123456789D,
                        Decimal.fromBigDecimal(new java.math.BigDecimal("100.99"), 5, 2),
                        true,
                        null,
                        20000,
                        Timestamp.fromMicros(1_234_567_890_123_456L),
                        Timestamp.fromMicros(9_876_543_210_123_456L),
                        3.14f,
                        "bytes".getBytes(StandardCharsets.UTF_8));

        // shredding to real type
        assertVariantStructEquals(new RowType(allTypes.getFields()), allTypes, v, expected);

        // no shredding
        assertVariantStructEquals(RowType.of(), allTypes, v, expected);

        // shredding to string, then cast to the real type
        RowType shreddedType =
                RowType.of(
                        allTypes.getFields().stream()
                                .map(a -> a.newType(DataTypes.STRING()))
                                .toArray(DataField[]::new));
        assertVariantStructEquals(shreddedType, allTypes, v, expected);

        // shredding to real type, then cast to the string
        expected =
                GenericRow.of(
                        BinaryString.fromString("{\"age\":3,\"name\":\"Apache Paimon\"}"),
                        BinaryString.fromString("[1,2,3,4,5]"),
                        BinaryString.fromString("Hello, World!"),
                        BinaryString.fromString("1"),
                        BinaryString.fromString("3000"),
                        BinaryString.fromString("400000"),
                        BinaryString.fromString("12345678901234"),
                        BinaryString.fromString("1.0123456789012346"),
                        BinaryString.fromString("100.99"),
                        BinaryString.fromString("true"),
                        null,
                        BinaryString.fromString(DateTimeUtils.formatDate(20000)),
                        BinaryString.fromString(
                                DateTimeUtils.formatTimestamp(
                                        Timestamp.fromMicros(1_234_567_890_123_456L),
                                        TimeZone.getDefault(),
                                        6)),
                        BinaryString.fromString(
                                DateTimeUtils.formatTimestamp(
                                        Timestamp.fromMicros(9_876_543_210_123_456L),
                                        DateTimeUtils.UTC_ZONE,
                                        6)),
                        BinaryString.fromString("3.14"),
                        BinaryString.fromString("bytes"));

        shreddedType = new RowType(allTypes.getFields());
        RowType shreddingSchema = variantShreddingSchema(shreddedType);
        VariantSchema variantSchema = buildVariantSchema(shreddingSchema);
        FieldToExtract[] fields = new FieldToExtract[allTypes.getFieldCount()];
        for (int i = 0; i < allTypes.getFields().size(); i++) {
            fields[i] =
                    buildFieldsToExtract(
                            DataTypes.STRING(),
                            "$." + allTypes.getFields().get(i).name(),
                            castArgs,
                            variantSchema);
        }
        assertThat(assembleVariantStruct(castShredded(v, variantSchema), variantSchema, fields))
                .isEqualTo(expected);

        // no shredding, then cast to the string
        shreddedType = RowType.of();
        shreddingSchema = variantShreddingSchema(shreddedType);
        variantSchema = buildVariantSchema(shreddingSchema);
        fields = new FieldToExtract[allTypes.getFieldCount()];
        for (int i = 0; i < allTypes.getFields().size(); i++) {
            fields[i] =
                    buildFieldsToExtract(
                            DataTypes.STRING(),
                            "$." + allTypes.getFields().get(i).name(),
                            castArgs,
                            variantSchema);
        }

        assertThat(assembleVariantStruct(castShredded(v, variantSchema), variantSchema, fields))
                .isEqualTo(expected);

        // cast struct to map
        shreddedType = RowType.of(f1);
        shreddingSchema = variantShreddingSchema(shreddedType);
        variantSchema = buildVariantSchema(shreddingSchema);
        fields = new FieldToExtract[1];
        fields[0] =
                buildFieldsToExtract(
                        new MapType(DataTypes.STRING(), DataTypes.STRING()),
                        "$.object",
                        castArgs,
                        variantSchema);
        assertThat(assembleVariantStruct(castShredded(v, variantSchema), variantSchema, fields))
                .isEqualTo(
                        GenericRow.of(
                                new GenericMap(
                                        new HashMap<BinaryString, BinaryString>() {
                                            {
                                                put(
                                                        BinaryString.fromString("age"),
                                                        BinaryString.fromString("3"));
                                                put(
                                                        BinaryString.fromString("name"),
                                                        BinaryString.fromString("Apache Paimon"));
                                            }
                                        })));
    }

    private static void assertVariantStructEquals(
            RowType shreddedType, RowType allTypes, GenericVariant v, GenericRow expected) {
        VariantCastArgs castArgs = new VariantCastArgs(true, ZoneOffset.UTC);

        RowType shreddingSchema = variantShreddingSchema(shreddedType);
        VariantSchema variantSchema = buildVariantSchema(shreddingSchema);

        FieldToExtract[] fieldsToExtract = new FieldToExtract[allTypes.getFieldCount()];
        for (int i = 0; i < allTypes.getFields().size(); i++) {
            fieldsToExtract[i] =
                    buildFieldsToExtract(
                            allTypes.getFields().get(i).type(),
                            "$." + allTypes.getFields().get(i).name(),
                            castArgs,
                            variantSchema);
        }

        InternalRow inputRow = castShredded(v, variantSchema);
        InternalRow outputRow = assembleVariantStruct(inputRow, variantSchema, fieldsToExtract);
        assertThat(outputRow).isEqualTo(expected);

        Variant rebuilt = assembleVariant(inputRow, variantSchema);
        assertThat(rebuilt.toJson(castArgs.zoneId())).isEqualTo(v.toJson(castArgs.zoneId()));
    }

    @Test
    void testAssembleVariantStructWithShredding() {
        VariantCastArgs castArgs = new VariantCastArgs(true, ZoneOffset.UTC);

        // shreddedType: ROW<a INT, b STRING>
        RowType shreddedType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"a", "b"});
        RowType shreddingSchema = variantShreddingSchema(shreddedType);
        VariantSchema variantSchema = buildVariantSchema(shreddingSchema);

        // fieldsToExtract: $.a : int, $.b : string
        FieldToExtract f1 = buildFieldsToExtract(DataTypes.INT(), "$.a", castArgs, variantSchema);
        FieldToExtract f2 =
                buildFieldsToExtract(DataTypes.STRING(), "$.b", castArgs, variantSchema);
        FieldToExtract[] fields = new FieldToExtract[] {f1, f2};

        GenericVariant v = GenericVariant.fromJson("{\"a\": 1, \"b\": \"hello\"}");
        assertThat(assembleVariantStruct(castShredded(v, variantSchema), variantSchema, fields))
                .isEqualTo(GenericRow.of(1, BinaryString.fromString("hello")));

        v = GenericVariant.fromJson("{\"a\": 27}");
        assertThat(assembleVariantStruct(castShredded(v, variantSchema), variantSchema, fields))
                .isEqualTo(GenericRow.of(27, null));

        v = GenericVariant.fromJson("{\"b\":\"hangzhou\", \"other\":\"xxx\"}");
        assertThat(assembleVariantStruct(castShredded(v, variantSchema), variantSchema, fields))
                .isEqualTo(GenericRow.of(null, BinaryString.fromString("hangzhou")));

        v = GenericVariant.fromJson("{\"other\":\"yyy\"}");
        assertThat(assembleVariantStruct(castShredded(v, variantSchema), variantSchema, fields))
                .isEqualTo(GenericRow.of(null, null));

        v = GenericVariant.fromJson("{\"a\":\"27\"}");
        assertThat(assembleVariantStruct(castShredded(v, variantSchema), variantSchema, fields))
                .isEqualTo(GenericRow.of(27, null));

        v = GenericVariant.fromJson("{}");
        assertThat(assembleVariantStruct(castShredded(v, variantSchema), variantSchema, fields))
                .isEqualTo(GenericRow.of(null, null));

        // fieldsToExtract: $.a : int, $.b : string, $.c : string and c is not in shreddedType
        FieldToExtract f3 =
                buildFieldsToExtract(DataTypes.STRING(), "$.c", castArgs, variantSchema);
        fields = new FieldToExtract[] {f1, f2, f3};
        v = GenericVariant.fromJson("{\"c\": \"hi\", \"a\": 27}");
        assertThat(assembleVariantStruct(castShredded(v, variantSchema), variantSchema, fields))
                .isEqualTo(GenericRow.of(27, null, BinaryString.fromString("hi")));

        v = GenericVariant.fromJson("{\"c\": 1111, \"a\": 27}");
        assertThat(assembleVariantStruct(castShredded(v, variantSchema), variantSchema, fields))
                .isEqualTo(GenericRow.of(27, null, BinaryString.fromString("1111")));
    }

    @Test
    void testVariantCastArgs() {
        // shreddedType: ROW<a INT, b STRING>
        RowType shreddedType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"a"});
        RowType shreddingSchema = variantShreddingSchema(shreddedType);
        VariantSchema variantSchema = buildVariantSchema(shreddingSchema);

        // failOnError = true
        VariantCastArgs castArgs = new VariantCastArgs(true, ZoneOffset.UTC);

        FieldToExtract f1 = buildFieldsToExtract(DataTypes.INT(), "$.a", castArgs, variantSchema);
        FieldToExtract[] fields1 = new FieldToExtract[] {f1};

        GenericVariant v1 = GenericVariant.fromJson("{\"a\": \"not a num\"}");
        assertThatThrownBy(
                () ->
                        assembleVariantStruct(
                                castShredded(v1, variantSchema), variantSchema, fields1));

        // failOnError = false
        castArgs = new VariantCastArgs(false, ZoneOffset.UTC);
        FieldToExtract f2 = buildFieldsToExtract(DataTypes.INT(), "$.a", castArgs, variantSchema);
        FieldToExtract[] fields2 = new FieldToExtract[] {f2};

        GenericVariant v2 = GenericVariant.fromJson("{\"a\": \"not a num\"}");
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, null);
        assertThat(assembleVariantStruct(castShredded(v2, variantSchema), variantSchema, fields2))
                .isEqualTo(genericRow);
    }
}
