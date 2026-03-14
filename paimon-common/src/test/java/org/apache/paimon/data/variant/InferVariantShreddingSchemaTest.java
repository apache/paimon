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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.data.variant.PaimonShreddingUtils.variantShreddingSchema;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link InferVariantShreddingSchema}. */
public class InferVariantShreddingSchemaTest {

    public static InferVariantShreddingSchema defaultInferVariantShreddingSchema(RowType schema) {
        return new InferVariantShreddingSchema(schema, 300, 50, 0.1);
    }

    @Test
    void testInferSchemaWithSimpleObject() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        // Create test data: {"name": "Alice", "age": 30}
        GenericVariant variant1 = GenericVariant.fromJson("{\"name\": \"Alice\", \"age\": 30}");
        GenericVariant variant2 = GenericVariant.fromJson("{\"name\": \"Bob\", \"age\": 25}");
        GenericVariant variant3 = GenericVariant.fromJson("{\"name\": \"Charlie\", \"age\": 35}");

        List<InternalRow> rows = new ArrayList<>();
        rows.add(GenericRow.of(variant1));
        rows.add(GenericRow.of(variant2));
        rows.add(GenericRow.of(variant3));

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        // Verify schema structure
        RowType expectShreddedType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"age", "name"});
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectShreddedType));
    }

    @Test
    void testInferSchemaWithNestedStruct() {
        // Schema: row<data: row<v: variant>>
        RowType innerSchema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});
        RowType schema = RowType.of(new DataType[] {innerSchema}, new String[] {"data"});

        // Create test data
        GenericVariant variant1 = GenericVariant.fromJson("{\"x\": 1, \"y\": 2}");
        GenericRow innerRow1 = GenericRow.of(variant1);

        GenericVariant variant2 = GenericVariant.fromJson("{\"x\": 3, \"y\": 4}");
        GenericRow innerRow2 = GenericRow.of(variant2);

        List<InternalRow> rows = new ArrayList<>();
        rows.add(GenericRow.of(innerRow1));
        rows.add(GenericRow.of(innerRow2));

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        // Nested variant in row<data: row<v: variant>>
        // Expected: inferred schema for inner variant field with x and y
        RowType expectedInnerType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.BIGINT()},
                        new String[] {"x", "y"});
        RowType expectedDataType =
                RowType.of(
                        new DataType[] {variantShreddingSchema(expectedInnerType)},
                        new String[] {"v"});
        assertThat(inferredSchema.getField("data").type()).isEqualTo(expectedDataType);
    }

    @Test
    void testInferSchemaWithArray() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        // Create test data with arrays
        GenericVariant variant1 = GenericVariant.fromJson("{\"numbers\": [1, 2, 3]}");
        GenericVariant variant2 = GenericVariant.fromJson("{\"numbers\": [4, 5, 6]}");

        List<InternalRow> rows = Arrays.asList(GenericRow.of(variant1), GenericRow.of(variant2));

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        // Array field [1,2,3] should be inferred as array<bigint>
        RowType expectedType =
                RowType.of(
                        new DataType[] {DataTypes.ARRAY(DataTypes.BIGINT())},
                        new String[] {"numbers"});
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType));
    }

    @Test
    void testInferSchemaWithMixedTypes() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        // Create test data with mixed types
        GenericVariant variant1 =
                GenericVariant.fromJson(
                        "{\"str\": \"hello\", \"num\": 42, \"bool\": true, \"dec\": 3.14}");
        GenericVariant variant2 =
                GenericVariant.fromJson(
                        "{\"str\": \"world\", \"num\": 100, \"bool\": false, \"dec\": 2.71}");

        List<InternalRow> rows = Arrays.asList(GenericRow.of(variant1), GenericRow.of(variant2));

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        // Mixed types: string, bigint, boolean, decimal (3.14 and 2.71 become DECIMAL(18, 2))
        RowType expectedType =
                RowType.of(
                        new DataType[] {
                            DataTypes.BOOLEAN(),
                            DataTypes.DECIMAL(18, 2),
                            DataTypes.BIGINT(),
                            DataTypes.STRING()
                        },
                        new String[] {"bool", "dec", "num", "str"});
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType));
    }

    @Test
    void testInferSchemaWithNullValues() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        // Create test data with null
        GenericVariant variant1 = GenericVariant.fromJson("{\"a\": 1, \"b\": null}");
        GenericVariant variant2 = GenericVariant.fromJson("{\"a\": 2, \"b\": 3}");

        List<InternalRow> rows = Arrays.asList(GenericRow.of(variant1), GenericRow.of(variant2));

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        // Field "a" appears in all rows, "b" has null in one row
        // With NULL case handling, b field can now be inferred as BIGINT (null is handled properly)
        RowType expectedType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.BIGINT()},
                        new String[] {"a", "b"});
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType));
    }

    @Test
    void testInferSchemaWithEmptyRows() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        List<InternalRow> rows = new ArrayList<>();

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        // Empty rows should result in variant type without typed schema
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(DataTypes.VARIANT()));
    }

    @Test
    void testInferSchemaWithDeepNesting() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        // Create deeply nested JSON
        String deepJson = "{\"level1\": {\"level2\": {\"level3\": {\"value\": 42}}}}";
        GenericVariant variant = GenericVariant.fromJson(deepJson);

        List<InternalRow> rows = Arrays.asList(GenericRow.of(variant));

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        // Deep nested structure: level1 -> level2 -> level3 -> value
        RowType level3Type =
                RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"value"});
        RowType level2Type = RowType.of(new DataType[] {level3Type}, new String[] {"level3"});
        RowType level1Type = RowType.of(new DataType[] {level2Type}, new String[] {"level2"});
        RowType expectedType = RowType.of(new DataType[] {level1Type}, new String[] {"level1"});
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType));
    }

    @Test
    void testInferSchemaWithMinFieldCardinalityRatio() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        List<InternalRow> rows = new ArrayList<>();

        // Add 10 rows, where field "rare" appears in 2/10 rows (20%)
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"common\": 1, \"rare\": 99}")));
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"common\": 2, \"rare\": 88}")));
        for (int i = 0; i < 8; i++) {
            rows.add(GenericRow.of(GenericVariant.fromJson("{\"common\": " + i + "}")));
        }

        // Test with threshold (0.1 = 10%): rare field should be included (2/10 = 20% >= 10%)
        InferVariantShreddingSchema inferrer1 =
                new InferVariantShreddingSchema(schema, 300, 50, 0.1);
        RowType inferredSchema1 = inferrer1.inferSchema(rows);
        RowType expectedType1 =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.BIGINT()},
                        new String[] {"common", "rare"});
        assertThat(inferredSchema1.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType1));

        // Test with higher threshold (0.25 = 25%): rare field should be excluded (2/10 = 20% < 25%)
        InferVariantShreddingSchema inferrer2 =
                new InferVariantShreddingSchema(schema, 300, 50, 0.25);
        RowType inferredSchema2 = inferrer2.inferSchema(rows);
        RowType expectedType2 =
                RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"common"});
        assertThat(inferredSchema2.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType2));
    }

    @Test
    void testInferSchemaWithMaxDepthLimit() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        // Create a 3-level nested JSON
        String json = "{\"level1\": {\"level2\": {\"level3\": {\"value\": 42}}}}";
        GenericVariant variant = GenericVariant.fromJson(json);
        List<InternalRow> rows = Arrays.asList(GenericRow.of(variant));

        // Test with max depth = 50: should infer full structure
        InferVariantShreddingSchema inferrer1 =
                new InferVariantShreddingSchema(schema, 300, 50, 0.1);
        RowType inferredSchema1 = inferrer1.inferSchema(rows);

        // Should have full nested structure: level1 -> level2 -> level3 -> value
        RowType level3Type =
                RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"value"});
        RowType level2Type = RowType.of(new DataType[] {level3Type}, new String[] {"level3"});
        RowType level1Type = RowType.of(new DataType[] {level2Type}, new String[] {"level2"});
        RowType expectedType1 = RowType.of(new DataType[] {level1Type}, new String[] {"level1"});
        assertThat(inferredSchema1.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType1));

        // Test with max depth = 1: level1 is inferred, but level2 and deeper become variant
        InferVariantShreddingSchema inferrer2 =
                new InferVariantShreddingSchema(schema, 300, 1, 0.1);
        RowType inferredSchema2 = inferrer2.inferSchema(rows);

        // Result: level1 field exists but is variant
        RowType expectedType2 =
                RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"level1"});
        assertThat(inferredSchema2.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType2));
    }

    @Test
    void testInferSchemaWithMaxWidthLimit() {
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        // Create JSON with 5 fields
        String json = "{\"field1\": 1, \"field2\": 2, \"field3\": 3, \"field4\": 4, \"field5\": 5}";
        GenericVariant variant = GenericVariant.fromJson(json);
        List<InternalRow> rows = Arrays.asList(GenericRow.of(variant));

        // Test with maxSchemaWidth = 11: all 5 fields should be inferred
        // Each field consumes 2 counts (one for field itself, one for BIGINT type)
        // So 5 fields = 10 counts, plus 1 for the root = 11 total
        InferVariantShreddingSchema inferrer1 =
                new InferVariantShreddingSchema(schema, 11, 50, 0.1);
        RowType inferredSchema1 = inferrer1.inferSchema(rows);

        // Should have all 5 fields
        RowType expectedType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"field1", "field2", "field3", "field4", "field5"});
        assertThat(inferredSchema1.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType1));

        // Test with maxSchemaWidth = 9: only first 4 fields are inferred
        // 4 fields = 8 counts, plus 1 for root = 9 total
        InferVariantShreddingSchema inferrer2 = new InferVariantShreddingSchema(schema, 9, 50, 0.1);
        RowType inferredSchema2 = inferrer2.inferSchema(rows);

        // Should have only 4 fields (field1-4), field5 is dropped
        RowType expectedType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"field1", "field2", "field3", "field4"});
        assertThat(inferredSchema2.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType2));
    }

    @Test
    void testInferSchemaWithAllPrimitiveTypes() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        String json =
                "{"
                        + "\"string\": \"test\", "
                        + "\"long\": 123456789, "
                        + "\"double\": 3.14159, "
                        + "\"boolean\": true, "
                        + "\"null\": null"
                        + "}";

        GenericVariant variant = GenericVariant.fromJson(json);
        List<InternalRow> rows = Arrays.asList(GenericRow.of(variant));

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        // All primitive types: boolean, decimal (3.14159 becomes DECIMAL), bigint, variant (null),
        // string
        RowType expectedType =
                RowType.of(
                        new DataType[] {
                            DataTypes.BOOLEAN(),
                            DataTypes.DECIMAL(18, 5),
                            DataTypes.BIGINT(),
                            DataTypes.VARIANT(),
                            DataTypes.STRING()
                        },
                        new String[] {"boolean", "double", "long", "null", "string"});
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType));
    }

    @Test
    void testInferSchemaWithConflictingTypes() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        // Field "value" has different types in different rows
        GenericVariant variant1 = GenericVariant.fromJson("{\"value\": 123}");
        GenericVariant variant2 = GenericVariant.fromJson("{\"value\": \"string\"}");

        List<InternalRow> rows = Arrays.asList(GenericRow.of(variant1), GenericRow.of(variant2));

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        // Conflicting types (int vs string) should fall back to variant type
        RowType expectedType =
                RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"value"});
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType));
    }

    @Test
    void testMultipleVariantFields() {
        // Schema: row<v1: variant, v2: variant, id: int>
        RowType schema =
                RowType.of(
                        new DataType[] {DataTypes.VARIANT(), DataTypes.VARIANT(), DataTypes.INT()},
                        new String[] {"v1", "v2", "id"});

        GenericVariant variant1 = GenericVariant.fromJson("{\"name\": \"Alice\"}");
        GenericVariant variant2 = GenericVariant.fromJson("{\"age\": 30}");

        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(variant1, variant2, 1), GenericRow.of(variant1, variant2, 2));

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        // Multiple variant fields: v1 with {name: string}, v2 with {age: bigint}, id: int
        RowType expectedV1Type =
                RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"name"});
        RowType expectedV2Type =
                RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"age"});
        assertThat(inferredSchema.getFieldCount()).isEqualTo(3);
        assertThat(inferredSchema.getField("v1").type())
                .isEqualTo(variantShreddingSchema(expectedV1Type));
        assertThat(inferredSchema.getField("v2").type())
                .isEqualTo(variantShreddingSchema(expectedV2Type));
        assertThat(inferredSchema.getField("id").type()).isEqualTo(DataTypes.INT());
    }

    @Test
    void testLargeDatasetWithManyFields() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        List<InternalRow> rows = new ArrayList<>();
        // Generate 500 rows, each with 50 fields
        for (int i = 0; i < 500; i++) {
            StringBuilder jsonBuilder = new StringBuilder("{");
            for (int j = 0; j < 50; j++) {
                if (j > 0) {
                    jsonBuilder.append(", ");
                }
                jsonBuilder.append(String.format("\"field%d\": %d", j, (i * 50 + j) % 1000));
            }
            jsonBuilder.append("}");
            rows.add(GenericRow.of(GenericVariant.fromJson(jsonBuilder.toString())));
        }

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        // All 50 fields should be inferred as BIGINT
        DataType variantFieldType = inferredSchema.getField("v").type();
        assertThat(variantFieldType).isInstanceOf(RowType.class);
        RowType shreddedSchema = (RowType) variantFieldType;

        // Should have metadata, value, and typed_value fields
        DataField typedValueField = shreddedSchema.getField("typed_value");
        assertThat(typedValueField).isNotNull();
        assertThat(typedValueField.type()).isInstanceOf(RowType.class);

        RowType typedValue = (RowType) typedValueField.type();
        // Should infer all 50 fields (field0 to field49) as BIGINT
        assertThat(typedValue.getFieldCount()).isEqualTo(50);

        // Verify the schema structure (not exact equality due to field ordering)
        assertThat(inferredSchema.getField("v").type()).isInstanceOf(RowType.class);
        RowType actualShreddedSchema = (RowType) inferredSchema.getField("v").type();
        assertThat(actualShreddedSchema.getFieldCount())
                .isEqualTo(3); // metadata, value, typed_value

        // Verify all 50 fields are present with correct types
        RowType actualTypedValue = (RowType) actualShreddedSchema.getField("typed_value").type();
        for (int i = 0; i < 50; i++) {
            String fieldName = "field" + i;
            DataField field = actualTypedValue.getField(fieldName);
            assertThat(field).as("Field %s should exist", fieldName).isNotNull();
            // Each field is shredded: ROW<value BYTES, typed_value BIGINT> NOT NULL
            assertThat(field.type()).isInstanceOf(RowType.class);
            RowType shreddedFieldType = (RowType) field.type();
            assertThat(shreddedFieldType.getField("typed_value").type())
                    .isEqualTo(DataTypes.BIGINT());
        }
    }

    @Test
    void testNullRecords() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        List<InternalRow> rows = new ArrayList<>();
        // Add rows where the entire variant value is null
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"a\": 1, \"b\": 2}")));
        rows.add(GenericRow.of((GenericVariant) null)); // Entire record is null
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"a\": 3, \"b\": 4}")));
        rows.add(GenericRow.of((GenericVariant) null)); // Entire record is null
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"a\": 5, \"b\": 6}")));

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        // Verify the inferred schema structure
        assertThat(inferredSchema).isNotNull();
        assertThat(inferredSchema.getFieldCount()).isEqualTo(1);

        // The variant field should contain shredded schema with fields a and b
        DataType variantFieldType = inferredSchema.getFields().get(0).type();
        assertThat(variantFieldType).isInstanceOf(RowType.class);

        RowType shreddedSchema = (RowType) variantFieldType;
        // Should have fields for metadata, typed_value, and value
        assertThat(shreddedSchema.getFieldCount()).isGreaterThanOrEqualTo(3);
    }

    @Test
    void testAllNullRecords() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        List<InternalRow> rows = new ArrayList<>();
        // All records are null
        for (int i = 0; i < 100; i++) {
            rows.add(GenericRow.of((GenericVariant) null));
        }

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        // When all records are null, should result in variant type without typed schema
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(DataTypes.VARIANT()));
    }

    @Test
    void testMixedNullAndValidRecords() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        List<InternalRow> rows = new ArrayList<>();
        // Mix of null records and valid records
        for (int i = 0; i < 200; i++) {
            if (i % 3 == 0) {
                rows.add(GenericRow.of((GenericVariant) null));
            } else {
                rows.add(
                        GenericRow.of(
                                GenericVariant.fromJson(
                                        String.format(
                                                "{\"id\": %d, \"value\": \"data%d\"}", i, i))));
            }
        }

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        RowType expectedTypedValue =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"id", "value"});
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedTypedValue));
    }

    @Test
    void testMixedArrayTypes() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        List<InternalRow> rows = new ArrayList<>();
        // Arrays with different element types
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"arr\": [1, 2, 3]}")));
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"arr\": [\"a\", \"b\", \"c\"]}")));
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"arr\": [true, false, true]}")));
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"arr\": [1, \"mixed\", true]}")));

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        RowType expectedType =
                RowType.of(
                        new DataType[] {DataTypes.ARRAY(DataTypes.VARIANT())},
                        new String[] {"arr"});
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType));
    }

    @Test
    void testIntegerTypeMixing() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        List<InternalRow> rows = new ArrayList<>();
        // Mix of small and large integers - should merge to BIGINT
        for (int i = 0; i < 100; i++) {
            long value = (i % 2 == 0) ? i : (i * 1000000000L);
            rows.add(GenericRow.of(GenericVariant.fromJson(String.format("{\"num\": %d}", value))));
        }

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        RowType expectedTypedValue =
                RowType.of(new DataType[] {DataTypes.BIGINT()}, new String[] {"num"});
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedTypedValue));
    }

    @Test
    void testDoubleAndIntegerMixing() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        List<InternalRow> rows = new ArrayList<>();
        // Mix integers and doubles - should become variant type
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"value\": 100}")));
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"value\": 100}")));
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"value\": 100}")));
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"value\": 3.14}")));

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        RowType expectedTypedValue =
                RowType.of(new DataType[] {DataTypes.DECIMAL(18, 2)}, new String[] {"value"});
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedTypedValue));
    }

    @Test
    void testNullInNestedArrays() {
        // Schema: row<v: variant>
        RowType schema = RowType.of(new DataType[] {DataTypes.VARIANT()}, new String[] {"v"});

        List<InternalRow> rows = new ArrayList<>();
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"arr\": [1, 2, 3, null, 5]}")));
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"arr\": [null, null, null]}")));
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"arr\": [10, null, 20, null, 30]}")));
        rows.add(GenericRow.of(GenericVariant.fromJson("{\"arr\": null}")));

        InferVariantShreddingSchema inferrer = defaultInferVariantShreddingSchema(schema);
        RowType inferredSchema = inferrer.inferSchema(rows);

        RowType expectedType =
                RowType.of(
                        new DataType[] {DataTypes.ARRAY(DataTypes.VARIANT())},
                        new String[] {"arr"});
        assertThat(inferredSchema.getField("v").type())
                .isEqualTo(variantShreddingSchema(expectedType));
    }
}
