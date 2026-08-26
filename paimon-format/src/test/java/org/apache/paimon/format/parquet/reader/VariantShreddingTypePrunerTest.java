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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.data.variant.VariantMetadataUtils;
import org.apache.paimon.format.parquet.ParquetSchemaConverter;
import org.apache.paimon.format.parquet.VariantShreddingTypePruner;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link VariantShreddingTypePruner}. */
public class VariantShreddingTypePrunerTest {

    @Test
    public void testObjectNestedFieldsPrunesTypedValue() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.a.b")
                        .field(DataTypes.INT(), "$.a.c")
                        .field(DataTypes.INT(), "$.d")
                        .build();

        GroupType parquetType = createParquetVariantType(nestedObjectShreddingSchema());
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        RowType physicalV = toRowType(clipped);

        assertThat(physicalV.getFieldNames()).doesNotContain("value");
        RowType physicalTypedValue = getRowType(physicalV, "typed_value");
        assertThat(physicalTypedValue.getFieldNames()).containsExactly("a", "d");

        RowType physicalA = getRowType(physicalTypedValue, "a");
        RowType physicalATypedValue = getRowType(physicalA, "typed_value");
        assertThat(physicalATypedValue.getFieldNames()).containsExactly("b", "c");
    }

    @Test
    public void testObjectMissingTopLevelFieldFallsBackToValue() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.z")
                        .build();

        GroupType parquetType = createParquetVariantType(nestedObjectShreddingSchema());
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        RowType physicalV = toRowType(clipped);

        assertThat(physicalV.getFieldNames()).contains("value");
        assertThat(physicalV.getFieldNames()).doesNotContain("typed_value");
    }

    @Test
    public void testObjectMixedExistingAndMissingFieldsKeepsTypedValueAndValue() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.a.b")
                        .field(DataTypes.INT(), "$.z")
                        .build();

        GroupType parquetType = createParquetVariantType(nestedObjectShreddingSchema());
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        RowType physicalV = toRowType(clipped);

        assertThat(physicalV.getFieldNames()).contains("value");
        RowType physicalTypedValue = getRowType(physicalV, "typed_value");
        assertThat(physicalTypedValue.getFieldNames()).containsExactly("a");

        RowType physicalA = getRowType(physicalTypedValue, "a");
        RowType physicalATypedValue = getRowType(physicalA, "typed_value");
        assertThat(physicalATypedValue.getFieldNames()).containsExactly("b");
    }

    @Test
    public void testObjectKeepAllKeepsWholeField() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "b", DataTypes.INT()),
                                        DataTypes.FIELD(1, "c", DataTypes.INT())),
                                "$.a")
                        .build();

        GroupType parquetType = createParquetVariantType(nestedObjectShreddingSchema());
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        RowType physicalV = toRowType(clipped);

        assertThat(physicalV.getFieldNames()).doesNotContain("value");
        RowType physicalTypedValue = getRowType(physicalV, "typed_value");
        assertThat(physicalTypedValue.getFieldNames()).containsExactly("a");
        RowType physicalA = getRowType(physicalTypedValue, "a");
        RowType physicalATypedValue = getRowType(physicalA, "typed_value");
        assertThat(physicalATypedValue.getFieldNames()).containsExactly("b", "c");
    }

    private static RowType nestedObjectShreddingSchema() {
        return PaimonShreddingUtils.variantShreddingSchema(
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "a",
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "b", DataTypes.INT()),
                                        DataTypes.FIELD(1, "c", DataTypes.INT()))),
                        DataTypes.FIELD(1, "d", DataTypes.INT()),
                        DataTypes.FIELD(2, "e", DataTypes.INT())));
    }

    @Test
    public void testCaseSensitive() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.a")
                        .build();

        GroupType parquetType =
                createParquetVariantType(
                        PaimonShreddingUtils.variantShreddingSchema(
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "a", DataTypes.INT()),
                                        DataTypes.FIELD(1, "A", DataTypes.INT()))));
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        RowType physicalV = toRowType(clipped);
        RowType physicalTypedValue = getRowType(physicalV, "typed_value");
        assertThat(physicalTypedValue.getFieldNames()).containsExactly("a");
    }

    @Test
    public void testArrayKeepAllArrayKeepsElementValueAndTypedValue() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.ARRAY(DataTypes.VARIANT()), "$.arr")
                        .build();

        GroupType parquetType = createParquetVariantType(arrayShreddingSchema());
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        RowType physicalV = toRowType(clipped);

        assertThat(physicalV.getFieldNames()).doesNotContain("value");
        RowType physicalTypedValue = getRowType(physicalV, "typed_value");
        assertThat(physicalTypedValue.getFieldNames()).containsExactly("arr");

        RowType physicalArr = getRowType(physicalTypedValue, "arr");
        ArrayType physicalArrList =
                (ArrayType) physicalArr.getTypeAt(physicalArr.getFieldIndex("typed_value"));
        RowType physicalArrElement = (RowType) physicalArrList.getElementType();
        assertThat(physicalArrElement.getFieldNames()).contains("value", "typed_value");
        RowType physicalArrElementTypedValue = getRowType(physicalArrElement, "typed_value");
        assertThat(physicalArrElementTypedValue.getFieldNames()).containsExactly("x", "y");
    }

    @Test
    public void testArrayKeepAllElementKeepsElementValueAndTypedValue() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.VARIANT(), "$.arr[0]")
                        .build();

        GroupType parquetType = createParquetVariantType(arrayShreddingSchema());
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        RowType physicalV = toRowType(clipped);

        assertThat(physicalV.getFieldNames()).doesNotContain("value");
        RowType physicalTypedValue = getRowType(physicalV, "typed_value");
        assertThat(physicalTypedValue.getFieldNames()).containsExactly("arr");

        RowType physicalArr = getRowType(physicalTypedValue, "arr");
        ArrayType physicalArrList =
                (ArrayType) physicalArr.getTypeAt(physicalArr.getFieldIndex("typed_value"));
        RowType physicalArrElement = (RowType) physicalArrList.getElementType();
        assertThat(physicalArrElement.getFieldNames()).contains("value", "typed_value");
        RowType physicalArrElementTypedValue = getRowType(physicalArrElement, "typed_value");
        assertThat(physicalArrElementTypedValue.getFieldNames()).containsExactly("x", "y");
    }

    @Test
    public void testArrayElementFieldPrunesElementTypedValue() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.arr[0].x")
                        .build();

        GroupType parquetType = createParquetVariantType(arrayShreddingSchema());
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        RowType physicalV = toRowType(clipped);

        assertThat(physicalV.getFieldNames()).doesNotContain("value");
        RowType physicalTypedValue = getRowType(physicalV, "typed_value");
        assertThat(physicalTypedValue.getFieldNames()).containsExactly("arr");

        RowType physicalArr = getRowType(physicalTypedValue, "arr");
        ArrayType physicalArrList =
                (ArrayType) physicalArr.getTypeAt(physicalArr.getFieldIndex("typed_value"));
        RowType physicalArrElement = (RowType) physicalArrList.getElementType();
        RowType physicalArrElementTypedValue = getRowType(physicalArrElement, "typed_value");
        assertThat(physicalArrElementTypedValue.getFieldNames()).containsExactly("x");
    }

    @Test
    public void testArrayElementFieldMissingFallsBackToElementValue() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.arr[0].z")
                        .build();

        GroupType parquetType = createParquetVariantType(arrayShreddingSchema());
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        RowType physicalV = toRowType(clipped);

        assertThat(physicalV.getFieldNames()).doesNotContain("value");
        RowType physicalTypedValue = getRowType(physicalV, "typed_value");
        assertThat(physicalTypedValue.getFieldNames()).containsExactly("arr");

        RowType physicalArr = getRowType(physicalTypedValue, "arr");
        ArrayType physicalArrList =
                (ArrayType) physicalArr.getTypeAt(physicalArr.getFieldIndex("typed_value"));
        RowType physicalArrElement = (RowType) physicalArrList.getElementType();
        assertThat(physicalArrElement.getFieldNames()).contains("value");
        assertThat(physicalArrElement.getFieldNames()).doesNotContain("typed_value");
    }

    private static RowType arrayShreddingSchema() {
        return PaimonShreddingUtils.variantShreddingSchema(
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "arr",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD(0, "x", DataTypes.INT()),
                                                DataTypes.FIELD(1, "y", DataTypes.INT()))))));
    }

    @Test
    public void testArrayPrimitiveElementKeepAllKeepsElementShreddingRow() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.arr[0]")
                        .build();

        GroupType parquetType = createParquetVariantType(primitiveArrayShreddingSchema());
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        RowType physicalV = toRowType(clipped);

        assertThat(physicalV.getFieldNames()).doesNotContain("value");
        RowType physicalTypedValue = getRowType(physicalV, "typed_value");
        assertThat(physicalTypedValue.getFieldNames()).containsExactly("arr");

        RowType physicalArr = getRowType(physicalTypedValue, "arr");
        ArrayType physicalArrList =
                (ArrayType) physicalArr.getTypeAt(physicalArr.getFieldIndex("typed_value"));
        RowType physicalArrElement = (RowType) physicalArrList.getElementType();
        assertThat(physicalArrElement.getFieldNames()).contains("value", "typed_value");
        assertThat(physicalArrElement.getTypeAt(physicalArrElement.getFieldIndex("typed_value")))
                .isEqualTo(DataTypes.INT());
    }

    @Test
    public void testTopLevelArrayElementFieldPrunesElementTypedValue() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$[0].x")
                        .build();

        GroupType parquetType =
                createParquetVariantType(
                        PaimonShreddingUtils.variantShreddingSchema(
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD(0, "x", DataTypes.INT()),
                                                DataTypes.FIELD(1, "y", DataTypes.INT())))));
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        RowType physicalV = toRowType(clipped);

        assertThat(physicalV.getFieldNames()).doesNotContain("value");
        ArrayType physicalList =
                (ArrayType) physicalV.getTypeAt(physicalV.getFieldIndex("typed_value"));
        RowType physicalElement = (RowType) physicalList.getElementType();
        RowType physicalElementTypedValue = getRowType(physicalElement, "typed_value");
        assertThat(physicalElementTypedValue.getFieldNames()).containsExactly("x");
    }

    @Test
    public void testTopLevelArrayKeepAllVariantKeepsElementValue() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.VARIANT(), "$[0]")
                        .build();

        GroupType parquetType =
                createParquetVariantType(
                        PaimonShreddingUtils.variantShreddingSchema(
                                DataTypes.ARRAY(DataTypes.VARIANT())));
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        RowType physicalV = toRowType(clipped);

        assertThat(physicalV.getFieldNames()).contains("value");
        ArrayType physicalList =
                (ArrayType) physicalV.getTypeAt(physicalV.getFieldIndex("typed_value"));
        RowType physicalElement = (RowType) physicalList.getElementType();
        assertThat(physicalElement.getFieldNames()).containsExactly("value");
    }

    @Test
    public void testTopLevelArrayNestedProjectionKeepsWholeList() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$[0].x")
                        .build();

        GroupType parquetType =
                createParquetVariantType(
                        PaimonShreddingUtils.variantShreddingSchema(
                                DataTypes.ARRAY(DataTypes.VARIANT())));
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        RowType physicalV = toRowType(clipped);

        // The element has no typed_value column, so we cannot prune inside it.
        // The whole list must be retained and the assembler reads x from the binary value.
        assertThat(physicalV.getFieldNames()).doesNotContain("value");
        ArrayType physicalList =
                (ArrayType) physicalV.getTypeAt(physicalV.getFieldIndex("typed_value"));
        RowType physicalElement = (RowType) physicalList.getElementType();
        assertThat(physicalElement.getFieldNames()).containsExactly("value");
    }

    private static RowType primitiveArrayShreddingSchema() {
        return PaimonShreddingUtils.variantShreddingSchema(
                DataTypes.ROW(DataTypes.FIELD(0, "arr", DataTypes.ARRAY(DataTypes.INT()))));
    }

    @Test
    public void testTwoLevelArrayGroupElementKeepsWholeList() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.arr[0].x")
                        .build();

        GroupType parquetType = createTwoLevelArrayVariantType();
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        GroupType actual =
                clipped.asGroupType()
                        .getType("typed_value")
                        .asGroupType()
                        .getType(0)
                        .asGroupType()
                        .getType(1)
                        .asGroupType();
        GroupType expected =
                parquetType
                        .asGroupType()
                        .getType("typed_value")
                        .asGroupType()
                        .getType(0)
                        .asGroupType()
                        .getType(1)
                        .asGroupType();
        assertThat(actual).isEqualTo(expected);
    }

    private static GroupType createTwoLevelArrayVariantType() {
        // x shredding row: value + typed_value
        GroupType xShreddingRow =
                Types.buildGroup(Type.Repetition.REQUIRED)
                        .optional(BINARY)
                        .named("value")
                        .optional(INT32)
                        .named("typed_value")
                        .named("x");

        // y shredding row: value + typed_value
        GroupType yShreddingRow =
                Types.buildGroup(Type.Repetition.REQUIRED)
                        .optional(BINARY)
                        .named("value")
                        .optional(INT32)
                        .named("typed_value")
                        .named("y");

        // element shredding row: value + typed_value(x, y)
        GroupType elementTypedValue =
                Types.buildGroup(OPTIONAL)
                        .addField(xShreddingRow)
                        .addField(yShreddingRow)
                        .named("typed_value");

        GroupType elementRow =
                Types.buildGroup(REPEATED)
                        .optional(BINARY)
                        .named("value")
                        .addField(elementTypedValue)
                        .named("element");

        // Two-level list: the list group's immediate child is the repeated element row.
        GroupType arrList =
                Types.buildGroup(OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(elementRow)
                        .named("typed_value");

        // arr shredding row: value + typed_value(list)
        GroupType arrShreddingRow =
                Types.buildGroup(Type.Repetition.REQUIRED)
                        .optional(BINARY)
                        .named("value")
                        .addField(arrList)
                        .named("arr");

        GroupType typedValue =
                Types.buildGroup(OPTIONAL).addField(arrShreddingRow).named("typed_value");

        return Types.buildGroup(OPTIONAL)
                .required(BINARY)
                .named("metadata")
                .required(BINARY)
                .named("value")
                .addField(typedValue)
                .named("v");
    }

    @Test
    public void testTwoLevelPrimitiveArrayKeepAllKeepsWholeList() {
        RowType variantRowType =
                VariantMetadataUtils.VariantRowTypeBuilder.builder()
                        .field(DataTypes.INT(), "$.arr[0]")
                        .build();

        GroupType parquetType = createTwoLevelPrimitiveArrayVariantType();
        Type clipped = VariantShreddingTypePruner.clip(variantRowType, parquetType);
        GroupType actual =
                clipped.asGroupType()
                        .getType("typed_value")
                        .asGroupType()
                        .getType(0)
                        .asGroupType()
                        .getType(1)
                        .asGroupType();
        GroupType expected =
                parquetType
                        .asGroupType()
                        .getType("typed_value")
                        .asGroupType()
                        .getType(0)
                        .asGroupType()
                        .getType(1)
                        .asGroupType();
        assertThat(actual).isEqualTo(expected);
    }

    private static GroupType createTwoLevelPrimitiveArrayVariantType() {
        // Primitive array elements are still wrapped in a shredding row.
        GroupType elementRow =
                Types.buildGroup(Type.Repetition.REPEATED)
                        .optional(BINARY)
                        .id(0)
                        .named("value")
                        .optional(INT32)
                        .id(1)
                        .named("typed_value")
                        .named("element")
                        .withId(123);

        // Two-level list: the list group's immediate child is the repeated element row.
        GroupType arrList =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(elementRow)
                        .named("typed_value")
                        .withId(1);

        // arr shredding row: value + typed_value(list)
        GroupType arrShreddingRow =
                Types.buildGroup(Type.Repetition.REQUIRED)
                        .optional(BINARY)
                        .id(0)
                        .named("value")
                        .addField(arrList)
                        .named("arr")
                        .withId(0);

        GroupType typedValue =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .addField(arrShreddingRow)
                        .named("typed_value")
                        .withId(2);

        return Types.buildGroup(Type.Repetition.OPTIONAL)
                .required(PrimitiveTypeName.BINARY)
                .id(0)
                .named("metadata")
                .required(PrimitiveTypeName.BINARY)
                .id(1)
                .named("value")
                .addField(typedValue)
                .named("v")
                .withId(0);
    }

    private static GroupType createParquetVariantType(RowType shreddingSchema) {
        DataField field = new DataField(0, "v", shreddingSchema);
        return ParquetSchemaConverter.convertToParquetType(field).asGroupType();
    }

    private static RowType toRowType(Type type) {
        return (RowType) ParquetSchemaConverter.convertToPaimonField(type).type();
    }

    private static RowType getRowType(RowType rowType, String fieldName) {
        return (RowType) rowType.getTypeAt(rowType.getFieldIndex(fieldName));
    }
}
