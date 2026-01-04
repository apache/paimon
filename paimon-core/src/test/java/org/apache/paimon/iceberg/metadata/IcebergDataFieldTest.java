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

package org.apache.paimon.iceberg.metadata;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergDataFieldTest {

    @Test
    @DisplayName("Test constructor with DataField")
    void testConstructorWithDataField() {
        DataField dataField =
                new DataField(1, "test_field", new IntType(false), "test description");
        IcebergDataField icebergField = new IcebergDataField(dataField);

        assertThat(icebergField.id()).isEqualTo(1);
        assertThat(icebergField.name()).isEqualTo("test_field");
        assertThat(icebergField.required()).isTrue();
        assertThat(icebergField.type()).isEqualTo("int");
        assertThat(icebergField.doc()).isEqualTo("test description");
        assertThat(icebergField.dataType()).isEqualTo(new IntType(false));
    }

    @Test
    @DisplayName("Test constructor with individual parameters")
    void testConstructorWithIndividualParameters() {
        IcebergDataField field = new IcebergDataField(2, "test_name", true, "string", "test doc");

        assertThat(field.id()).isEqualTo(2);
        assertThat(field.name()).isEqualTo("test_name");
        assertThat(field.required()).isTrue();
        assertThat(field.type()).isEqualTo("string");
        assertThat(field.doc()).isEqualTo("test doc");
    }

    @Test
    @DisplayName("Test constructor with DataType")
    void testConstructorWithDataType() {
        DataType dataType = new BooleanType(true);
        IcebergDataField field =
                new IcebergDataField(3, "bool_field", false, "boolean", dataType, "bool doc");

        assertThat(field.id()).isEqualTo(3);
        assertThat(field.name()).isEqualTo("bool_field");
        assertThat(field.required()).isFalse();
        assertThat(field.type()).isEqualTo("boolean");
        assertThat(field.doc()).isEqualTo("bool doc");
        assertThat(field.dataType()).isEqualTo(dataType);
    }

    @Test
    @DisplayName("Test primitive type conversions")
    void testPrimitiveTypeConversions() {
        // Test boolean
        DataField boolField = new DataField(1, "bool", new BooleanType(false));
        IcebergDataField icebergBool = new IcebergDataField(boolField);
        assertThat(icebergBool.type()).isEqualTo("boolean");

        // Test int types
        DataField intField = new DataField(2, "int", new IntType(false));
        IcebergDataField icebergInt = new IcebergDataField(intField);
        assertThat(icebergInt.type()).isEqualTo("int");

        DataField tinyIntField = new DataField(3, "tinyint", new IntType(false));
        IcebergDataField icebergTinyInt = new IcebergDataField(tinyIntField);
        assertThat(icebergTinyInt.type()).isEqualTo("int");

        DataField smallIntField = new DataField(4, "smallint", new IntType(false));
        IcebergDataField icebergSmallInt = new IcebergDataField(smallIntField);
        assertThat(icebergSmallInt.type()).isEqualTo("int");

        // Test bigint
        DataField bigIntField = new DataField(5, "bigint", new BigIntType(false));
        IcebergDataField icebergBigInt = new IcebergDataField(bigIntField);
        assertThat(icebergBigInt.type()).isEqualTo("long");

        // Test float
        DataField floatField = new DataField(6, "float", new FloatType(false));
        IcebergDataField icebergFloat = new IcebergDataField(floatField);
        assertThat(icebergFloat.type()).isEqualTo("float");

        // Test double
        DataField doubleField = new DataField(7, "double", new DoubleType(false));
        IcebergDataField icebergDouble = new IcebergDataField(doubleField);
        assertThat(icebergDouble.type()).isEqualTo("double");

        // Test date
        DataField dateField = new DataField(8, "date", new DateType(false));
        IcebergDataField icebergDate = new IcebergDataField(dateField);
        assertThat(icebergDate.type()).isEqualTo("date");

        // Test string types
        DataField charField = new DataField(9, "char", new VarCharType(false, 10));
        IcebergDataField icebergChar = new IcebergDataField(charField);
        assertThat(icebergChar.type()).isEqualTo("string");

        DataField varcharField = new DataField(10, "varchar", new VarCharType(false, 100));
        IcebergDataField icebergVarchar = new IcebergDataField(varcharField);
        assertThat(icebergVarchar.type()).isEqualTo("string");

        // Test binary types
        DataField binaryField = new DataField(11, "binary", new BinaryType(false, 16));
        IcebergDataField icebergBinary = new IcebergDataField(binaryField);
        assertThat(icebergBinary.type()).isEqualTo("binary");

        DataField varBinaryField = new DataField(12, "varbinary", new VarBinaryType(false, 32));
        IcebergDataField icebergVarBinary = new IcebergDataField(varBinaryField);
        assertThat(icebergVarBinary.type()).isEqualTo("binary");
    }

    @Test
    @DisplayName("Test decimal type conversion")
    void testDecimalTypeConversion() {
        DataField decimalField = new DataField(1, "decimal", new DecimalType(false, 10, 2));
        IcebergDataField icebergDecimal = new IcebergDataField(decimalField);

        assertThat(icebergDecimal.type()).isEqualTo("decimal(10, 2)");
    }

    @Test
    @DisplayName("Test timestamp type conversions")
    void testTimestampTypeConversions() {
        // Test timestamp without timezone
        DataField timestampField = new DataField(1, "timestamp", new TimestampType(false, 6));
        IcebergDataField icebergTimestamp = new IcebergDataField(timestampField);
        assertThat(icebergTimestamp.type()).isEqualTo("timestamp");

        // Test timestamp with local timezone
        DataField timestampLtzField =
                new DataField(2, "timestamptz", new LocalZonedTimestampType(false, 6));
        IcebergDataField icebergTimestampLtz = new IcebergDataField(timestampLtzField);
        assertThat(icebergTimestampLtz.type()).isEqualTo("timestamptz");

        // Test timestamp_ns (precision 7)
        DataField timestampNs7Field = new DataField(3, "timestamp_ns", new TimestampType(false, 7));
        IcebergDataField icebergTimestampNs7 = new IcebergDataField(timestampNs7Field);
        assertThat(icebergTimestampNs7.type()).isEqualTo("timestamp_ns");

        // Test timestamp_ns (precision 8)
        DataField timestampNs8Field = new DataField(4, "timestamp_ns", new TimestampType(false, 8));
        IcebergDataField icebergTimestampNs8 = new IcebergDataField(timestampNs8Field);
        assertThat(icebergTimestampNs8.type()).isEqualTo("timestamp_ns");

        // Test timestamp_ns (precision 9)
        DataField timestampNs9Field = new DataField(5, "timestamp_ns", new TimestampType(false, 9));
        IcebergDataField icebergTimestampNs9 = new IcebergDataField(timestampNs9Field);
        assertThat(icebergTimestampNs9.type()).isEqualTo("timestamp_ns");

        // Test timestamptz_ns (precision 7)
        DataField timestampLtzNs7Field =
                new DataField(6, "timestamptz_ns", new LocalZonedTimestampType(false, 7));
        IcebergDataField icebergTimestampLtzNs7 = new IcebergDataField(timestampLtzNs7Field);
        assertThat(icebergTimestampLtzNs7.type()).isEqualTo("timestamptz_ns");

        // Test timestamptz_ns (precision 8)
        DataField timestampLtzNs8Field =
                new DataField(7, "timestamptz_ns", new LocalZonedTimestampType(false, 8));
        IcebergDataField icebergTimestampLtzNs8 = new IcebergDataField(timestampLtzNs8Field);
        assertThat(icebergTimestampLtzNs8.type()).isEqualTo("timestamptz_ns");

        // Test timestamptz_ns (precision 9)
        DataField timestampLtzNs9Field =
                new DataField(8, "timestamptz_ns", new LocalZonedTimestampType(false, 9));
        IcebergDataField icebergTimestampLtzNs9 = new IcebergDataField(timestampLtzNs9Field);
        assertThat(icebergTimestampLtzNs9.type()).isEqualTo("timestamptz_ns");
    }

    @Test
    @DisplayName("Test timestamp precision validation")
    void testTimestampPrecisionValidation() {
        // Test invalid precision (<= 3)
        DataField invalidTimestampField =
                new DataField(1, "timestamp", new TimestampType(false, 2));
        assertThatThrownBy(() -> new IcebergDataField(invalidTimestampField))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Paimon Iceberg compatibility only support timestamp type with precision from 3 to 9");

        // Test invalid precision (<= 3)
        DataField invalidTimestampField2 =
                new DataField(2, "timestamp", new TimestampType(false, 2));
        assertThatThrownBy(() -> new IcebergDataField(invalidTimestampField2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Paimon Iceberg compatibility only support timestamp type with precision from 3 to 9");

        // Test invalid local timezone timestamp precision (<= 3)
        DataField invalidTimestampLtzField =
                new DataField(3, "timestamptz", new LocalZonedTimestampType(false, 2));
        assertThatThrownBy(() -> new IcebergDataField(invalidTimestampLtzField))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Paimon Iceberg compatibility only support timestamp type with precision from 3 to 9");

        // Test valid precision boundaries
        DataField validTimestamp4 = new DataField(4, "timestamp", new TimestampType(false, 4));
        IcebergDataField icebergTimestamp4 = new IcebergDataField(validTimestamp4);
        assertThat(icebergTimestamp4.type()).isEqualTo("timestamp");

        DataField validTimestamp9 = new DataField(5, "timestamp", new TimestampType(false, 9));
        IcebergDataField icebergTimestamp9 = new IcebergDataField(validTimestamp9);
        assertThat(icebergTimestamp9.type()).isEqualTo("timestamp_ns");

        DataField validTimestampLtz4 =
                new DataField(6, "timestamptz", new LocalZonedTimestampType(false, 4));
        IcebergDataField icebergTimestampLtz4 = new IcebergDataField(validTimestampLtz4);
        assertThat(icebergTimestampLtz4.type()).isEqualTo("timestamptz");

        DataField validTimestampLtz9 =
                new DataField(7, "timestamptz", new LocalZonedTimestampType(false, 9));
        IcebergDataField icebergTimestampLtz9 = new IcebergDataField(validTimestampLtz9);
        assertThat(icebergTimestampLtz9.type()).isEqualTo("timestamptz_ns");
    }

    @Test
    @DisplayName("Test array type conversion")
    void testArrayTypeConversion() {
        DataField arrayField = new DataField(1, "array", new ArrayType(false, new IntType(false)));
        IcebergDataField icebergArray = new IcebergDataField(arrayField);

        assertThat(icebergArray.type()).isInstanceOf(IcebergListType.class);
        IcebergListType listType = (IcebergListType) icebergArray.type();
        assertThat(listType.type()).isEqualTo("list");
        assertThat(listType.element()).isEqualTo("int");
        assertThat(listType.elementRequired()).isTrue();
    }

    @Test
    @DisplayName("Test map type conversion")
    void testMapTypeConversion() {
        DataField mapField =
                new DataField(
                        1,
                        "map",
                        new MapType(
                                false,
                                new VarCharType(false, VarCharType.MAX_LENGTH),
                                new IntType(false)));
        IcebergDataField icebergMap = new IcebergDataField(mapField);

        assertThat(icebergMap.type()).isInstanceOf(IcebergMapType.class);
        IcebergMapType mapType = (IcebergMapType) icebergMap.type();
        assertThat(mapType.type()).isEqualTo("map");
        assertThat(mapType.key()).isEqualTo("string");
        assertThat(mapType.value()).isEqualTo("int");
        assertThat(mapType.valueRequired()).isTrue();
    }

    @Test
    @DisplayName("Test row type conversion")
    void testRowTypeConversion() {
        List<DataField> nestedFields =
                Arrays.asList(
                        new DataField(2, "nested_int", new IntType(false)),
                        new DataField(
                                3,
                                "nested_string",
                                new VarCharType(false, VarCharType.MAX_LENGTH)));
        DataField rowField = new DataField(1, "row", new RowType(false, nestedFields));
        IcebergDataField icebergRow = new IcebergDataField(rowField);

        assertThat(icebergRow.type()).isInstanceOf(IcebergStructType.class);
        IcebergStructType structType = (IcebergStructType) icebergRow.type();
        assertThat(structType.type()).isEqualTo("struct");
        assertThat(structType.fields()).hasSize(2);
        assertThat(structType.fields().get(0).name()).isEqualTo("nested_int");
        assertThat(structType.fields().get(1).name()).isEqualTo("nested_string");
    }

    @Test
    @DisplayName("Test dataType() method with cached value")
    void testDataTypeMethodWithCachedValue() {
        DataField dataField = new DataField(1, "test", new IntType(false));
        IcebergDataField icebergField = new IcebergDataField(dataField);

        // First call should compute and cache
        DataType firstCall = icebergField.dataType();
        assertThat(firstCall).isEqualTo(new IntType(false));

        // Second call should return cached value
        DataType secondCall = icebergField.dataType();
        assertThat(secondCall).isSameAs(firstCall);
    }

    @Test
    @DisplayName("Test dataType() method without cached value")
    void testDataTypeMethodWithoutCachedValue() {
        IcebergDataField field = new IcebergDataField(1, "test", false, "int", null, "doc");

        DataType dataType = field.dataType();
        assertThat(dataType).isEqualTo(new IntType(true));
    }

    @Test
    @DisplayName("Test toDatafield() method")
    void testToDatafieldMethod() {
        DataField originalField =
                new DataField(
                        1,
                        "test_field",
                        new VarCharType(false, VarCharType.MAX_LENGTH),
                        "test description");
        IcebergDataField icebergField = new IcebergDataField(originalField);

        DataField convertedField = icebergField.toDatafield();

        assertThat(convertedField.id()).isEqualTo(1);
        assertThat(convertedField.name()).isEqualTo("test_field");
        assertThat(convertedField.type()).isEqualTo(new VarCharType(false, VarCharType.MAX_LENGTH));
        assertThat(convertedField.description()).isEqualTo("test description");
    }

    @Test
    @DisplayName("Test primitive type parsing from string")
    void testPrimitiveTypeParsingFromString() {
        // Test boolean
        IcebergDataField boolField = new IcebergDataField(1, "bool", false, "boolean", null, "doc");
        assertThat(boolField.dataType()).isEqualTo(new BooleanType(true));

        // Test int
        IcebergDataField intField = new IcebergDataField(2, "int", false, "int", null, "doc");
        assertThat(intField.dataType()).isEqualTo(new IntType(true));

        // Test long
        IcebergDataField longField = new IcebergDataField(3, "long", false, "long", null, "doc");
        assertThat(longField.dataType()).isEqualTo(new BigIntType(true));

        // Test float
        IcebergDataField floatField = new IcebergDataField(4, "float", false, "float", null, "doc");
        assertThat(floatField.dataType()).isEqualTo(new FloatType(true));

        // Test double
        IcebergDataField doubleField =
                new IcebergDataField(5, "double", false, "double", null, "doc");
        assertThat(doubleField.dataType()).isEqualTo(new DoubleType(true));

        // Test date
        IcebergDataField dateField = new IcebergDataField(6, "date", false, "date", null, "doc");
        assertThat(dateField.dataType()).isEqualTo(new DateType(true));

        // Test string
        IcebergDataField stringField =
                new IcebergDataField(7, "string", false, "string", null, "doc");
        assertThat(stringField.dataType()).isEqualTo(new VarCharType(true, VarCharType.MAX_LENGTH));

        // Test binary
        IcebergDataField binaryField =
                new IcebergDataField(8, "binary", false, "binary", null, "doc");
        assertThat(binaryField.dataType())
                .isEqualTo(new VarBinaryType(true, VarBinaryType.MAX_LENGTH));
    }

    @Test
    @DisplayName("Test fixed type parsing")
    void testFixedTypeParsing() {
        IcebergDataField fixedField =
                new IcebergDataField(1, "fixed", false, "fixed[32]", null, "doc");
        assertThat(fixedField.dataType()).isEqualTo(new BinaryType(true, 32));
    }

    @Test
    @DisplayName("Test uuid type parsing")
    void testUuidTypeParsing() {
        IcebergDataField uuidField = new IcebergDataField(1, "uuid", false, "uuid", null, "doc");
        assertThat(uuidField.dataType()).isEqualTo(new BinaryType(true, 16));
    }

    @Test
    @DisplayName("Test decimal type parsing")
    void testDecimalTypeParsing() {
        IcebergDataField decimalField =
                new IcebergDataField(1, "decimal", false, "decimal(10, 2)", null, "doc");
        assertThat(decimalField.dataType()).isEqualTo(new DecimalType(true, 10, 2));
    }

    @Test
    @DisplayName("Test timestamp type parsing")
    void testTimestampTypeParsing() {
        // Test timestamp
        IcebergDataField timestampField =
                new IcebergDataField(1, "timestamp", false, "timestamp", null, "doc");
        assertThat(timestampField.dataType()).isEqualTo(new TimestampType(true, 6));

        // Test timestamptz
        IcebergDataField timestamptzField =
                new IcebergDataField(2, "timestamptz", false, "timestamptz", null, "doc");
        assertThat(timestamptzField.dataType()).isEqualTo(new LocalZonedTimestampType(true, 6));

        // Test timestamp_ns (iceberg v3 format)
        IcebergDataField timestampNsField =
                new IcebergDataField(3, "timestamp_ns", false, "timestamp_ns", null, "doc");
        assertThat(timestampNsField.dataType()).isEqualTo(new TimestampType(true, 9));

        // Test timestamptz_ns (iceberg v3 format)
        IcebergDataField timestamptzNsField =
                new IcebergDataField(4, "timestamptz_ns", false, "timestamptz_ns", null, "doc");
        assertThat(timestamptzNsField.dataType()).isEqualTo(new LocalZonedTimestampType(true, 9));
    }

    @Test
    @DisplayName("Test unsupported primitive type parsing")
    void testUnsupportedPrimitiveTypeParsing() {
        IcebergDataField unsupportedField =
                new IcebergDataField(1, "unsupported", false, "unsupported_type", null, "doc");
        assertThatThrownBy(() -> unsupportedField.dataType())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported primitive data type");
    }

    @Test
    @DisplayName("Test nested type parsing")
    void testNestedTypeParsing() {
        // Test list type
        IcebergListType listType = new IcebergListType(2, false, "int");
        IcebergDataField listField = new IcebergDataField(1, "list", false, listType, null, "doc");
        assertThat(listField.dataType()).isEqualTo(new ArrayType(true, new IntType(true)));

        // Test map type
        IcebergMapType mapType = new IcebergMapType(2, "string", 3, false, "int");
        IcebergDataField mapField = new IcebergDataField(1, "map", false, mapType, null, "doc");
        assertThat(mapField.dataType())
                .isEqualTo(
                        new MapType(
                                true,
                                new VarCharType(false, VarCharType.MAX_LENGTH),
                                new IntType(true)));

        // Test struct type
        List<IcebergDataField> structFields =
                Arrays.asList(
                        new IcebergDataField(2, "field1", false, "int", null, "doc1"),
                        new IcebergDataField(3, "field2", false, "string", null, "doc2"));
        IcebergStructType structType = new IcebergStructType(structFields);
        IcebergDataField structField =
                new IcebergDataField(1, "struct", false, structType, null, "doc");

        DataType resultType = structField.dataType();
        assertThat(resultType).isInstanceOf(RowType.class);
        RowType rowType = (RowType) resultType;
        assertThat(rowType.getFields()).hasSize(2);
        assertThat(rowType.getFields().get(0).name()).isEqualTo("field1");
        assertThat(rowType.getFields().get(1).name()).isEqualTo("field2");
    }

    @Test
    @DisplayName("Test unsupported nested type parsing")
    void testUnsupportedNestedTypeParsing() {
        Object unsupportedNestedType = new Object();
        IcebergDataField field =
                new IcebergDataField(1, "unsupported", false, unsupportedNestedType, null, "doc");

        assertThatThrownBy(() -> field.dataType())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported nested data type");
    }

    @Test
    @DisplayName("Test required field conversion")
    void testRequiredFieldConversion() {
        // Test nullable field becomes not required
        DataField nullableField = new DataField(1, "nullable", new IntType(true));
        IcebergDataField icebergNullable = new IcebergDataField(nullableField);
        assertThat(icebergNullable.required()).isFalse();

        // Test not nullable field becomes required
        DataField notNullableField = new DataField(2, "not_nullable", new IntType(false));
        IcebergDataField icebergNotNullable = new IcebergDataField(notNullableField);
        assertThat(icebergNotNullable.required()).isTrue();
    }

    @Test
    @DisplayName("Test complex nested type conversion")
    void testComplexNestedTypeConversion() {
        // Create a complex nested structure: Map<String, Array<Row<Int, String>>>
        List<DataField> rowFields =
                Arrays.asList(
                        new DataField(3, "nested_int", new IntType(false)),
                        new DataField(
                                4,
                                "nested_string",
                                new VarCharType(false, VarCharType.MAX_LENGTH)));
        RowType nestedRowType = new RowType(false, rowFields);
        ArrayType arrayType = new ArrayType(false, nestedRowType);
        MapType mapType =
                new MapType(false, new VarCharType(false, VarCharType.MAX_LENGTH), arrayType);

        DataField complexField = new DataField(1, "complex", mapType);
        IcebergDataField icebergComplex = new IcebergDataField(complexField);

        assertThat(icebergComplex.type()).isInstanceOf(IcebergMapType.class);
        IcebergMapType icebergMap = (IcebergMapType) icebergComplex.type();

        // Check key type
        assertThat(icebergMap.key()).isEqualTo("string");

        // Check value type (should be list)
        assertThat(icebergMap.value()).isInstanceOf(IcebergListType.class);
        IcebergListType icebergList = (IcebergListType) icebergMap.value();
        assertThat(icebergList.element()).isInstanceOf(IcebergStructType.class);

        // Check nested struct fields
        IcebergStructType icebergStruct = (IcebergStructType) icebergList.element();
        assertThat(icebergStruct.fields()).hasSize(2);
        assertThat(icebergStruct.fields().get(0).type()).isEqualTo("int");
        assertThat(icebergStruct.fields().get(1).type()).isEqualTo("string");
    }
}
