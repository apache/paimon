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

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test class for DebeziumSchemaUtils. */
public class DebeziumSchemaUtilsTest {
    @Test
    public void testFromDebeziumAvroTypeWithGenericUnion() {
        // Create a union schema with multiple non-null types
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Schema intSchema = Schema.create(Schema.Type.INT);
        Schema unionSchema = Schema.createUnion(Arrays.asList(stringSchema, intSchema));

        // Test that an exception is thrown for generic unions
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> DebeziumSchemaUtils.avroToPaimonDataType(unionSchema));

        assertEquals("Generic unions are not supported", exception.getMessage());
    }

    @Test
    public void testFromDebeziumAvroTypeWithLogicalTypes() {
        // Test date logical type
        Schema dateSchema = Schema.create(Schema.Type.INT);
        LogicalTypes.date().addToSchema(dateSchema);
        DataType dateType = DebeziumSchemaUtils.avroToPaimonDataType(dateSchema);
        assertEquals(DataTypes.DATE(), dateType);

        // Test decimal logical type
        Schema decimalSchema = Schema.create(Schema.Type.BYTES);
        LogicalTypes.decimal(10, 2).addToSchema(decimalSchema);
        DataType decimalType = DebeziumSchemaUtils.avroToPaimonDataType(decimalSchema);
        assertEquals(DataTypes.DECIMAL(10, 2), decimalType);

        // Test timestamp-millis logical type
        Schema timestampMillisSchema = Schema.create(Schema.Type.LONG);
        LogicalTypes.timestampMillis().addToSchema(timestampMillisSchema);
        DataType timestampMillisType =
                DebeziumSchemaUtils.avroToPaimonDataType(timestampMillisSchema);
        assertEquals(DataTypes.TIMESTAMP_MILLIS(), timestampMillisType);

        // Test timestamp-micros logical type
        Schema timestampMicrosSchema = Schema.create(Schema.Type.LONG);
        LogicalTypes.timestampMicros().addToSchema(timestampMicrosSchema);
        DataType timestampMicrosType =
                DebeziumSchemaUtils.avroToPaimonDataType(timestampMicrosSchema);
        assertEquals(DataTypes.TIMESTAMP(), timestampMicrosType);
    }

    @Test
    public void testFromDebeziumAvroTypeWithPrimitiveTypes() {
        // Test boolean type
        Schema booleanSchema = Schema.create(Schema.Type.BOOLEAN);
        DataType booleanType = DebeziumSchemaUtils.avroToPaimonDataType(booleanSchema);
        assertEquals(DataTypes.BOOLEAN(), booleanType);

        // Test int type
        Schema intSchema = Schema.create(Schema.Type.INT);
        DataType intType = DebeziumSchemaUtils.avroToPaimonDataType(intSchema);
        assertEquals(DataTypes.INT(), intType);

        // Test long type
        Schema longSchema = Schema.create(Schema.Type.LONG);
        DataType longType = DebeziumSchemaUtils.avroToPaimonDataType(longSchema);
        assertEquals(DataTypes.BIGINT(), longType);

        // Test float type
        Schema floatSchema = Schema.create(Schema.Type.FLOAT);
        DataType floatType = DebeziumSchemaUtils.avroToPaimonDataType(floatSchema);
        assertEquals(DataTypes.FLOAT(), floatType);

        // Test double type
        Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);
        DataType doubleType = DebeziumSchemaUtils.avroToPaimonDataType(doubleSchema);
        assertEquals(DataTypes.DOUBLE(), doubleType);

        // Test enum type
        Schema enumSchema =
                Schema.createEnum("TestEnum", null, null, Arrays.asList("VALUE1", "VALUE2"));
        DataType enumType = DebeziumSchemaUtils.avroToPaimonDataType(enumSchema);
        assertEquals(DataTypes.STRING(), enumType);

        // Test string type
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        DataType stringType = DebeziumSchemaUtils.avroToPaimonDataType(stringSchema);
        assertEquals(DataTypes.STRING(), stringType);

        // Test bytes type
        Schema bytesSchema = Schema.create(Schema.Type.BYTES);
        DataType bytesType = DebeziumSchemaUtils.avroToPaimonDataType(bytesSchema);
        assertEquals(DataTypes.BYTES(), bytesType);
    }

    @Test
    public void testFromDebeziumAvroTypeWithComplexTypes() {
        // Test array type
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Schema arraySchema = Schema.createArray(stringSchema);
        DataType arrayType = DebeziumSchemaUtils.avroToPaimonDataType(arraySchema);
        assertEquals(DataTypes.ARRAY(DataTypes.STRING()), arrayType);

        // Test map type
        Schema mapSchema = Schema.createMap(stringSchema);
        DataType mapType = DebeziumSchemaUtils.avroToPaimonDataType(mapSchema);
        assertEquals(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()), mapType);

        // Test record type
        Schema recordSchema = Schema.createRecord("TestRecord", null, null, false);
        List<Schema.Field> fields =
                Arrays.asList(
                        new Schema.Field("field1", stringSchema, null, null),
                        new Schema.Field("field2", Schema.create(Schema.Type.INT), null, null));
        recordSchema.setFields(fields);
        DataType recordType = DebeziumSchemaUtils.avroToPaimonDataType(recordSchema);

        DataField[] expectedFields =
                new DataField[] {
                    DataTypes.FIELD(0, "field1", DataTypes.STRING(), null),
                    DataTypes.FIELD(1, "field2", DataTypes.INT(), null)
                };
        assertEquals(DataTypes.ROW(expectedFields), recordType);
    }
}
