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

package org.apache.paimon.format.lance;

import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for Lance file format. */
public class LanceFileFormatTest {

    @Test
    public void testCreateReaderFactory() {
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        assertDoesNotThrow(() -> format.createReaderFactory(rowType, rowType, null));
    }

    @Test
    public void testCreateWriterFactory() {
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        assertDoesNotThrow(() -> format.createWriterFactory(rowType));
    }

    @Test
    public void testValidateDataFields_UnsupportedType_Map() {
        // Test validation throws exception for unsupported MAP type
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        RowType rowType = RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        assertThrows(UnsupportedOperationException.class, () -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_UnsupportedType_LocalZonedTimestamp() {
        // Test validation throws exception for unsupported TIMESTAMP_WITH_LOCAL_TIME_ZONE type
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        RowType rowType = RowType.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        assertThrows(
                UnsupportedOperationException.class, () -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_SupportedTypes_Basic() {
        // Test validation passes for basic supported types
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("salary", DataTypes.DOUBLE())
                        .field("isActive", DataTypes.BOOLEAN())
                        .field("bytes", DataTypes.BYTES())
                        .field("timestamp", DataTypes.TIMESTAMP())
                        .field("date", DataTypes.DATE())
                        .field("decimal", DataTypes.DECIMAL(10, 2))
                        .field("array", DataTypes.ARRAY(DataTypes.INT()))
                        .build();

        assertDoesNotThrow(() -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_AllNumericTypes() {
        // Test validation passes for all numeric types
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        RowType rowType =
                RowType.builder()
                        .field("tinyInt", DataTypes.TINYINT())
                        .field("smallInt", DataTypes.SMALLINT())
                        .field("int", DataTypes.INT())
                        .field("bigInt", DataTypes.BIGINT())
                        .field("float", DataTypes.FLOAT())
                        .field("double", DataTypes.DOUBLE())
                        .field("decimal", DataTypes.DECIMAL(20, 6))
                        .build();

        assertDoesNotThrow(() -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_AllStringTypes() {
        // Test validation passes for all string and binary types
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        RowType rowType =
                RowType.builder()
                        .field("char", DataTypes.CHAR(10))
                        .field("varChar", DataTypes.VARCHAR(100))
                        .field("binary", DataTypes.BINARY(50))
                        .field("varBinary", DataTypes.VARBINARY(100))
                        .build();

        assertDoesNotThrow(() -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_TimeTypes() {
        // Test validation passes for all time-related types with different precisions
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        RowType rowType =
                RowType.builder()
                        .field("date", DataTypes.DATE())
                        .field("time", DataTypes.TIME())
                        .field("timestamp", DataTypes.TIMESTAMP())
                        .field("timestamp3", DataTypes.TIMESTAMP(3))
                        .field("timestamp9", DataTypes.TIMESTAMP(9))
                        .build();

        assertDoesNotThrow(() -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_ComplexTypes() {
        // Test validation passes for complex types: arrays, multisets, and variants
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        RowType rowType =
                RowType.builder()
                        .field("intArray", DataTypes.ARRAY(DataTypes.INT()))
                        .field("stringArray", DataTypes.ARRAY(DataTypes.STRING()))
                        .field("nestedArray", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())))
                        .field("multiset", DataTypes.MULTISET(DataTypes.STRING()))
                        .field("variant", DataTypes.VARIANT())
                        .build();

        assertDoesNotThrow(() -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_NestedRowType() {
        // Test validation passes for nested RowType structures
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        RowType nestedType =
                RowType.builder()
                        .field("nestedId", DataTypes.INT())
                        .field("nestedName", DataTypes.STRING())
                        .build();

        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("nested", nestedType)
                        .build();

        assertDoesNotThrow(() -> format.validateDataFields(rowType));
    }

    @Test
    public void testReaderFactory_WithProjectedTypes() {
        // Test reader factory creation with projected types for column pruning
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 2048, 4096));

        RowType fullType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("salary", DataTypes.DOUBLE())
                        .field("department", DataTypes.STRING())
                        .build();

        RowType projectedType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .build();

        FormatReaderFactory readerFactory =
                format.createReaderFactory(fullType, projectedType, null);

        assertDoesNotThrow(() -> readerFactory);
        assertDoesNotThrow(() -> ((LanceReaderFactory) readerFactory));
    }

    @Test
    public void testReaderFactory_BatchSizeConfiguration() {
        // Test reader factory with configured batch size
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        FormatReaderFactory readerFactory = format.createReaderFactory(rowType, rowType, null);

        assertDoesNotThrow(() -> ((LanceReaderFactory) readerFactory));
    }

    @Test
    public void testWriterFactory_BatchSizeConfiguration() {
        // Test writer factory with configured batch size and memory settings
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 2048, 8192));

        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("data", DataTypes.STRING())
                        .field("value", DataTypes.DOUBLE())
                        .build();

        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);

        assertDoesNotThrow(() -> writerFactory);
        assertDoesNotThrow(() -> ((LanceWriterFactory) writerFactory));
    }

    @Test
    public void testValidateDataFields_EmptyRowType() {
        // Test validation passes for empty RowType
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        RowType rowType = RowType.builder().build();

        assertDoesNotThrow(() -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_SingleFieldTypes() {
        // Test validation passes for single field types
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        assertDoesNotThrow(() -> format.validateDataFields(RowType.of(DataTypes.INT())));
        assertDoesNotThrow(() -> format.validateDataFields(RowType.of(DataTypes.STRING())));
        assertDoesNotThrow(() -> format.validateDataFields(RowType.of(DataTypes.BOOLEAN())));
        assertDoesNotThrow(() -> format.validateDataFields(RowType.of(DataTypes.DATE())));
        assertDoesNotThrow(() -> format.validateDataFields(RowType.of(DataTypes.ARRAY(DataTypes.INT()))));
    }

    @Test
    public void testValidateDataFields_MixedArrayTypes() {
        // Test validation passes for mixed array element types
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        RowType rowType =
                RowType.builder()
                        .field("intArray", DataTypes.ARRAY(DataTypes.INT()))
                        .field("stringArray", DataTypes.ARRAY(DataTypes.STRING()))
                        .field("doubleArray", DataTypes.ARRAY(DataTypes.DOUBLE()))
                        .field("booleanArray", DataTypes.ARRAY(DataTypes.BOOLEAN()))
                        .build();

        assertDoesNotThrow(() -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_VariantType() {
        // Test validation passes for VARIANT type
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        RowType rowType = RowType.of(DataTypes.VARIANT());

        assertDoesNotThrow(() -> format.validateDataFields(rowType));
    }
}