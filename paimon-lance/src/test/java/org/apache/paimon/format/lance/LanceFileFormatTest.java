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
    public void testValidateDataFields_UnsupportedType() {
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        RowType rowType = RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        assertThrows(UnsupportedOperationException.class, () -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_SupportedTypes() {
        LanceFileFormat format =
                new LanceFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        // Define a row type with various supported data types
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

        // Validate that no exception is thrown for supported types
        assertDoesNotThrow(() -> format.validateDataFields(rowType));
    }
}
