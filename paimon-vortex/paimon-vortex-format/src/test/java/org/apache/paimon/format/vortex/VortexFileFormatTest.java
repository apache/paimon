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

package org.apache.paimon.format.vortex;

import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for Vortex file format. */
public class VortexFileFormatTest {

    @Test
    public void testIdentifier() {
        VortexFileFormatFactory factory = new VortexFileFormatFactory();
        assertEquals("vortex", factory.identifier());
    }

    @Test
    public void testCreateFormat() {
        VortexFileFormatFactory factory = new VortexFileFormatFactory();
        VortexFileFormat format =
                factory.create(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        assertEquals("vortex", format.getFormatIdentifier());
    }

    @Test
    public void testCreateReaderFactory() {
        VortexFileFormat format =
                new VortexFileFormat(
                        new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        assertDoesNotThrow(() -> format.createReaderFactory(rowType, rowType, null));
    }

    @Test
    public void testCreateWriterFactory() {
        VortexFileFormat format =
                new VortexFileFormat(
                        new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        assertDoesNotThrow(() -> format.createWriterFactory(rowType));
    }

    @Test
    public void testValidateDataFields_UnsupportedMapType() {
        VortexFileFormat format =
                new VortexFileFormat(
                        new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        RowType rowType = RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        assertThrows(UnsupportedOperationException.class, () -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_UnsupportedMultisetType() {
        VortexFileFormat format =
                new VortexFileFormat(
                        new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        RowType rowType = RowType.of(DataTypes.MULTISET(DataTypes.STRING()));
        assertThrows(UnsupportedOperationException.class, () -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_UnsupportedVariantType() {
        VortexFileFormat format =
                new VortexFileFormat(
                        new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        RowType rowType = RowType.of(DataTypes.VARIANT());
        assertThrows(UnsupportedOperationException.class, () -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_UnsupportedBlobType() {
        VortexFileFormat format =
                new VortexFileFormat(
                        new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        RowType rowType = RowType.of(DataTypes.BLOB());
        assertThrows(UnsupportedOperationException.class, () -> format.validateDataFields(rowType));
    }

    @Test
    public void testValidateDataFields_SupportedTypes() {
        VortexFileFormat format =
                new VortexFileFormat(
                        new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

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
                        .field("tinyint", DataTypes.TINYINT())
                        .field("smallint", DataTypes.SMALLINT())
                        .field("bigint", DataTypes.BIGINT())
                        .field("float", DataTypes.FLOAT())
                        .field("time", DataTypes.TIME())
                        .field("ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
                        .field("vector", DataTypes.VECTOR(3, DataTypes.FLOAT()))
                        .build();

        assertDoesNotThrow(() -> format.validateDataFields(rowType));
    }
}
