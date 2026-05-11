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

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import dev.vortex.api.DType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Test for {@link VortexTypeUtils}. */
public class VortexTypeUtilsTest {

    @BeforeAll
    static void checkNativeLibrary() {
        assumeTrue(isNativeAvailable(), "Vortex native library not available, skipping tests");
    }

    private static boolean isNativeAvailable() {
        try {
            dev.vortex.jni.NativeLoader.loadJni();
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    @Test
    public void testSimpleTypes() {
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_bigint", DataTypes.BIGINT())
                        .field("f_string", DataTypes.STRING())
                        .field("f_boolean", DataTypes.BOOLEAN())
                        .field("f_double", DataTypes.DOUBLE())
                        .field("f_float", DataTypes.FLOAT())
                        .build();

        DType dtype = VortexTypeUtils.toDType(rowType);
        assertEquals(DType.Variant.STRUCT, dtype.getVariant());
        assertEquals(6, dtype.getFieldNames().size());
        assertEquals("f_int", dtype.getFieldNames().get(0));
        assertEquals("f_bigint", dtype.getFieldNames().get(1));
        assertEquals("f_string", dtype.getFieldNames().get(2));
        assertEquals("f_boolean", dtype.getFieldNames().get(3));
        assertEquals("f_double", dtype.getFieldNames().get(4));
        assertEquals("f_float", dtype.getFieldNames().get(5));
    }

    @Test
    public void testDecimalType() {
        RowType rowType = RowType.builder().field("f_decimal", DataTypes.DECIMAL(10, 2)).build();

        DType dtype = VortexTypeUtils.toDType(rowType);
        DType decimalField = dtype.getFieldTypes().get(0);
        assertEquals(DType.Variant.DECIMAL, decimalField.getVariant());
        assertEquals(10, decimalField.getPrecision());
        assertEquals(2, decimalField.getScale());
    }

    @Test
    public void testArrayType() {
        RowType rowType =
                RowType.builder().field("f_array", DataTypes.ARRAY(DataTypes.INT())).build();

        DType dtype = VortexTypeUtils.toDType(rowType);
        DType arrayField = dtype.getFieldTypes().get(0);
        assertEquals(DType.Variant.LIST, arrayField.getVariant());
    }

    @Test
    public void testNestedRowType() {
        RowType innerType =
                RowType.builder()
                        .field("inner_int", DataTypes.INT())
                        .field("inner_str", DataTypes.STRING())
                        .build();
        RowType rowType = RowType.builder().field("f_row", innerType).build();

        DType dtype = VortexTypeUtils.toDType(rowType);
        DType rowField = dtype.getFieldTypes().get(0);
        assertEquals(DType.Variant.STRUCT, rowField.getVariant());
        assertEquals(2, rowField.getFieldNames().size());
        assertEquals("inner_int", rowField.getFieldNames().get(0));
        assertEquals("inner_str", rowField.getFieldNames().get(1));
    }

    @Test
    public void testUnsupportedMapType() {
        RowType rowType =
                RowType.builder()
                        .field("f_map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .build();

        assertThrows(UnsupportedOperationException.class, () -> VortexTypeUtils.toDType(rowType));
    }

    @Test
    public void testNullability() {
        RowType rowType =
                RowType.builder()
                        .field("f_nullable", DataTypes.INT().nullable())
                        .field("f_not_null", DataTypes.INT().notNull())
                        .build();

        DType dtype = VortexTypeUtils.toDType(rowType);
        assertEquals(true, dtype.getFieldTypes().get(0).isNullable());
        assertEquals(false, dtype.getFieldTypes().get(1).isNullable());
    }
}
