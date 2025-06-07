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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** IT cases for {@link UpdatedDataFieldsProcessFunctionBaseTest}. */
public class UpdatedDataFieldsProcessFunctionBaseTest {

    @Test
    public void testCanConvertString() {
        VarCharType oldVarchar = new VarCharType(true, 10);
        VarCharType biggerLengthVarchar = new VarCharType(true, 20);
        VarCharType smallerLengthVarchar = new VarCharType(true, 5);

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction = null;
        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldVarchar, biggerLengthVarchar, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldVarchar, smallerLengthVarchar, TypeMapping.defaultMapping());

        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.IGNORE, convertAction);
    }

    @Test
    public void testCanConvertNumber() {
        IntType oldType = new IntType();
        BigIntType bigintType = new BigIntType();
        SmallIntType smallintType = new SmallIntType();

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction = null;
        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, bigintType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, smallintType, TypeMapping.defaultMapping());

        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.IGNORE, convertAction);
    }

    @Test
    public void testCanConvertDecimal() {
        DecimalType oldType = new DecimalType(20, 9);
        DecimalType biggerRangeType = new DecimalType(30, 10);
        DecimalType smallerRangeType = new DecimalType(10, 3);

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction = null;
        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, biggerRangeType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, smallerRangeType, TypeMapping.defaultMapping());

        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.IGNORE, convertAction);
    }

    @Test
    public void testCanConvertTimestamp() {
        TimestampType oldType = new TimestampType(true, 3);
        TimestampType biggerLengthTimestamp = new TimestampType(true, 5);
        TimestampType smallerLengthTimestamp = new TimestampType(true, 2);

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction = null;
        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, biggerLengthTimestamp, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, smallerLengthTimestamp, TypeMapping.defaultMapping());

        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.IGNORE, convertAction);
    }

    @Test
    public void testArrayNullabilityEvolution() {
        // Test 1: Nullable array to non-nullable array (should fail)
        ArrayType oldType = new ArrayType(true, DataTypes.INT());
        ArrayType newType = new ArrayType(false, DataTypes.INT());

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);

        // Test 2: Non-nullable array to nullable array (should succeed)
        oldType = new ArrayType(false, DataTypes.INT());
        newType = new ArrayType(true, DataTypes.INT());

        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);

        // Test 3: Array with nullable elements to non-nullable elements (should fail)
        oldType = new ArrayType(true, new IntType(true));
        newType = new ArrayType(true, new IntType(false));

        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);
    }

    @Test
    public void testNestedRowNullabilityEvolution() {
        // Old type: ROW(f1 ROW(inner1 INT NULL, inner2 STRING) NULL)
        RowType oldInnerType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "inner1", new IntType(true)),
                                new DataField(2, "inner2", DataTypes.STRING())));
        RowType oldRowType = new RowType(true, Arrays.asList(new DataField(1, "f1", oldInnerType)));

        // Test 1: Making nested row non-nullable (should fail)
        RowType newInnerType =
                new RowType(
                        false, // Changed to non-nullable
                        Arrays.asList(
                                new DataField(1, "inner1", new IntType(true)),
                                new DataField(2, "inner2", DataTypes.STRING())));
        RowType newRowType = new RowType(true, Arrays.asList(new DataField(1, "f1", newInnerType)));

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldRowType, newRowType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);

        // Test 2: Making nested field non-nullable (should fail)
        newInnerType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(
                                        1, "inner1", new IntType(false)), // Changed to non-nullable
                                new DataField(2, "inner2", DataTypes.STRING())));
        newRowType = new RowType(true, Arrays.asList(new DataField(1, "f1", newInnerType)));

        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldRowType, newRowType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);
    }

    @Test
    public void testMultisetNullabilityEvolution() {
        // Test 1: Nullable multiset to non-nullable multiset (should fail)
        MultisetType oldType = new MultisetType(true, DataTypes.INT());
        MultisetType newType = new MultisetType(false, DataTypes.INT());

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);

        // Test 2: Non-nullable multiset to nullable multiset (should succeed)
        oldType = new MultisetType(false, DataTypes.INT());
        newType = new MultisetType(true, DataTypes.INT());

        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);

        // Test 3: Multiset with nullable elements to non-nullable elements (should fail)
        oldType = new MultisetType(true, new IntType(true));
        newType = new MultisetType(true, new IntType(false));

        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);
    }

    @Test
    public void testComplexNestedNullabilityEvolution() {
        // Old type: ROW(
        //   f1 ARRAY<ROW<x INT NULL, y STRING> NULL> NULL,
        //   f2 MAP<STRING, ROW<a INT NULL, b STRING> NULL> NULL
        // )
        RowType oldArrayInnerType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "x", new IntType(true)),
                                new DataField(2, "y", DataTypes.STRING())));
        ArrayType oldArrayType = new ArrayType(true, oldArrayInnerType);

        RowType oldMapValueType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "a", new IntType(true)),
                                new DataField(2, "b", DataTypes.STRING())));
        MapType oldMapType = new MapType(true, DataTypes.STRING(), oldMapValueType);

        RowType oldRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "f1", oldArrayType),
                                new DataField(2, "f2", oldMapType)));

        // Test 1: Making nested array's row type non-nullable (should fail)
        RowType newArrayInnerType =
                new RowType(
                        false, // Changed to non-nullable
                        Arrays.asList(
                                new DataField(1, "x", new IntType(true)),
                                new DataField(2, "y", DataTypes.STRING())));
        ArrayType newArrayType = new ArrayType(true, newArrayInnerType);
        MapType newMapType = oldMapType; // Keep the same

        RowType newRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "f1", newArrayType),
                                new DataField(2, "f2", newMapType)));

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldRowType, newRowType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);

        // Test 2: Making map's value type's field non-nullable (should fail)
        newArrayType = oldArrayType; // Restore to original
        RowType newMapValueType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(
                                        1, "a", new IntType(false)), // Changed to non-nullable
                                new DataField(2, "b", DataTypes.STRING())));
        newMapType = new MapType(true, DataTypes.STRING(), newMapValueType);

        newRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "f1", newArrayType),
                                new DataField(2, "f2", newMapType)));

        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldRowType, newRowType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);
    }

    @Test
    public void testNestedRowTypeConversion() {
        // Old type: ROW(f1 INT, f2 STRING)
        RowType oldRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "f1", DataTypes.INT()),
                                new DataField(2, "f2", DataTypes.STRING())));

        // New type: ROW(f1 BIGINT, f2 STRING, f3 DOUBLE)
        RowType newRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "f1", DataTypes.BIGINT()),
                                new DataField(2, "f2", DataTypes.STRING()),
                                new DataField(3, "f3", DataTypes.DOUBLE())));

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldRowType, newRowType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
    }

    @Test
    public void testNestedArrayTypeConversion() {
        // Old type: ARRAY<INT>
        ArrayType oldArrayType = new ArrayType(true, DataTypes.INT());

        // New type: ARRAY<BIGINT>
        ArrayType newArrayType = new ArrayType(true, DataTypes.BIGINT());

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldArrayType, newArrayType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
    }

    @Test
    public void testNestedMapTypeConversion() {
        // Old type: MAP<STRING, INT>
        MapType oldMapType = new MapType(true, DataTypes.STRING(), DataTypes.INT());

        // New type: MAP<STRING, BIGINT>
        MapType newMapType = new MapType(true, DataTypes.STRING(), DataTypes.BIGINT());

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldMapType, newMapType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
    }

    @Test
    public void testMapWithNullableComplexTypes() {
        // Old type: MAP<STRING, ROW<f1 ARRAY<INT> NULL, f2 ROW<x INT, y STRING> NULL>>
        RowType oldInnerRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "x", DataTypes.INT()),
                                new DataField(2, "y", DataTypes.STRING())));

        RowType oldValueType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "f1", new ArrayType(true, DataTypes.INT())),
                                new DataField(2, "f2", oldInnerRowType)));

        MapType oldType = new MapType(true, DataTypes.STRING(), oldValueType);

        // New type: MAP<STRING, ROW<f1 ARRAY<BIGINT> NULL, f2 ROW<x BIGINT, y STRING, z DOUBLE>
        // NULL>>
        RowType newInnerRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "x", DataTypes.BIGINT()),
                                new DataField(2, "y", DataTypes.STRING()),
                                new DataField(3, "z", DataTypes.DOUBLE())));

        RowType newValueType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "f1", new ArrayType(true, DataTypes.BIGINT())),
                                new DataField(2, "f2", newInnerRowType)));

        MapType newType = new MapType(true, DataTypes.STRING(), newValueType);

        // Test compatible evolution
        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);

        // Test incompatible element type
        RowType incompatibleValueType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(
                                        1,
                                        "f1",
                                        new ArrayType(
                                                true, DataTypes.STRING())), // INT to STRING is
                                // incompatible
                                new DataField(2, "f2", newInnerRowType)));

        MapType incompatibleType = new MapType(true, DataTypes.STRING(), incompatibleValueType);

        convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, incompatibleType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);
    }

    @Test
    public void testComplexNestedTypeConversion() {
        // Old type: ARRAY<MAP<INT, ROW(f1 INT, f2 STRING)>>
        RowType oldInnerRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "f1", DataTypes.INT()),
                                new DataField(2, "f2", DataTypes.STRING())));
        MapType oldMapType = new MapType(true, DataTypes.INT(), oldInnerRowType);
        ArrayType oldType = new ArrayType(true, oldMapType);

        // New type: ARRAY<MAP<INT, ROW(f1 BIGINT, f2 STRING, f3 DOUBLE)>>
        RowType newInnerRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "f1", DataTypes.BIGINT()),
                                new DataField(2, "f2", DataTypes.STRING()),
                                new DataField(3, "f3", DataTypes.DOUBLE())));
        MapType newMapType = new MapType(true, DataTypes.INT(), newInnerRowType);
        ArrayType newType = new ArrayType(true, newMapType);

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
    }

    @Test
    public void testRowTypeWithMultipleChanges() {
        // Old type: ROW(f1 INT, f2 STRING, f3 DOUBLE)
        RowType oldRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "f1", DataTypes.INT()),
                                new DataField(2, "f2", DataTypes.STRING()),
                                new DataField(3, "f3", DataTypes.DOUBLE())));

        // New type: ROW(f1 BIGINT, f2 VARCHAR(100), f4 INT)
        RowType newRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "f1", DataTypes.BIGINT()),
                                new DataField(2, "f2", new VarCharType(true, 100)),
                                new DataField(4, "f4", DataTypes.INT())));

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldRowType, newRowType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
    }

    @Test
    public void testNonNullableFieldRemoval() {
        // Old type: ROW(f1 INT NOT NULL, f2 STRING)
        RowType oldRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "f1", new IntType(false)),
                                new DataField(2, "f2", DataTypes.STRING())));

        // New type: ROW(f2 STRING)
        RowType newRowType =
                new RowType(true, Arrays.asList(new DataField(2, "f2", DataTypes.STRING())));

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldRowType, newRowType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);
    }

    @Test
    public void testAddingNonNullableField() {
        // Old type: ROW(f1 INT)
        RowType oldRowType =
                new RowType(true, Arrays.asList(new DataField(1, "f1", DataTypes.INT())));

        // New type: ROW(f1 INT, f2 STRING NOT NULL)
        RowType newRowType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "f1", DataTypes.INT()),
                                new DataField(2, "f2", new VarCharType(false, 100))));

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldRowType, newRowType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);
    }

    @Test
    public void testNestedRowWithinRowEvolution() {
        // Old type: ROW(f1 ROW(inner1 INT, inner2 STRING))
        RowType oldInnerType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "inner1", DataTypes.INT()),
                                new DataField(2, "inner2", DataTypes.STRING())));
        RowType oldRowType = new RowType(true, Arrays.asList(new DataField(1, "f1", oldInnerType)));

        // New type: ROW(f1 ROW(inner1 BIGINT, inner2 STRING, inner3 DOUBLE))
        RowType newInnerType =
                new RowType(
                        true,
                        Arrays.asList(
                                new DataField(1, "inner1", DataTypes.BIGINT()),
                                new DataField(2, "inner2", DataTypes.STRING()),
                                new DataField(3, "inner3", DataTypes.DOUBLE())));
        RowType newRowType = new RowType(true, Arrays.asList(new DataField(1, "f1", newInnerType)));

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldRowType, newRowType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
    }

    @Test
    public void testArrayOfArraysEvolution() {
        // Old type: ARRAY<ARRAY<INT>>
        ArrayType oldInnerArray = new ArrayType(true, DataTypes.INT());
        ArrayType oldType = new ArrayType(true, oldInnerArray);

        // New type: ARRAY<ARRAY<BIGINT>>
        ArrayType newInnerArray = new ArrayType(true, DataTypes.BIGINT());
        ArrayType newType = new ArrayType(true, newInnerArray);

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
    }

    @Test
    public void testArrayWithIncompatibleElementType() {
        // Old type: ARRAY<INT>
        ArrayType oldType = new ArrayType(true, DataTypes.INT());

        // New type: ARRAY<STRING>
        ArrayType newType = new ArrayType(true, DataTypes.STRING());

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);
    }

    @Test
    public void testMapOfMapsEvolution() {
        // Old type: MAP<STRING, MAP<INT, STRING>>
        MapType oldInnerMap = new MapType(true, DataTypes.INT(), DataTypes.STRING());
        MapType oldType = new MapType(true, DataTypes.STRING(), oldInnerMap);

        // New type: MAP<STRING, MAP<INT, VARCHAR(100)>>
        MapType newInnerMap = new MapType(true, DataTypes.INT(), new VarCharType(true, 100));
        MapType newType = new MapType(true, DataTypes.STRING(), newInnerMap);

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.IGNORE, convertAction);
    }

    @Test
    public void testMapWithIncompatibleValueType() {
        // Old type: MAP<STRING, INT>
        MapType oldType = new MapType(true, DataTypes.STRING(), DataTypes.INT());

        // New type: MAP<STRING, BOOLEAN>
        MapType newType = new MapType(true, DataTypes.STRING(), DataTypes.BOOLEAN());

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);
    }

    @Test
    public void testMultisetTypeEvolution() {
        // Old type: MULTISET<INT>
        MultisetType oldType = new MultisetType(true, DataTypes.INT());

        // New type: MULTISET<BIGINT>
        MultisetType newType = new MultisetType(true, DataTypes.BIGINT());

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT, convertAction);
    }

    @Test
    public void testIncompatibleMultisetTypeEvolution() {
        // Old type: MULTISET<INT>
        MultisetType oldType = new MultisetType(true, DataTypes.INT());

        // New type: MULTISET<STRING>
        MultisetType newType = new MultisetType(true, DataTypes.STRING());

        UpdatedDataFieldsProcessFunctionBase.ConvertAction convertAction =
                UpdatedDataFieldsProcessFunctionBase.canConvert(
                        oldType, newType, TypeMapping.defaultMapping());
        assertEquals(UpdatedDataFieldsProcessFunctionBase.ConvertAction.EXCEPTION, convertAction);
    }
}
