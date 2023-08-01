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

package org.apache.paimon.schema;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** UT for SchemaMergingUtils. */
public class SchemaMergingUtilsTest {

    @Test
    public void testMergeTableSchemas() {
        // Init the table schema
        DataField a = new DataField(0, "a", new IntType());
        DataField b = new DataField(1, "b", new DoubleType());
        DataField c =
                new DataField(
                        2,
                        "c",
                        new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BooleanType()));
        DataField d = new DataField(3, "d", new VarCharType(VarCharType.MAX_LENGTH));
        TableSchema current =
                new TableSchema(
                        0,
                        Lists.newArrayList(a, b, c, d),
                        3,
                        Lists.newArrayList("d"),
                        Lists.newArrayList("a", "d"),
                        new HashMap<>(),
                        "");

        // fake the RowType of data
        DataField f1 = new DataField(-1, "f1", new CharType(10));
        DataField f2 = new DataField(-1, "f2", new DateType());
        RowType fDataType = new RowType(Lists.newArrayList(f1, f2));
        DataField f = new DataField(-1, "f", fDataType);
        RowType t = new RowType(Lists.newArrayList(a, b, d, f));

        TableSchema merged = SchemaMergingUtils.mergeSchemas(current, t, false);
        Assertions.assertEquals(merged.id(), 1);
        Assertions.assertEquals(merged.highestFieldId(), 6);
        Assertions.assertArrayEquals(
                merged.primaryKeys().toArray(new String[0]), new String[] {"a", "d"});
        Assertions.assertArrayEquals(
                merged.partitionKeys().toArray(new String[0]), new String[] {"d"});
        List<DataField> fields = merged.fields();
        Assertions.assertEquals(fields.size(), 5);
        Assertions.assertTrue(fields.get(4).type() instanceof RowType);
    }

    @Test
    public void testMergeSchemas() {
        // This will test both `mergeSchemas` and `merge` methods.
        // Init the source schema
        DataField a = new DataField(0, "a", new IntType());
        DataField b = new DataField(1, "b", new DoubleType());
        DataField c =
                new DataField(
                        2,
                        "c",
                        new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BooleanType()));
        DataField d = new DataField(3, "d", new VarCharType(VarCharType.MAX_LENGTH));
        RowType source = new RowType(Lists.newArrayList(a, b, c, d));
        AtomicInteger highestFieldId = new AtomicInteger(3);

        // Case 1: an additional field.
        DataField e = new DataField(-1, "e", new DateType());
        RowType t1 = new RowType(Lists.newArrayList(a, b, c, d, e));
        RowType r1 = (RowType) SchemaMergingUtils.merge(source, t1, highestFieldId, false);
        Assertions.assertEquals(highestFieldId.get(), 4);
        Assertions.assertTrue(r1.isNullable());
        Assertions.assertEquals(r1.getFieldCount(), 5);
        Assertions.assertEquals(r1.getTypeAt(2), c.type());
        // the id of DataField has assigned to an incremental num.
        DataField expectedE = e.newId(4);
        Assertions.assertEquals(r1.getFields().get(highestFieldId.get()), expectedE);

        // Case 2: two missing fields.
        RowType t2 = new RowType(Lists.newArrayList(a, c, e));
        RowType r2 = SchemaMergingUtils.mergeSchemas(r1, t2, highestFieldId, false);
        Assertions.assertEquals(highestFieldId.get(), 4);
        Assertions.assertEquals(r2.getFieldCount(), 5);
        Assertions.assertEquals(r2.getTypeAt(3), d.type());
        Assertions.assertEquals(r2.getFields().get(highestFieldId.get()), expectedE);

        // Case 3: an additional nested field and a missing field.
        DataField f1 = new DataField(-1, "f1", new CharType(10));
        DataField f2 = new DataField(-1, "f2", new IntType());
        RowType fDataType = new RowType(Lists.newArrayList(f1, f2));
        DataField f = new DataField(-1, "f", fDataType);
        RowType t3 = new RowType(Lists.newArrayList(a, b, c, d, f));
        RowType r3 = (RowType) SchemaMergingUtils.merge(r2, t3, highestFieldId, false);
        Assertions.assertEquals(highestFieldId.get(), 7);
        Assertions.assertEquals(r3.getFieldCount(), 6);
        RowType expectedFDataType = new RowType(Lists.newArrayList(f1.newId(5), f2.newId(6)));
        Assertions.assertEquals(r3.getTypeAt(5), expectedFDataType);
        DataField expectedF = new DataField(7, "f", expectedFDataType);
        Assertions.assertEquals(r3.getFields().get(5), expectedF);

        // Case 4: an additional sub-field in a nested field.
        DataField f3 = new DataField(-1, "f3", new TimestampType());
        RowType newFDataType = new RowType(Lists.newArrayList(f1, f2, f3));
        DataField newF = new DataField(-1, "f", newFDataType);
        RowType t4 = new RowType(Lists.newArrayList(a, b, c, d, e, newF));
        RowType r4 = SchemaMergingUtils.mergeSchemas(r3, t4, highestFieldId, false);
        Assertions.assertEquals(highestFieldId.get(), 8);
        Assertions.assertEquals(r4.getFieldCount(), 6);
        RowType newExpectedFDataType =
                new RowType(Lists.newArrayList(f1.newId(5), f2.newId(6), f3.newId(8)));
        Assertions.assertEquals(r4.getTypeAt(5), newExpectedFDataType);

        // Case 5: a field that isn't compatible with the existing one.
        DataField newA = new DataField(-1, "a", new SmallIntType());
        RowType t5 = new RowType(Lists.newArrayList(newA, b, c, d, e, newF));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(r4, t5, highestFieldId, false));

        // Case 6: all new-coming fields
        DataField g = new DataField(-1, "g", new TimeType());
        DataField h = new DataField(-1, "h", new TimeType());
        RowType t6 = new RowType(Lists.newArrayList(g, h));
        RowType r6 = SchemaMergingUtils.mergeSchemas(r4, t6, highestFieldId, false);
        Assertions.assertEquals(highestFieldId.get(), 10);
        Assertions.assertEquals(r6.getFieldCount(), 8);
    }

    @Test
    public void testMergeArrayTypes() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        DataType source = new ArrayType(false, new IntType());

        // the element types are same.
        DataType t1 = new ArrayType(true, new IntType());
        ArrayType r1 = (ArrayType) SchemaMergingUtils.merge(source, t1, highestFieldId, false);
        Assertions.assertFalse(r1.isNullable());
        Assertions.assertTrue(r1.getElementType() instanceof IntType);

        // the element types aren't same, but can be evolved safety.
        DataType t2 = new ArrayType(true, new BigIntType());
        ArrayType r2 = (ArrayType) SchemaMergingUtils.merge(source, t2, highestFieldId, false);
        Assertions.assertTrue(r2.getElementType() instanceof BigIntType);

        // the element types aren't same, and can't be evolved safety.
        DataType t3 = new ArrayType(true, new SmallIntType());
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(source, t3, highestFieldId, false));
        // the value type of target's isn't same to the source's, but the source type can be cast to
        // the target type explicitly.
        ArrayType r3 = (ArrayType) SchemaMergingUtils.merge(source, t3, highestFieldId, true);
        Assertions.assertTrue(r3.getElementType() instanceof SmallIntType);
    }

    @Test
    public void testMergeMapTypes() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        DataType source = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType());

        // both the key and value types are same to the source's.
        DataType t1 = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType());
        MapType r1 = (MapType) SchemaMergingUtils.merge(source, t1, highestFieldId, false);
        Assertions.assertTrue(r1.isNullable());
        Assertions.assertTrue(r1.getKeyType() instanceof VarCharType);
        Assertions.assertTrue(r1.getValueType() instanceof IntType);

        // the value type of target's isn't same to the source's, but can be evolved safety.
        DataType t2 = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new DoubleType());
        MapType r2 = (MapType) SchemaMergingUtils.merge(source, t2, highestFieldId, false);
        Assertions.assertTrue(r2.getKeyType() instanceof VarCharType);
        Assertions.assertTrue(r2.getValueType() instanceof DoubleType);

        // the value type of target's isn't same to the source's, and can't be evolved safety.
        DataType t3 = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new SmallIntType());
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(source, t3, highestFieldId, false));
        // the value type of target's isn't same to the source's, but the source type can be cast to
        // the target type explicitly.
        MapType r3 = (MapType) SchemaMergingUtils.merge(source, t3, highestFieldId, true);
        Assertions.assertTrue(r3.getKeyType() instanceof VarCharType);
        Assertions.assertTrue(r3.getValueType() instanceof SmallIntType);
    }

    @Test
    public void testMergeMultisetTypes() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        DataType source = new MultisetType(false, new IntType());

        // the element types are same.
        DataType t1 = new MultisetType(true, new IntType());
        MultisetType r1 =
                (MultisetType) SchemaMergingUtils.merge(source, t1, highestFieldId, false);
        Assertions.assertFalse(r1.isNullable());
        Assertions.assertTrue(r1.getElementType() instanceof IntType);

        // the element types aren't same, but can be evolved safety.
        DataType t2 = new MultisetType(true, new BigIntType());
        MultisetType r2 =
                (MultisetType) SchemaMergingUtils.merge(source, t2, highestFieldId, false);
        Assertions.assertTrue(r2.getElementType() instanceof BigIntType);

        // the element types aren't same, and can't be evolved safety.
        DataType t3 = new MultisetType(true, new SmallIntType());
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(source, t3, highestFieldId, false));
        // the value type of target's isn't same to the source's, but the source type can be cast to
        // the target type explicitly.
        MultisetType r3 = (MultisetType) SchemaMergingUtils.merge(source, t3, highestFieldId, true);
        Assertions.assertTrue(r3.getElementType() instanceof SmallIntType);
    }

    @Test
    public void testMergeDecimalTypes() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        DataType s1 = new DecimalType();
        DataType t1 = new DecimalType(10, 0);
        DecimalType r1 = (DecimalType) SchemaMergingUtils.merge(s1, t1, highestFieldId, false);
        Assertions.assertTrue(r1.isNullable());
        Assertions.assertEquals(r1.getPrecision(), DecimalType.DEFAULT_PRECISION);
        Assertions.assertEquals(r1.getScale(), DecimalType.DEFAULT_SCALE);

        DataType s2 = new DecimalType(5, 2);
        DataType t2 = new DecimalType(7, 2);
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s2, t2, highestFieldId, false));

        // DecimalType -> Other Numeric Type
        DataType dcmSource = new DecimalType();
        DataType iTarget = new IntType();
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(dcmSource, iTarget, highestFieldId, false));
        // DecimalType -> Other Numeric Type with allowExplicitCast = true
        DataType res = SchemaMergingUtils.merge(dcmSource, iTarget, highestFieldId, true);
        Assertions.assertTrue(res instanceof IntType);
    }

    @Test
    public void testMergeTypesWithLength() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        // BinaryType
        DataType s1 = new BinaryType(10);
        DataType t1 = new BinaryType(10);
        BinaryType r1 = (BinaryType) SchemaMergingUtils.merge(s1, t1, highestFieldId, false);
        Assertions.assertEquals(r1.getLength(), 10);

        DataType s2 = new BinaryType(2);
        DataType t2 = new BinaryType();
        // smaller length
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s2, t2, highestFieldId, false));
        // smaller length  with allowExplicitCast = true
        BinaryType r2 = (BinaryType) SchemaMergingUtils.merge(s2, t2, highestFieldId, true);
        Assertions.assertEquals(r2.getLength(), BinaryType.DEFAULT_LENGTH);
        // bigger length
        DataType t3 = new BinaryType(5);
        BinaryType r3 = (BinaryType) SchemaMergingUtils.merge(s2, t3, highestFieldId, false);
        Assertions.assertEquals(r3.getLength(), 5);

        // VarCharType
        DataType s4 = new VarCharType();
        DataType t4 = new VarCharType(1);
        VarCharType r4 = (VarCharType) SchemaMergingUtils.merge(s4, t4, highestFieldId, false);
        Assertions.assertEquals(r4.getLength(), VarCharType.DEFAULT_LENGTH);

        DataType s5 = new VarCharType(2);
        DataType t5 = new VarCharType();
        // smaller length
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s5, t5, highestFieldId, false));
        // smaller length  with allowExplicitCast = true
        VarCharType r5 = (VarCharType) SchemaMergingUtils.merge(s5, t5, highestFieldId, true);
        Assertions.assertEquals(r5.getLength(), VarCharType.DEFAULT_LENGTH);
        // bigger length
        DataType t6 = new VarCharType(5);
        VarCharType r6 = (VarCharType) SchemaMergingUtils.merge(s5, t6, highestFieldId, false);
        Assertions.assertEquals(r6.getLength(), 5);

        // CharType
        DataType s7 = new CharType();
        DataType t7 = new CharType(1);
        CharType r7 = (CharType) SchemaMergingUtils.merge(s7, t7, highestFieldId, false);
        Assertions.assertEquals(r7.getLength(), CharType.DEFAULT_LENGTH);

        DataType s8 = new CharType(2);
        DataType t8 = new CharType();
        // smaller length
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s8, t8, highestFieldId, false));
        // smaller length  with allowExplicitCast = true
        CharType r8 = (CharType) SchemaMergingUtils.merge(s8, t8, highestFieldId, true);
        Assertions.assertEquals(r8.getLength(), CharType.DEFAULT_LENGTH);
        // bigger length
        DataType t9 = new CharType(5);
        CharType r9 = (CharType) SchemaMergingUtils.merge(s8, t9, highestFieldId, false);
        Assertions.assertEquals(r9.getLength(), 5);

        // VarBinaryType
        DataType s10 = new VarBinaryType();
        DataType t10 = new VarBinaryType(1);
        VarBinaryType r10 =
                (VarBinaryType) SchemaMergingUtils.merge(s10, t10, highestFieldId, false);
        Assertions.assertEquals(r10.getLength(), VarBinaryType.DEFAULT_LENGTH);

        DataType s11 = new VarBinaryType(2);
        DataType t11 = new VarBinaryType();
        // smaller length
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s11, t11, highestFieldId, false));
        // smaller length  with allowExplicitCast = true
        VarBinaryType r11 =
                (VarBinaryType) SchemaMergingUtils.merge(s11, t11, highestFieldId, true);
        Assertions.assertEquals(r11.getLength(), VarBinaryType.DEFAULT_LENGTH);
        // bigger length
        DataType t12 = new VarBinaryType(5);
        VarBinaryType r12 =
                (VarBinaryType) SchemaMergingUtils.merge(s11, t12, highestFieldId, false);
        Assertions.assertEquals(r12.getLength(), 5);

        // CharType -> VarCharType
        DataType s13 = new CharType();
        DataType t13 = new VarCharType(10);
        VarCharType r13 = (VarCharType) SchemaMergingUtils.merge(s13, t13, highestFieldId, false);
        Assertions.assertEquals(r13.getLength(), 10);

        // VarCharType ->CharType
        DataType s14 = new VarCharType(10);
        DataType t14 = new CharType();
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s14, t14, highestFieldId, false));
        CharType r14 = (CharType) SchemaMergingUtils.merge(s14, t14, highestFieldId, true);
        Assertions.assertEquals(r14.getLength(), CharType.DEFAULT_LENGTH);

        // BinaryType -> VarBinaryType
        DataType s15 = new BinaryType();
        DataType t15 = new VarBinaryType(10);
        VarBinaryType r15 =
                (VarBinaryType) SchemaMergingUtils.merge(s15, t15, highestFieldId, false);
        Assertions.assertEquals(r15.getLength(), 10);

        // VarBinaryType -> BinaryType
        DataType s16 = new VarBinaryType(10);
        DataType t16 = new BinaryType();
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s16, t16, highestFieldId, false));
        BinaryType r16 = (BinaryType) SchemaMergingUtils.merge(s16, t16, highestFieldId, true);
        Assertions.assertEquals(r16.getLength(), BinaryType.DEFAULT_LENGTH);

        // VarCharType -> VarBinaryType
        DataType s17 = new VarCharType(10);
        DataType t17 = new VarBinaryType();
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s17, t17, highestFieldId, false));
        VarBinaryType r17 =
                (VarBinaryType) SchemaMergingUtils.merge(s17, t17, highestFieldId, true);
        Assertions.assertEquals(r17.getLength(), VarBinaryType.DEFAULT_LENGTH);
    }

    @Test
    public void testMergeTypesWithPrecision() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        // LocalZonedTimestampType
        DataType s1 = new LocalZonedTimestampType();
        DataType t1 = new LocalZonedTimestampType();
        LocalZonedTimestampType r1 =
                (LocalZonedTimestampType) SchemaMergingUtils.merge(s1, t1, highestFieldId, false);
        Assertions.assertTrue(r1.isNullable());
        Assertions.assertEquals(r1.getPrecision(), LocalZonedTimestampType.DEFAULT_PRECISION);

        // lower precision
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () ->
                        SchemaMergingUtils.merge(
                                s1, new LocalZonedTimestampType(3), highestFieldId, false));
        // higher precision
        DataType t2 = new LocalZonedTimestampType(6);
        LocalZonedTimestampType r2 =
                (LocalZonedTimestampType) SchemaMergingUtils.merge(s1, t2, highestFieldId, false);
        Assertions.assertEquals(r2.getPrecision(), 6);

        // LocalZonedTimestampType -> TimeType
        DataType s3 = new LocalZonedTimestampType();
        DataType t3 = new TimeType(6);
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s3, t3, highestFieldId, false));

        // LocalZonedTimestampType -> TimestampType
        DataType s4 = new LocalZonedTimestampType();
        DataType t4 = new TimestampType();
        TimestampType r4 = (TimestampType) SchemaMergingUtils.merge(s4, t4, highestFieldId, false);
        Assertions.assertEquals(r4.getPrecision(), TimestampType.DEFAULT_PRECISION);

        // TimestampType.
        DataType s5 = new TimestampType();
        DataType t5 = new TimestampType();
        TimestampType r5 = (TimestampType) SchemaMergingUtils.merge(s5, t5, highestFieldId, false);
        Assertions.assertTrue(r5.isNullable());
        Assertions.assertEquals(r5.getPrecision(), TimestampType.DEFAULT_PRECISION);

        // lower precision
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s5, new TimestampType(3), highestFieldId, false));
        // higher precision
        DataType t6 = new TimestampType(9);
        TimestampType r6 = (TimestampType) SchemaMergingUtils.merge(s5, t6, highestFieldId, false);
        Assertions.assertEquals(r6.getPrecision(), 9);

        // TimestampType -> LocalZonedTimestampType
        DataType s7 = new TimestampType();
        DataType t7 = new LocalZonedTimestampType();
        LocalZonedTimestampType r7 =
                (LocalZonedTimestampType) SchemaMergingUtils.merge(s7, t7, highestFieldId, false);
        Assertions.assertEquals(r7.getPrecision(), TimestampType.DEFAULT_PRECISION);

        // TimestampType -> TimestampType
        DataType s8 = new TimestampType();
        DataType t8 = new TimeType(6);
        TimeType r8 = (TimeType) SchemaMergingUtils.merge(s8, t8, highestFieldId, false);
        Assertions.assertEquals(r8.getPrecision(), TimestampType.DEFAULT_PRECISION);

        // TimeType.
        DataType s9 = new TimeType();
        DataType t9 = new TimeType();
        TimeType r9 = (TimeType) SchemaMergingUtils.merge(s9, t9, highestFieldId, false);
        Assertions.assertTrue(r9.isNullable());
        Assertions.assertEquals(r9.getPrecision(), TimeType.DEFAULT_PRECISION);

        // lower precision
        DataType s10 = new TimeType(6);
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s10, new TimeType(3), highestFieldId, false));
        // higher precision
        DataType t10 = new TimeType(9);
        TimeType r10 = (TimeType) SchemaMergingUtils.merge(s9, t10, highestFieldId, false);
        Assertions.assertEquals(r10.getPrecision(), 9);

        // TimeType -> LocalZonedTimestampType
        DataType s11 = new TimeType();
        DataType t11 = new LocalZonedTimestampType();
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s11, t11, highestFieldId, false));
        // TimeType -> LocalZonedTimestampType with allowExplicitCast = true
        LocalZonedTimestampType r11 =
                (LocalZonedTimestampType) SchemaMergingUtils.merge(s11, t11, highestFieldId, true);
        Assertions.assertEquals(r11.getPrecision(), LocalZonedTimestampType.DEFAULT_PRECISION);

        // TimeType -> TimestampType
        DataType s12 = new TimeType();
        DataType t12 = new TimestampType();
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s12, t12, highestFieldId, false));
        // TimeType -> TimestampType with allowExplicitCast = true
        TimestampType r12 =
                (TimestampType) SchemaMergingUtils.merge(s12, t12, highestFieldId, true);
        Assertions.assertEquals(r12.getPrecision(), TimestampType.DEFAULT_PRECISION);
    }

    @Test
    public void testMergePrimitiveTypes() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        // declare the primitive source and target types
        DataType bSource = new BooleanType();
        DataType bTarget = new BooleanType();
        DataType tiSource = new TinyIntType();
        DataType tiTarget = new TinyIntType();
        DataType siSource = new SmallIntType();
        DataType siTarget = new SmallIntType();
        DataType iSource = new IntType();
        DataType iTarget = new IntType();
        DataType biSource = new BigIntType();
        DataType biTarget = new BigIntType();
        DataType fSource = new FloatType();
        DataType fTarget = new FloatType();
        DataType dSource = new DoubleType();
        DataType dTarget = new DoubleType();
        DataType dcmTarget = new DecimalType();

        // BooleanType
        DataType btRes1 = SchemaMergingUtils.merge(bSource, bTarget, highestFieldId, false);
        Assertions.assertTrue(btRes1 instanceof BooleanType);
        // BooleanType -> Numeric Type
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(bSource, tiTarget, highestFieldId, false));
        // BooleanType -> Numeric Type with allowExplicitCast = true
        DataType btRes2 = SchemaMergingUtils.merge(bSource, tiTarget, highestFieldId, true);
        Assertions.assertTrue(btRes2 instanceof TinyIntType);

        // TinyIntType
        DataType tiRes1 = SchemaMergingUtils.merge(tiSource, tiTarget, highestFieldId, false);
        Assertions.assertTrue(tiRes1 instanceof TinyIntType);
        // TinyIntType -> SmallIntType
        DataType tiRes2 = SchemaMergingUtils.merge(tiSource, siTarget, highestFieldId, false);
        Assertions.assertTrue(tiRes2 instanceof SmallIntType);
        // TinyIntType -> IntType
        DataType tiRes3 = SchemaMergingUtils.merge(tiSource, iTarget, highestFieldId, false);
        Assertions.assertTrue(tiRes3 instanceof IntType);
        // TinyIntType -> BigIntType
        DataType tiRes4 = SchemaMergingUtils.merge(tiSource, biTarget, highestFieldId, false);
        Assertions.assertTrue(tiRes4 instanceof BigIntType);
        // TinyIntType -> FloatType
        DataType tiRes5 = SchemaMergingUtils.merge(tiSource, fTarget, highestFieldId, false);
        Assertions.assertTrue(tiRes5 instanceof FloatType);
        // TinyIntType -> DoubleType
        DataType tiRes6 = SchemaMergingUtils.merge(tiSource, dTarget, highestFieldId, false);
        Assertions.assertTrue(tiRes6 instanceof DoubleType);
        // TinyIntType -> DecimalType
        DataType tiRes7 = SchemaMergingUtils.merge(tiSource, dcmTarget, highestFieldId, false);
        Assertions.assertTrue(tiRes7 instanceof DecimalType);
        // TinyIntType -> BooleanType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(tiSource, bTarget, highestFieldId, false));
        // TinyIntType -> BooleanType with allowExplicitCast = true
        DataType tiRes8 = SchemaMergingUtils.merge(tiSource, bTarget, highestFieldId, true);
        Assertions.assertTrue(tiRes8 instanceof BooleanType);

        // SmallIntType
        DataType siRes1 = SchemaMergingUtils.merge(siSource, siTarget, highestFieldId, false);
        Assertions.assertTrue(siRes1 instanceof SmallIntType);
        // SmallIntType -> TinyIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(siSource, tiTarget, highestFieldId, false));
        // SmallIntType -> TinyIntType with allowExplicitCast = true
        DataType siRes2 = SchemaMergingUtils.merge(siSource, tiTarget, highestFieldId, true);
        Assertions.assertTrue(siRes2 instanceof TinyIntType);
        // SmallIntType -> IntType
        DataType siRes3 = SchemaMergingUtils.merge(siSource, iTarget, highestFieldId, false);
        Assertions.assertTrue(siRes3 instanceof IntType);
        // SmallIntType -> BigIntType
        DataType siRes4 = SchemaMergingUtils.merge(siSource, biTarget, highestFieldId, false);
        Assertions.assertTrue(siRes4 instanceof BigIntType);
        // SmallIntType -> FloatType
        DataType siRes5 = SchemaMergingUtils.merge(siSource, fTarget, highestFieldId, false);
        Assertions.assertTrue(siRes5 instanceof FloatType);
        // SmallIntType -> DoubleType
        DataType siRes6 = SchemaMergingUtils.merge(siSource, dTarget, highestFieldId, false);
        Assertions.assertTrue(siRes6 instanceof DoubleType);
        // SmallIntType -> DecimalType
        DataType siRes7 = SchemaMergingUtils.merge(siSource, dcmTarget, highestFieldId, false);
        Assertions.assertTrue(siRes7 instanceof DecimalType);

        // IntType
        DataType iRes1 = SchemaMergingUtils.merge(iSource, iTarget, highestFieldId, false);
        Assertions.assertTrue(iRes1 instanceof IntType);
        // IntType -> TinyIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(iSource, tiTarget, highestFieldId, false));
        // IntType -> TinyIntType with allowExplicitCast = true
        DataType iRes2 = SchemaMergingUtils.merge(iSource, tiTarget, highestFieldId, true);
        Assertions.assertTrue(iRes2 instanceof TinyIntType);
        // IntType -> SmallIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(iSource, siTarget, highestFieldId, false));
        // IntType -> SmallIntType with allowExplicitCast = true
        DataType iRes3 = SchemaMergingUtils.merge(iSource, siTarget, highestFieldId, true);
        Assertions.assertTrue(iRes3 instanceof SmallIntType);
        // IntType -> BigIntType
        DataType iRes4 = SchemaMergingUtils.merge(iSource, biTarget, highestFieldId, false);
        Assertions.assertTrue(iRes4 instanceof BigIntType);
        // IntType -> FloatType
        DataType iRes5 = SchemaMergingUtils.merge(iSource, fTarget, highestFieldId, false);
        Assertions.assertTrue(iRes5 instanceof FloatType);
        // IntType -> DoubleType
        DataType iRes6 = SchemaMergingUtils.merge(iSource, dTarget, highestFieldId, false);
        Assertions.assertTrue(iRes6 instanceof DoubleType);
        // IntType -> DecimalType
        DataType iRes7 = SchemaMergingUtils.merge(iSource, dcmTarget, highestFieldId, false);
        Assertions.assertTrue(iRes7 instanceof DecimalType);

        // BigIntType
        DataType biRes1 = SchemaMergingUtils.merge(biSource, biTarget, highestFieldId, false);
        Assertions.assertTrue(biRes1 instanceof BigIntType);
        // BigIntType -> TinyIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(biSource, tiTarget, highestFieldId, false));
        // BigIntType -> TinyIntType with allowExplicitCast = true
        DataType biRes2 = SchemaMergingUtils.merge(biSource, tiTarget, highestFieldId, true);
        Assertions.assertTrue(biRes2 instanceof TinyIntType);
        // BigIntType -> SmallIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(biSource, siTarget, highestFieldId, false));
        // BigIntType -> SmallIntType with allowExplicitCast = true
        DataType biRes3 = SchemaMergingUtils.merge(biSource, siTarget, highestFieldId, true);
        Assertions.assertTrue(biRes3 instanceof SmallIntType);
        // BigIntType -> IntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(biSource, iTarget, highestFieldId, false));
        // BigIntType -> IntType with allowExplicitCast = true
        DataType biRes4 = SchemaMergingUtils.merge(biSource, iTarget, highestFieldId, true);
        Assertions.assertTrue(biRes4 instanceof IntType);
        // BigIntType -> FloatType
        DataType biRes5 = SchemaMergingUtils.merge(biSource, fTarget, highestFieldId, false);
        Assertions.assertTrue(biRes5 instanceof FloatType);
        // BigIntType -> DoubleType
        DataType biRes6 = SchemaMergingUtils.merge(biSource, dTarget, highestFieldId, false);
        Assertions.assertTrue(biRes6 instanceof DoubleType);
        // BigIntType -> DecimalType
        DataType biRes7 = SchemaMergingUtils.merge(biSource, dcmTarget, highestFieldId, false);
        Assertions.assertTrue(biRes7 instanceof DecimalType);

        // FloatType
        DataType fRes1 = SchemaMergingUtils.merge(fSource, fTarget, highestFieldId, false);
        Assertions.assertTrue(fRes1 instanceof FloatType);
        // FloatType -> TinyIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(fSource, tiTarget, highestFieldId, false));
        // FloatType -> TinyIntType with allowExplicitCast = true
        DataType fRes2 = SchemaMergingUtils.merge(fSource, tiTarget, highestFieldId, true);
        Assertions.assertTrue(fRes2 instanceof TinyIntType);
        // FloatType -> SmallIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(fSource, siTarget, highestFieldId, false));
        // FloatType -> IntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(fSource, iTarget, highestFieldId, false));
        // FloatType -> IntType with allowExplicitCast = true
        DataType fRes4 = SchemaMergingUtils.merge(fSource, iTarget, highestFieldId, true);
        Assertions.assertTrue(fRes4 instanceof IntType);
        // FloatType -> BigIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(fSource, biTarget, highestFieldId, false));
        // FloatType -> DoubleType
        DataType fRes6 = SchemaMergingUtils.merge(fSource, dTarget, highestFieldId, false);
        Assertions.assertTrue(fRes6 instanceof DoubleType);
        // FloatType -> DecimalType
        DataType fRes7 = SchemaMergingUtils.merge(fSource, dcmTarget, highestFieldId, false);
        Assertions.assertTrue(fRes7 instanceof DecimalType);

        // DoubleType
        DataType dRes1 = SchemaMergingUtils.merge(dSource, dTarget, highestFieldId, false);
        Assertions.assertTrue(dRes1 instanceof DoubleType);
        // DoubleType -> TinyIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(dSource, tiTarget, highestFieldId, false));
        // DoubleType -> SmallIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(dSource, siTarget, highestFieldId, false));
        // DoubleType -> SmallIntType with allowExplicitCast = true
        DataType dRes3 = SchemaMergingUtils.merge(dSource, siTarget, highestFieldId, true);
        Assertions.assertTrue(dRes3 instanceof SmallIntType);
        // DoubleType -> IntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(dSource, iTarget, highestFieldId, false));
        // DoubleType -> BigIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(dSource, biTarget, highestFieldId, false));
        // DoubleType -> BigIntType with allowExplicitCast = true
        DataType dRes5 = SchemaMergingUtils.merge(dSource, biTarget, highestFieldId, true);
        Assertions.assertTrue(dRes5 instanceof BigIntType);
        // DoubleType -> FloatType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(dSource, fTarget, highestFieldId, false));
        // DoubleType -> DecimalType
        DataType dRes7 = SchemaMergingUtils.merge(dSource, dcmTarget, highestFieldId, false);
        Assertions.assertTrue(dRes7 instanceof DecimalType);
    }
}
