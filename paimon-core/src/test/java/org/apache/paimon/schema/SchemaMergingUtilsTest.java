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

        TableSchema merged = SchemaMergingUtils.mergeSchemas(current, t);
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
        RowType r1 = (RowType) SchemaMergingUtils.merge(source, t1, highestFieldId);
        Assertions.assertEquals(highestFieldId.get(), 4);
        Assertions.assertTrue(r1.isNullable());
        Assertions.assertEquals(r1.getFieldCount(), 5);
        Assertions.assertEquals(r1.getTypeAt(2), c.type());
        // the id of DataField has assigned to an incremental num.
        DataField expectedE = e.newId(4);
        Assertions.assertEquals(r1.getFields().get(highestFieldId.get()), expectedE);

        // Case 2: two missing fields.
        RowType t2 = new RowType(Lists.newArrayList(a, c, e));
        RowType r2 = SchemaMergingUtils.mergeSchemas(r1, t2, highestFieldId);
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
        RowType r3 = (RowType) SchemaMergingUtils.merge(r2, t3, highestFieldId);
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
        RowType r4 = SchemaMergingUtils.mergeSchemas(r3, t4, highestFieldId);
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
                () -> SchemaMergingUtils.merge(r4, t5, highestFieldId));

        // Case 6: all new-coming fields
        DataField g = new DataField(-1, "g", new TimeType());
        DataField h = new DataField(-1, "h", new TimeType());
        RowType t6 = new RowType(Lists.newArrayList(g, h));
        RowType r6 = SchemaMergingUtils.mergeSchemas(r4, t6, highestFieldId);
        Assertions.assertEquals(highestFieldId.get(), 10);
        Assertions.assertEquals(r6.getFieldCount(), 8);
    }

    @Test
    public void testMergeArrayTypes() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        DataType source = new ArrayType(false, new IntType());

        // the element types are same.
        DataType t1 = new ArrayType(true, new IntType());
        ArrayType r1 = (ArrayType) SchemaMergingUtils.merge(source, t1, highestFieldId);
        Assertions.assertFalse(r1.isNullable());
        Assertions.assertTrue(r1.getElementType() instanceof IntType);

        // the element types aren't same, but can be evolved safety.
        DataType t2 = new ArrayType(true, new BigIntType());
        ArrayType r2 = (ArrayType) SchemaMergingUtils.merge(source, t2, highestFieldId);
        Assertions.assertTrue(r2.getElementType() instanceof BigIntType);

        // the element types aren't same, and can't be evolved safety.
        DataType t3 = new ArrayType(true, new SmallIntType());
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(source, t3, highestFieldId));
    }

    @Test
    public void testMergeMapTypes() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        DataType source = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType());

        // both the key and value types are same to the source's.
        DataType t1 = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType());
        MapType r1 = (MapType) SchemaMergingUtils.merge(source, t1, highestFieldId);
        Assertions.assertTrue(r1.isNullable());
        Assertions.assertTrue(r1.getKeyType() instanceof VarCharType);
        Assertions.assertTrue(r1.getValueType() instanceof IntType);

        // the value type of target's isn't same to the source's, but can be evolved safety.
        DataType t2 = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new DoubleType());
        MapType r2 = (MapType) SchemaMergingUtils.merge(source, t2, highestFieldId);
        Assertions.assertTrue(r2.getKeyType() instanceof VarCharType);
        Assertions.assertTrue(r2.getValueType() instanceof DoubleType);

        // the value type of target's isn't same to the source's, and can't be evolved safety.
        DataType t3 = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new DateType());
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(source, t3, highestFieldId));
    }

    @Test
    public void testMergeMultisetTypes() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        DataType source = new MultisetType(false, new IntType());

        // the element types are same.
        DataType t1 = new MultisetType(true, new IntType());
        MultisetType r1 = (MultisetType) SchemaMergingUtils.merge(source, t1, highestFieldId);
        Assertions.assertFalse(r1.isNullable());
        Assertions.assertTrue(r1.getElementType() instanceof IntType);

        // the element types aren't same, but can be evolved safety.
        DataType t2 = new MultisetType(true, new BigIntType());
        MultisetType r2 = (MultisetType) SchemaMergingUtils.merge(source, t2, highestFieldId);
        Assertions.assertTrue(r2.getElementType() instanceof BigIntType);

        // the element types aren't same, and can't be evolved safety.
        DataType t3 = new MultisetType(true, new SmallIntType());
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(source, t3, highestFieldId));
    }

    @Test
    public void testMergeDecimalTypes() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        DataType s1 = new DecimalType();
        DataType t1 = new DecimalType(10, 0);
        DecimalType r1 = (DecimalType) SchemaMergingUtils.merge(s1, t1, highestFieldId);
        Assertions.assertTrue(r1.isNullable());
        Assertions.assertEquals(r1.getPrecision(), DecimalType.DEFAULT_PRECISION);
        Assertions.assertEquals(r1.getScale(), DecimalType.DEFAULT_SCALE);

        DataType s2 = new DecimalType(5, 2);
        DataType t2 = new DecimalType(7, 2);
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s2, t2, highestFieldId));

        // DecimalType -> Other Numeric Type
        DataType dcmSource = new DecimalType();
        DataType iTarget = new IntType();
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(dcmSource, iTarget, highestFieldId));
    }

    @Test
    public void testMergeTypesWithLength() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        // BinaryType
        DataType s1 = new BinaryType(10);
        DataType t1 = new BinaryType(10);
        BinaryType r1 = (BinaryType) SchemaMergingUtils.merge(s1, t1, highestFieldId);
        Assertions.assertEquals(r1.getLength(), 10);

        DataType s2 = new BinaryType(2);
        // smaller length
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s2, new BinaryType(), highestFieldId));
        // bigger length
        DataType t2 = new BinaryType(5);
        BinaryType r2 = (BinaryType) SchemaMergingUtils.merge(s2, t2, highestFieldId);
        Assertions.assertEquals(r2.getLength(), 5);

        // VarCharType
        DataType s3 = new VarCharType();
        DataType t3 = new VarCharType(1);
        VarCharType r3 = (VarCharType) SchemaMergingUtils.merge(s3, t3, highestFieldId);
        Assertions.assertEquals(r3.getLength(), VarCharType.DEFAULT_LENGTH);

        DataType s4 = new VarCharType(2);
        // smaller length
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s4, new VarCharType(), highestFieldId));
        // bigger length
        DataType t4 = new VarCharType(5);
        VarCharType r4 = (VarCharType) SchemaMergingUtils.merge(s4, t4, highestFieldId);
        Assertions.assertEquals(r4.getLength(), 5);

        // CharType
        DataType s5 = new CharType();
        DataType t5 = new CharType(1);
        CharType r5 = (CharType) SchemaMergingUtils.merge(s5, t5, highestFieldId);
        Assertions.assertEquals(r5.getLength(), CharType.DEFAULT_LENGTH);

        DataType s6 = new CharType(2);
        // smaller length
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s6, new CharType(), highestFieldId));
        // bigger length
        DataType t6 = new CharType(5);
        CharType r6 = (CharType) SchemaMergingUtils.merge(s6, t6, highestFieldId);
        Assertions.assertEquals(r6.getLength(), 5);

        // VarBinaryType
        DataType s7 = new VarBinaryType();
        DataType t7 = new VarBinaryType(1);
        VarBinaryType r7 = (VarBinaryType) SchemaMergingUtils.merge(s7, t7, highestFieldId);
        Assertions.assertEquals(r7.getLength(), VarBinaryType.DEFAULT_LENGTH);

        DataType s8 = new VarBinaryType(2);
        // smaller length
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s8, new VarBinaryType(), highestFieldId));
        // bigger length
        DataType t8 = new VarBinaryType(5);
        VarBinaryType r8 = (VarBinaryType) SchemaMergingUtils.merge(s8, t8, highestFieldId);
        Assertions.assertEquals(r8.getLength(), 5);

        // CharType -> VarCharType
        DataType s9 = new CharType();
        DataType t9 = new VarCharType(10);
        VarCharType r9 = (VarCharType) SchemaMergingUtils.merge(s9, t9, highestFieldId);
        Assertions.assertEquals(r9.getLength(), 10);

        // BinaryType -> VarBinaryType
        DataType s10 = new BinaryType();
        DataType t10 = new VarBinaryType(10);
        VarBinaryType r10 = (VarBinaryType) SchemaMergingUtils.merge(s10, t10, highestFieldId);
        Assertions.assertEquals(r10.getLength(), 10);
    }

    @Test
    public void testMergeTypesWithPrecision() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        // LocalZonedTimestampType
        DataType s1 = new LocalZonedTimestampType();
        DataType t1 = new LocalZonedTimestampType();
        LocalZonedTimestampType r1 =
                (LocalZonedTimestampType) SchemaMergingUtils.merge(s1, t1, highestFieldId);
        Assertions.assertTrue(r1.isNullable());
        Assertions.assertEquals(r1.getPrecision(), LocalZonedTimestampType.DEFAULT_PRECISION);

        // lower precision
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s1, new LocalZonedTimestampType(3), highestFieldId));
        // higher precision
        DataType t2 = new LocalZonedTimestampType(6);
        LocalZonedTimestampType r2 =
                (LocalZonedTimestampType) SchemaMergingUtils.merge(s1, t2, highestFieldId);
        Assertions.assertEquals(r2.getPrecision(), 6);

        // LocalZonedTimestampType -> TimeType
        DataType s3 = new LocalZonedTimestampType();
        DataType t3 = new TimeType(6);
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s3, t3, highestFieldId));

        // LocalZonedTimestampType -> TimestampType
        DataType s4 = new LocalZonedTimestampType();
        DataType t4 = new TimestampType();
        TimestampType r4 = (TimestampType) SchemaMergingUtils.merge(s4, t4, highestFieldId);
        Assertions.assertEquals(r4.getPrecision(), TimestampType.DEFAULT_PRECISION);

        // TimestampType.
        DataType s5 = new TimestampType();
        DataType t5 = new TimestampType();
        TimestampType r5 = (TimestampType) SchemaMergingUtils.merge(s5, t5, highestFieldId);
        Assertions.assertTrue(r5.isNullable());
        Assertions.assertEquals(r5.getPrecision(), TimestampType.DEFAULT_PRECISION);

        // lower precision
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s5, new TimestampType(3), highestFieldId));
        // higher precision
        DataType t6 = new TimestampType(9);
        TimestampType r6 = (TimestampType) SchemaMergingUtils.merge(s5, t6, highestFieldId);
        Assertions.assertEquals(r6.getPrecision(), 9);

        // TimestampType -> LocalZonedTimestampType
        DataType s7 = new TimestampType();
        DataType t7 = new LocalZonedTimestampType();
        LocalZonedTimestampType r7 =
                (LocalZonedTimestampType) SchemaMergingUtils.merge(s7, t7, highestFieldId);
        Assertions.assertEquals(r7.getPrecision(), TimestampType.DEFAULT_PRECISION);

        // TimestampType -> TimestampType
        DataType s8 = new TimestampType();
        DataType t8 = new TimeType(6);
        TimeType r8 = (TimeType) SchemaMergingUtils.merge(s8, t8, highestFieldId);
        Assertions.assertEquals(r8.getPrecision(), TimestampType.DEFAULT_PRECISION);

        // TimeType.
        DataType s9 = new TimeType();
        DataType t9 = new TimeType();
        TimeType r9 = (TimeType) SchemaMergingUtils.merge(s9, t9, highestFieldId);
        Assertions.assertTrue(r9.isNullable());
        Assertions.assertEquals(r9.getPrecision(), TimeType.DEFAULT_PRECISION);

        // lower precision
        DataType s10 = new TimeType(6);
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s10, new TimeType(3), highestFieldId));
        // higher precision
        DataType t10 = new TimeType(9);
        TimeType r10 = (TimeType) SchemaMergingUtils.merge(s9, t10, highestFieldId);
        Assertions.assertEquals(r10.getPrecision(), 9);

        // TimeType -> LocalZonedTimestampType
        DataType s11 = new TimeType();
        DataType t11 = new LocalZonedTimestampType();
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s11, t11, highestFieldId));

        // TimeType -> TimestampType
        DataType s12 = new TimeType();
        DataType t12 = new TimestampType();
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(s12, t12, highestFieldId));
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
        DataType btRes = SchemaMergingUtils.merge(bSource, bTarget, highestFieldId);
        Assertions.assertTrue(btRes instanceof BooleanType);
        // BooleanType -> Numeric Type
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(bSource, tiTarget, highestFieldId));

        // TinyIntType
        DataType tiRes1 = SchemaMergingUtils.merge(tiSource, tiTarget, highestFieldId);
        Assertions.assertTrue(tiRes1 instanceof TinyIntType);
        // TinyIntType -> SmallIntType
        DataType tiRes2 = SchemaMergingUtils.merge(tiSource, siTarget, highestFieldId);
        Assertions.assertTrue(tiRes2 instanceof SmallIntType);
        // TinyIntType -> IntType
        DataType tiRes3 = SchemaMergingUtils.merge(tiSource, iTarget, highestFieldId);
        Assertions.assertTrue(tiRes3 instanceof IntType);
        // TinyIntType -> BigIntType
        DataType tiRes4 = SchemaMergingUtils.merge(tiSource, biTarget, highestFieldId);
        Assertions.assertTrue(tiRes4 instanceof BigIntType);
        // TinyIntType -> FloatType
        DataType tiRes5 = SchemaMergingUtils.merge(tiSource, fTarget, highestFieldId);
        Assertions.assertTrue(tiRes5 instanceof FloatType);
        // TinyIntType -> DoubleType
        DataType tiRes6 = SchemaMergingUtils.merge(tiSource, dTarget, highestFieldId);
        Assertions.assertTrue(tiRes6 instanceof DoubleType);
        // TinyIntType -> DecimalType
        DataType tiRes7 = SchemaMergingUtils.merge(tiSource, dcmTarget, highestFieldId);
        Assertions.assertTrue(tiRes7 instanceof DecimalType);
        // TinyIntType -> BooleanType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(tiSource, bTarget, highestFieldId));

        // SmallIntType
        DataType siRes1 = SchemaMergingUtils.merge(siSource, siTarget, highestFieldId);
        Assertions.assertTrue(siRes1 instanceof SmallIntType);
        // SmallIntType -> TinyIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(siSource, tiTarget, highestFieldId));
        // SmallIntType -> IntType
        DataType siRes2 = SchemaMergingUtils.merge(siSource, iTarget, highestFieldId);
        Assertions.assertTrue(siRes2 instanceof IntType);
        // SmallIntType -> BigIntType
        DataType siRes3 = SchemaMergingUtils.merge(siSource, biTarget, highestFieldId);
        Assertions.assertTrue(siRes3 instanceof BigIntType);
        // SmallIntType -> FloatType
        DataType siRes4 = SchemaMergingUtils.merge(siSource, fTarget, highestFieldId);
        Assertions.assertTrue(siRes4 instanceof FloatType);
        // SmallIntType -> DoubleType
        DataType siRes5 = SchemaMergingUtils.merge(siSource, dTarget, highestFieldId);
        Assertions.assertTrue(siRes5 instanceof DoubleType);
        // SmallIntType -> DecimalType
        DataType siRes6 = SchemaMergingUtils.merge(siSource, dcmTarget, highestFieldId);
        Assertions.assertTrue(siRes6 instanceof DecimalType);

        // IntType
        DataType iRes1 = SchemaMergingUtils.merge(iSource, iTarget, highestFieldId);
        Assertions.assertTrue(iRes1 instanceof IntType);
        // IntType -> TinyIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(iSource, tiTarget, highestFieldId));
        // IntType -> SmallIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(iSource, siTarget, highestFieldId));
        // IntType -> BigIntType
        DataType iRes2 = SchemaMergingUtils.merge(iSource, biTarget, highestFieldId);
        Assertions.assertTrue(iRes2 instanceof BigIntType);
        // IntType -> FloatType
        DataType iRes3 = SchemaMergingUtils.merge(iSource, fTarget, highestFieldId);
        Assertions.assertTrue(iRes3 instanceof FloatType);
        // IntType -> DoubleType
        DataType iRes4 = SchemaMergingUtils.merge(iSource, dTarget, highestFieldId);
        Assertions.assertTrue(iRes4 instanceof DoubleType);
        // IntType -> DecimalType
        DataType iRes5 = SchemaMergingUtils.merge(iSource, dcmTarget, highestFieldId);
        Assertions.assertTrue(iRes5 instanceof DecimalType);

        // BigIntType
        DataType biRes1 = SchemaMergingUtils.merge(biSource, biTarget, highestFieldId);
        Assertions.assertTrue(biRes1 instanceof BigIntType);
        // BigIntType -> TinyIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(biSource, tiTarget, highestFieldId));
        // BigIntType -> SmallIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(biSource, siTarget, highestFieldId));
        // BigIntType -> IntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(biSource, iTarget, highestFieldId));
        // BigIntType -> FloatType
        DataType biRes2 = SchemaMergingUtils.merge(biSource, fTarget, highestFieldId);
        Assertions.assertTrue(biRes2 instanceof FloatType);
        // BigIntType -> DoubleType
        DataType biRes3 = SchemaMergingUtils.merge(biSource, dTarget, highestFieldId);
        Assertions.assertTrue(biRes3 instanceof DoubleType);
        // BigIntType -> DecimalType
        DataType biRes4 = SchemaMergingUtils.merge(biSource, dcmTarget, highestFieldId);
        Assertions.assertTrue(biRes4 instanceof DecimalType);

        // FloatType
        DataType fRes1 = SchemaMergingUtils.merge(fSource, fTarget, highestFieldId);
        Assertions.assertTrue(fRes1 instanceof FloatType);
        // FloatType -> TinyIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(fSource, tiTarget, highestFieldId));
        // FloatType -> SmallIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(fSource, siTarget, highestFieldId));
        // FloatType -> IntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(fSource, iTarget, highestFieldId));
        // FloatType -> BigIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(fSource, biTarget, highestFieldId));
        // FloatType -> DoubleType
        DataType fRes2 = SchemaMergingUtils.merge(fSource, dTarget, highestFieldId);
        Assertions.assertTrue(fRes2 instanceof DoubleType);
        // FloatType -> DecimalType
        DataType fRes3 = SchemaMergingUtils.merge(fSource, dcmTarget, highestFieldId);
        Assertions.assertTrue(fRes3 instanceof DecimalType);

        // DoubleType
        DataType dRes = SchemaMergingUtils.merge(dSource, dTarget, highestFieldId);
        Assertions.assertTrue(dRes instanceof DoubleType);
        // DoubleType -> TinyIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(dSource, tiTarget, highestFieldId));
        // DoubleType -> SmallIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(dSource, siTarget, highestFieldId));
        // DoubleType -> IntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(dSource, iTarget, highestFieldId));
        // DoubleType -> BigIntType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(dSource, biTarget, highestFieldId));
        // DoubleType -> FloatType
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> SchemaMergingUtils.merge(dSource, fTarget, highestFieldId));
        // DoubleType -> DecimalType
        DataType dRes2 = SchemaMergingUtils.merge(dSource, dcmTarget, highestFieldId);
        Assertions.assertTrue(dRes2 instanceof DecimalType);
    }
}
