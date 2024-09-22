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
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(merged.id()).isEqualTo(1);
        assertThat(merged.highestFieldId()).isEqualTo(6);
        assertThat(merged.primaryKeys()).containsExactlyInAnyOrder("a", "d");
        assertThat(merged.partitionKeys()).containsExactly("d");
        List<DataField> fields = merged.fields();
        assertThat(fields.size()).isEqualTo(5);
        assertThat(fields.get(4).type() instanceof RowType).isTrue();
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
        assertThat(highestFieldId.get()).isEqualTo(4);
        assertThat(r1.isNullable()).isTrue();
        assertThat(r1.getFieldCount()).isEqualTo(5);
        assertThat(r1.getTypeAt(2)).isEqualTo(c.type());
        // the id of DataField has assigned to an incremental num.
        DataField expectedE = e.newId(4);
        assertThat(r1.getFields().get(highestFieldId.get())).isEqualTo(expectedE);

        // Case 2: two missing fields.
        RowType t2 = new RowType(Lists.newArrayList(a, c, e));
        RowType r2 = SchemaMergingUtils.mergeSchemas(r1, t2, highestFieldId, false);
        assertThat(highestFieldId.get()).isEqualTo(4);
        assertThat(r2.getFieldCount()).isEqualTo(5);
        assertThat(r2.getTypeAt(3)).isEqualTo(d.type());
        assertThat(r2.getFields().get(highestFieldId.get())).isEqualTo(expectedE);

        // Case 3: an additional nested field and a missing field.
        DataField f1 = new DataField(-1, "f1", new CharType(10));
        DataField f2 = new DataField(-1, "f2", new IntType());
        RowType fDataType = new RowType(Lists.newArrayList(f1, f2));
        DataField f = new DataField(-1, "f", fDataType);
        RowType t3 = new RowType(Lists.newArrayList(a, b, c, d, f));
        RowType r3 = (RowType) SchemaMergingUtils.merge(r2, t3, highestFieldId, false);
        assertThat(highestFieldId.get()).isEqualTo(7);
        assertThat(r3.getFieldCount()).isEqualTo(6);
        RowType expectedFDataType = new RowType(Lists.newArrayList(f1.newId(5), f2.newId(6)));
        assertThat(r3.getTypeAt(5)).isEqualTo(expectedFDataType);
        DataField expectedF = new DataField(7, "f", expectedFDataType);
        assertThat(r3.getFields().get(5)).isEqualTo(expectedF);

        // Case 4: an additional sub-field in a nested field.
        DataField f3 = new DataField(-1, "f3", new TimestampType());
        RowType newFDataType = new RowType(Lists.newArrayList(f1, f2, f3));
        DataField newF = new DataField(-1, "f", newFDataType);
        RowType t4 = new RowType(Lists.newArrayList(a, b, c, d, e, newF));
        RowType r4 = SchemaMergingUtils.mergeSchemas(r3, t4, highestFieldId, false);
        assertThat(highestFieldId.get()).isEqualTo(8);
        assertThat(r4.getFieldCount()).isEqualTo(6);
        RowType newExpectedFDataType =
                new RowType(Lists.newArrayList(f1.newId(5), f2.newId(6), f3.newId(8)));
        assertThat(r4.getTypeAt(5)).isEqualTo(newExpectedFDataType);

        // Case 5: a field that isn't compatible with the existing one.
        DataField newA = new DataField(-1, "a", new SmallIntType());
        RowType t5 = new RowType(Lists.newArrayList(newA, b, c, d, e, newF));
        assertThatThrownBy(() -> SchemaMergingUtils.merge(r4, t5, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);

        // Case 6: all new-coming fields
        DataField g = new DataField(-1, "g", new TimeType());
        DataField h = new DataField(-1, "h", new TimeType());
        RowType t6 = new RowType(Lists.newArrayList(g, h));
        RowType r6 = SchemaMergingUtils.mergeSchemas(r4, t6, highestFieldId, false);
        assertThat(highestFieldId.get()).isEqualTo(10);
        assertThat(r6.getFieldCount()).isEqualTo(8);
    }

    @Test
    public void testMergeArrayTypes() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        DataType source = new ArrayType(false, new IntType());

        // the element types are same.
        DataType t1 = new ArrayType(true, new IntType());
        ArrayType r1 = (ArrayType) SchemaMergingUtils.merge(source, t1, highestFieldId, false);
        assertThat(r1.isNullable()).isFalse();
        assertThat(r1.getElementType() instanceof IntType).isTrue();

        // the element types aren't same, but can be evolved safety.
        DataType t2 = new ArrayType(true, new BigIntType());
        ArrayType r2 = (ArrayType) SchemaMergingUtils.merge(source, t2, highestFieldId, false);
        assertThat(r2.isNullable()).isFalse();
        assertThat(r2.getElementType() instanceof BigIntType).isTrue();

        // the element types aren't same, and can't be evolved safety.
        DataType t3 = new ArrayType(true, new SmallIntType());
        assertThatThrownBy(() -> SchemaMergingUtils.merge(source, t3, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // the value type of target's isn't same to the source's, but the source type can be cast to
        // the target type explicitly.
        ArrayType r3 = (ArrayType) SchemaMergingUtils.merge(source, t3, highestFieldId, true);
        assertThat(r3.isNullable()).isFalse();
        assertThat(r3.getElementType() instanceof SmallIntType).isTrue();
    }

    @Test
    public void testMergeMapTypes() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        DataType source = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType());

        // both the key and value types are same to the source's.
        DataType t1 = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType());
        MapType r1 = (MapType) SchemaMergingUtils.merge(source, t1, highestFieldId, false);
        assertThat(r1.isNullable()).isTrue();
        assertThat(r1.getKeyType() instanceof VarCharType).isTrue();
        assertThat(r1.getValueType() instanceof IntType).isTrue();

        // the value type of target's isn't same to the source's, but can be evolved safety.
        DataType t2 = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new DoubleType());
        MapType r2 = (MapType) SchemaMergingUtils.merge(source, t2, highestFieldId, false);
        assertThat(r2.isNullable()).isTrue();
        assertThat(r2.getKeyType() instanceof VarCharType).isTrue();
        assertThat(r2.getValueType() instanceof DoubleType).isTrue();

        // the value type of target's isn't same to the source's, and can't be evolved safety.
        DataType t3 = new MapType(new VarCharType(VarCharType.MAX_LENGTH), new SmallIntType());
        assertThatThrownBy(() -> SchemaMergingUtils.merge(source, t3, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // the value type of target's isn't same to the source's, but the source type can be cast to
        // the target type explicitly.
        MapType r3 = (MapType) SchemaMergingUtils.merge(source, t3, highestFieldId, true);
        assertThat(r3.isNullable()).isTrue();
        assertThat(r3.getKeyType() instanceof VarCharType).isTrue();
        assertThat(r3.getValueType() instanceof SmallIntType).isTrue();
    }

    @Test
    public void testMergeMultisetTypes() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        DataType source = new MultisetType(false, new IntType());

        // the element types are same.
        DataType t1 = new MultisetType(true, new IntType());
        MultisetType r1 =
                (MultisetType) SchemaMergingUtils.merge(source, t1, highestFieldId, false);
        assertThat(r1.isNullable()).isFalse();
        assertThat(r1.getElementType() instanceof IntType).isTrue();

        // the element types aren't same, but can be evolved safety.
        DataType t2 = new MultisetType(true, new BigIntType());
        MultisetType r2 =
                (MultisetType) SchemaMergingUtils.merge(source, t2, highestFieldId, false);
        assertThat(r2.isNullable()).isFalse();
        assertThat(r2.getElementType() instanceof BigIntType).isTrue();

        // the element types aren't same, and can't be evolved safety.
        DataType t3 = new MultisetType(true, new SmallIntType());
        assertThatThrownBy(() -> SchemaMergingUtils.merge(source, t3, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // the value type of target's isn't same to the source's, but the source type can be cast to
        // the target type explicitly.
        MultisetType r3 = (MultisetType) SchemaMergingUtils.merge(source, t3, highestFieldId, true);
        assertThat(r3.isNullable()).isFalse();
        assertThat(r3.getElementType() instanceof SmallIntType).isTrue();
    }

    @Test
    public void testMergeDecimalTypes() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        DataType s1 = new DecimalType();
        DataType t1 = new DecimalType(10, 0);
        DecimalType r1 = (DecimalType) SchemaMergingUtils.merge(s1, t1, highestFieldId, false);
        assertThat(r1.isNullable()).isTrue();
        assertThat(r1.getPrecision()).isEqualTo(DecimalType.DEFAULT_PRECISION);
        assertThat(r1.getScale()).isEqualTo(DecimalType.DEFAULT_SCALE);

        DataType s2 = new DecimalType(5, 2);
        DataType t2 = new DecimalType(7, 3);
        assertThatThrownBy(() -> SchemaMergingUtils.merge(s2, t2, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);

        DataType s3 = new DecimalType(false, 5, 2);
        DataType t3 = new DecimalType(7, 2);
        DecimalType r3 = (DecimalType) SchemaMergingUtils.merge(s3, t3, highestFieldId, false);
        assertThat(r3.isNullable()).isFalse();
        assertThat(r3.getPrecision()).isEqualTo(7);
        assertThat(r3.getScale()).isEqualTo(2);

        DataType s4 = new DecimalType(7, 2);
        DataType t4 = new DecimalType(5, 2);
        DecimalType r4 = (DecimalType) SchemaMergingUtils.merge(s4, t4, highestFieldId, false);
        assertThat(r4.isNullable()).isTrue();
        assertThat(r4.getPrecision()).isEqualTo(7);
        assertThat(r4.getScale()).isEqualTo(2);

        // DecimalType -> Other Numeric Type
        DataType dcmSource = new DecimalType();
        DataType iTarget = new IntType();
        assertThatThrownBy(
                        () -> SchemaMergingUtils.merge(dcmSource, iTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // DecimalType -> Other Numeric Type with allowExplicitCast = true
        DataType res = SchemaMergingUtils.merge(dcmSource, iTarget, highestFieldId, true);
        assertThat(res instanceof IntType).isTrue();
    }

    @Test
    public void testMergeTypesWithLength() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        // BinaryType
        DataType s1 = new BinaryType(10);
        DataType t1 = new BinaryType(10);
        BinaryType r1 = (BinaryType) SchemaMergingUtils.merge(s1, t1, highestFieldId, false);
        assertThat(r1.getLength()).isEqualTo(10);

        DataType s2 = new BinaryType(2);
        DataType t2 = new BinaryType();
        // smaller length
        assertThatThrownBy(() -> SchemaMergingUtils.merge(s2, t2, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // smaller length  with allowExplicitCast = true
        BinaryType r2 = (BinaryType) SchemaMergingUtils.merge(s2, t2, highestFieldId, true);
        assertThat(r2.getLength()).isEqualTo(BinaryType.DEFAULT_LENGTH);
        // bigger length
        DataType t3 = new BinaryType(5);
        BinaryType r3 = (BinaryType) SchemaMergingUtils.merge(s2, t3, highestFieldId, false);
        assertThat(r3.getLength()).isEqualTo(5);

        // VarCharType
        DataType s4 = new VarCharType();
        DataType t4 = new VarCharType(1);
        VarCharType r4 = (VarCharType) SchemaMergingUtils.merge(s4, t4, highestFieldId, false);
        assertThat(r4.getLength()).isEqualTo(VarCharType.DEFAULT_LENGTH);

        DataType s5 = new VarCharType(2);
        DataType t5 = new VarCharType();
        // smaller length
        assertThatThrownBy(() -> SchemaMergingUtils.merge(s5, t5, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // smaller length  with allowExplicitCast = true
        VarCharType r5 = (VarCharType) SchemaMergingUtils.merge(s5, t5, highestFieldId, true);
        assertThat(r5.getLength()).isEqualTo(VarCharType.DEFAULT_LENGTH);
        // bigger length
        DataType t6 = new VarCharType(5);
        VarCharType r6 = (VarCharType) SchemaMergingUtils.merge(s5, t6, highestFieldId, false);
        assertThat(r6.getLength()).isEqualTo(5);

        // CharType
        DataType s7 = new CharType();
        DataType t7 = new CharType(1);
        CharType r7 = (CharType) SchemaMergingUtils.merge(s7, t7, highestFieldId, false);
        assertThat(r7.getLength()).isEqualTo(CharType.DEFAULT_LENGTH);

        DataType s8 = new CharType(2);
        DataType t8 = new CharType();
        // smaller length
        assertThatThrownBy(() -> SchemaMergingUtils.merge(s8, t8, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // smaller length  with allowExplicitCast = true
        CharType r8 = (CharType) SchemaMergingUtils.merge(s8, t8, highestFieldId, true);
        assertThat(r8.getLength()).isEqualTo(CharType.DEFAULT_LENGTH);
        // bigger length
        DataType t9 = new CharType(5);
        CharType r9 = (CharType) SchemaMergingUtils.merge(s8, t9, highestFieldId, false);
        assertThat(r9.getLength()).isEqualTo(5);

        // VarBinaryType
        DataType s10 = new VarBinaryType();
        DataType t10 = new VarBinaryType(1);
        VarBinaryType r10 =
                (VarBinaryType) SchemaMergingUtils.merge(s10, t10, highestFieldId, false);
        assertThat(r10.getLength()).isEqualTo(VarBinaryType.DEFAULT_LENGTH);

        DataType s11 = new VarBinaryType(2);
        DataType t11 = new VarBinaryType();
        // smaller length
        assertThatThrownBy(() -> SchemaMergingUtils.merge(s11, t11, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // smaller length  with allowExplicitCast = true
        VarBinaryType r11 =
                (VarBinaryType) SchemaMergingUtils.merge(s11, t11, highestFieldId, true);
        assertThat(r11.getLength()).isEqualTo(VarBinaryType.DEFAULT_LENGTH);
        // bigger length
        DataType t12 = new VarBinaryType(5);
        VarBinaryType r12 =
                (VarBinaryType) SchemaMergingUtils.merge(s11, t12, highestFieldId, false);
        assertThat(r12.getLength()).isEqualTo(5);

        // CharType -> VarCharType
        DataType s13 = new CharType();
        DataType t13 = new VarCharType(10);
        VarCharType r13 = (VarCharType) SchemaMergingUtils.merge(s13, t13, highestFieldId, false);
        assertThat(r13.getLength()).isEqualTo(10);

        // VarCharType ->CharType
        DataType s14 = new VarCharType(10);
        DataType t14 = new CharType();
        assertThatThrownBy(() -> SchemaMergingUtils.merge(s14, t14, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        CharType r14 = (CharType) SchemaMergingUtils.merge(s14, t14, highestFieldId, true);
        assertThat(r14.getLength()).isEqualTo(CharType.DEFAULT_LENGTH);

        // BinaryType -> VarBinaryType
        DataType s15 = new BinaryType();
        DataType t15 = new VarBinaryType(10);
        VarBinaryType r15 =
                (VarBinaryType) SchemaMergingUtils.merge(s15, t15, highestFieldId, false);
        assertThat(r15.getLength()).isEqualTo(10);

        // VarBinaryType -> BinaryType
        DataType s16 = new VarBinaryType(10);
        DataType t16 = new BinaryType();
        assertThatThrownBy(() -> SchemaMergingUtils.merge(s16, t16, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        BinaryType r16 = (BinaryType) SchemaMergingUtils.merge(s16, t16, highestFieldId, true);
        assertThat(r16.getLength()).isEqualTo(BinaryType.DEFAULT_LENGTH);

        // VarCharType -> VarBinaryType
        DataType s17 = new VarCharType(10);
        DataType t17 = new VarBinaryType();
        assertThatThrownBy(() -> SchemaMergingUtils.merge(s17, t17, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        VarBinaryType r17 =
                (VarBinaryType) SchemaMergingUtils.merge(s17, t17, highestFieldId, true);
        assertThat(r17.getLength()).isEqualTo(VarBinaryType.DEFAULT_LENGTH);
    }

    @Test
    public void testMergeTypesWithPrecision() {
        AtomicInteger highestFieldId = new AtomicInteger(1);

        // LocalZonedTimestampType
        DataType s1 = new LocalZonedTimestampType();
        DataType t1 = new LocalZonedTimestampType();
        LocalZonedTimestampType r1 =
                (LocalZonedTimestampType) SchemaMergingUtils.merge(s1, t1, highestFieldId, false);
        assertThat(r1.isNullable()).isTrue();
        assertThat(r1.getPrecision()).isEqualTo(LocalZonedTimestampType.DEFAULT_PRECISION);

        // lower precision
        assertThatThrownBy(
                        () ->
                                SchemaMergingUtils.merge(
                                        s1, new LocalZonedTimestampType(3), highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);

        // higher precision
        DataType t2 = new LocalZonedTimestampType(6);
        LocalZonedTimestampType r2 =
                (LocalZonedTimestampType) SchemaMergingUtils.merge(s1, t2, highestFieldId, false);
        assertThat(r2.getPrecision()).isEqualTo(6);

        // LocalZonedTimestampType -> TimeType
        DataType s3 = new LocalZonedTimestampType();
        DataType t3 = new TimeType(6);
        assertThatThrownBy(() -> SchemaMergingUtils.merge(s3, t3, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);

        // LocalZonedTimestampType -> TimestampType
        DataType s4 = new LocalZonedTimestampType();
        DataType t4 = new TimestampType();
        TimestampType r4 = (TimestampType) SchemaMergingUtils.merge(s4, t4, highestFieldId, false);
        assertThat(r4.getPrecision()).isEqualTo(TimestampType.DEFAULT_PRECISION);

        // TimestampType.
        DataType s5 = new TimestampType();
        DataType t5 = new TimestampType();
        TimestampType r5 = (TimestampType) SchemaMergingUtils.merge(s5, t5, highestFieldId, false);
        assertThat(r5.isNullable()).isTrue();
        assertThat(r5.getPrecision()).isEqualTo(TimestampType.DEFAULT_PRECISION);

        // lower precision
        assertThatThrownBy(
                        () ->
                                SchemaMergingUtils.merge(
                                        s5, new TimestampType(3), highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);

        // higher precision
        DataType t6 = new TimestampType(9);
        TimestampType r6 = (TimestampType) SchemaMergingUtils.merge(s5, t6, highestFieldId, false);
        assertThat(r6.getPrecision()).isEqualTo(9);

        // TimestampType -> LocalZonedTimestampType
        DataType s7 = new TimestampType();
        DataType t7 = new LocalZonedTimestampType();
        LocalZonedTimestampType r7 =
                (LocalZonedTimestampType) SchemaMergingUtils.merge(s7, t7, highestFieldId, false);
        assertThat(r7.getPrecision()).isEqualTo(TimestampType.DEFAULT_PRECISION);

        // TimestampType -> TimestampType
        DataType s8 = new TimestampType();
        DataType t8 = new TimeType(6);
        TimeType r8 = (TimeType) SchemaMergingUtils.merge(s8, t8, highestFieldId, false);
        assertThat(r8.getPrecision()).isEqualTo(TimestampType.DEFAULT_PRECISION);

        // TimeType.
        DataType s9 = new TimeType();
        DataType t9 = new TimeType();
        TimeType r9 = (TimeType) SchemaMergingUtils.merge(s9, t9, highestFieldId, false);
        assertThat(r9.isNullable()).isTrue();
        assertThat(r9.getPrecision()).isEqualTo(TimeType.DEFAULT_PRECISION);

        // lower precision
        DataType s10 = new TimeType(6);
        assertThatThrownBy(
                        () -> SchemaMergingUtils.merge(s10, new TimeType(3), highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);

        // higher precision
        DataType t10 = new TimeType(9);
        TimeType r10 = (TimeType) SchemaMergingUtils.merge(s9, t10, highestFieldId, false);
        assertThat(r10.getPrecision()).isEqualTo(9);

        // TimeType -> LocalZonedTimestampType
        DataType s11 = new TimeType();
        DataType t11 = new LocalZonedTimestampType();
        assertThatThrownBy(() -> SchemaMergingUtils.merge(s11, t11, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);

        // TimeType -> LocalZonedTimestampType with allowExplicitCast = true
        LocalZonedTimestampType r11 =
                (LocalZonedTimestampType) SchemaMergingUtils.merge(s11, t11, highestFieldId, true);
        assertThat(r11.getPrecision()).isEqualTo(LocalZonedTimestampType.DEFAULT_PRECISION);

        // TimeType -> TimestampType
        DataType s12 = new TimeType();
        DataType t12 = new TimestampType();
        assertThatThrownBy(() -> SchemaMergingUtils.merge(s12, t12, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);

        // TimeType -> TimestampType with allowExplicitCast = true
        TimestampType r12 =
                (TimestampType) SchemaMergingUtils.merge(s12, t12, highestFieldId, true);
        assertThat(r12.getPrecision()).isEqualTo(TimestampType.DEFAULT_PRECISION);
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
        assertThat(btRes1 instanceof BooleanType).isTrue();
        // BooleanType -> Numeric Type
        assertThatThrownBy(() -> SchemaMergingUtils.merge(bSource, tiTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);

        // BooleanType -> Numeric Type with allowExplicitCast = true
        DataType btRes2 = SchemaMergingUtils.merge(bSource, tiTarget, highestFieldId, true);
        assertThat(btRes2 instanceof TinyIntType).isTrue();

        // TinyIntType
        DataType tiRes1 = SchemaMergingUtils.merge(tiSource, tiTarget, highestFieldId, false);
        assertThat(tiRes1 instanceof TinyIntType).isTrue();
        // TinyIntType -> SmallIntType
        DataType tiRes2 = SchemaMergingUtils.merge(tiSource, siTarget, highestFieldId, false);
        assertThat(tiRes2 instanceof SmallIntType).isTrue();
        // TinyIntType -> IntType
        DataType tiRes3 = SchemaMergingUtils.merge(tiSource, iTarget, highestFieldId, false);
        assertThat(tiRes3 instanceof IntType).isTrue();
        // TinyIntType -> BigIntType
        DataType tiRes4 = SchemaMergingUtils.merge(tiSource, biTarget, highestFieldId, false);
        assertThat(tiRes4 instanceof BigIntType).isTrue();
        // TinyIntType -> FloatType
        DataType tiRes5 = SchemaMergingUtils.merge(tiSource, fTarget, highestFieldId, false);
        assertThat(tiRes5 instanceof FloatType).isTrue();
        // TinyIntType -> DoubleType
        DataType tiRes6 = SchemaMergingUtils.merge(tiSource, dTarget, highestFieldId, false);
        assertThat(tiRes6 instanceof DoubleType).isTrue();
        // TinyIntType -> DecimalType
        DataType tiRes7 = SchemaMergingUtils.merge(tiSource, dcmTarget, highestFieldId, false);
        assertThat(tiRes7 instanceof DecimalType).isTrue();
        // TinyIntType -> BooleanType
        assertThatThrownBy(() -> SchemaMergingUtils.merge(tiSource, bTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);

        // TinyIntType -> BooleanType with allowExplicitCast = true
        DataType tiRes8 = SchemaMergingUtils.merge(tiSource, bTarget, highestFieldId, true);
        assertThat(tiRes8 instanceof BooleanType).isTrue();

        // SmallIntType
        DataType siRes1 = SchemaMergingUtils.merge(siSource, siTarget, highestFieldId, false);
        assertThat(siRes1 instanceof SmallIntType).isTrue();
        // SmallIntType -> TinyIntType
        assertThatThrownBy(
                        () -> SchemaMergingUtils.merge(siSource, tiTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // SmallIntType -> TinyIntType with allowExplicitCast = true
        DataType siRes2 = SchemaMergingUtils.merge(siSource, tiTarget, highestFieldId, true);
        assertThat(siRes2 instanceof TinyIntType).isTrue();
        // SmallIntType -> IntType
        DataType siRes3 = SchemaMergingUtils.merge(siSource, iTarget, highestFieldId, false);
        assertThat(siRes3 instanceof IntType).isTrue();
        // SmallIntType -> BigIntType
        DataType siRes4 = SchemaMergingUtils.merge(siSource, biTarget, highestFieldId, false);
        assertThat(siRes4 instanceof BigIntType).isTrue();
        // SmallIntType -> FloatType
        DataType siRes5 = SchemaMergingUtils.merge(siSource, fTarget, highestFieldId, false);
        assertThat(siRes5 instanceof FloatType).isTrue();
        // SmallIntType -> DoubleType
        DataType siRes6 = SchemaMergingUtils.merge(siSource, dTarget, highestFieldId, false);
        assertThat(siRes6 instanceof DoubleType).isTrue();
        // SmallIntType -> DecimalType
        DataType siRes7 = SchemaMergingUtils.merge(siSource, dcmTarget, highestFieldId, false);
        assertThat(siRes7 instanceof DecimalType).isTrue();

        // IntType
        DataType iRes1 = SchemaMergingUtils.merge(iSource, iTarget, highestFieldId, false);
        assertThat(iRes1 instanceof IntType).isTrue();
        // IntType -> TinyIntType
        assertThatThrownBy(() -> SchemaMergingUtils.merge(iSource, tiTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // IntType -> TinyIntType with allowExplicitCast = true
        DataType iRes2 = SchemaMergingUtils.merge(iSource, tiTarget, highestFieldId, true);
        assertThat(iRes2 instanceof TinyIntType).isTrue();
        // IntType -> SmallIntType
        assertThatThrownBy(() -> SchemaMergingUtils.merge(iSource, siTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // IntType -> SmallIntType with allowExplicitCast = true
        DataType iRes3 = SchemaMergingUtils.merge(iSource, siTarget, highestFieldId, true);
        assertThat(iRes3 instanceof SmallIntType).isTrue();
        // IntType -> BigIntType
        DataType iRes4 = SchemaMergingUtils.merge(iSource, biTarget, highestFieldId, false);
        assertThat(iRes4 instanceof BigIntType).isTrue();
        // IntType -> FloatType
        DataType iRes5 = SchemaMergingUtils.merge(iSource, fTarget, highestFieldId, false);
        assertThat(iRes5 instanceof FloatType).isTrue();
        // IntType -> DoubleType
        DataType iRes6 = SchemaMergingUtils.merge(iSource, dTarget, highestFieldId, false);
        assertThat(iRes6 instanceof DoubleType).isTrue();
        // IntType -> DecimalType
        DataType iRes7 = SchemaMergingUtils.merge(iSource, dcmTarget, highestFieldId, false);
        assertThat(iRes7 instanceof DecimalType).isTrue();

        // BigIntType
        DataType biRes1 = SchemaMergingUtils.merge(biSource, biTarget, highestFieldId, false);
        assertThat(biRes1 instanceof BigIntType).isTrue();
        // BigIntType -> TinyIntType
        assertThatThrownBy(
                        () -> SchemaMergingUtils.merge(biSource, tiTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // BigIntType -> TinyIntType with allowExplicitCast = true
        DataType biRes2 = SchemaMergingUtils.merge(biSource, tiTarget, highestFieldId, true);
        assertThat(biRes2 instanceof TinyIntType).isTrue();
        // BigIntType -> SmallIntType
        assertThatThrownBy(
                        () -> SchemaMergingUtils.merge(biSource, siTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // BigIntType -> SmallIntType with allowExplicitCast = true
        DataType biRes3 = SchemaMergingUtils.merge(biSource, siTarget, highestFieldId, true);
        assertThat(biRes3 instanceof SmallIntType).isTrue();
        // BigIntType -> IntType
        assertThatThrownBy(() -> SchemaMergingUtils.merge(biSource, iTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // BigIntType -> IntType with allowExplicitCast = true
        DataType biRes4 = SchemaMergingUtils.merge(biSource, iTarget, highestFieldId, true);
        assertThat(biRes4 instanceof IntType).isTrue();
        // BigIntType -> FloatType
        DataType biRes5 = SchemaMergingUtils.merge(biSource, fTarget, highestFieldId, false);
        assertThat(biRes5 instanceof FloatType).isTrue();
        // BigIntType -> DoubleType
        DataType biRes6 = SchemaMergingUtils.merge(biSource, dTarget, highestFieldId, false);
        assertThat(biRes6 instanceof DoubleType).isTrue();
        // BigIntType -> DecimalType
        DataType biRes7 = SchemaMergingUtils.merge(biSource, dcmTarget, highestFieldId, false);
        assertThat(biRes7 instanceof DecimalType).isTrue();

        // FloatType
        DataType fRes1 = SchemaMergingUtils.merge(fSource, fTarget, highestFieldId, false);
        assertThat(fRes1 instanceof FloatType).isTrue();
        // FloatType -> TinyIntType
        assertThatThrownBy(() -> SchemaMergingUtils.merge(fSource, tiTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // FloatType -> TinyIntType with allowExplicitCast = true
        DataType fRes2 = SchemaMergingUtils.merge(fSource, tiTarget, highestFieldId, true);
        assertThat(fRes2 instanceof TinyIntType).isTrue();
        // FloatType -> SmallIntType
        assertThatThrownBy(() -> SchemaMergingUtils.merge(fSource, siTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // FloatType -> IntType
        assertThatThrownBy(() -> SchemaMergingUtils.merge(fSource, iTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // FloatType -> IntType with allowExplicitCast = true
        DataType fRes4 = SchemaMergingUtils.merge(fSource, iTarget, highestFieldId, true);
        assertThat(fRes4 instanceof IntType).isTrue();
        // FloatType -> BigIntType
        assertThatThrownBy(() -> SchemaMergingUtils.merge(fSource, biTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // FloatType -> DoubleType
        DataType fRes6 = SchemaMergingUtils.merge(fSource, dTarget, highestFieldId, false);
        assertThat(fRes6 instanceof DoubleType).isTrue();
        // FloatType -> DecimalType
        DataType fRes7 = SchemaMergingUtils.merge(fSource, dcmTarget, highestFieldId, false);
        assertThat(fRes7 instanceof DecimalType).isTrue();

        // DoubleType
        DataType dRes1 = SchemaMergingUtils.merge(dSource, dTarget, highestFieldId, false);
        assertThat(dRes1 instanceof DoubleType).isTrue();
        // DoubleType -> TinyIntType
        assertThatThrownBy(() -> SchemaMergingUtils.merge(dSource, tiTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // DoubleType -> SmallIntType
        assertThatThrownBy(() -> SchemaMergingUtils.merge(dSource, siTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // DoubleType -> SmallIntType with allowExplicitCast = true
        DataType dRes3 = SchemaMergingUtils.merge(dSource, siTarget, highestFieldId, true);
        assertThat(dRes3 instanceof SmallIntType).isTrue();
        // DoubleType -> IntType
        assertThatThrownBy(() -> SchemaMergingUtils.merge(dSource, iTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // DoubleType -> BigIntType
        assertThatThrownBy(() -> SchemaMergingUtils.merge(dSource, biTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // DoubleType -> BigIntType with allowExplicitCast = true
        DataType dRes5 = SchemaMergingUtils.merge(dSource, biTarget, highestFieldId, true);
        assertThat(dRes5 instanceof BigIntType).isTrue();
        // DoubleType -> FloatType
        assertThatThrownBy(() -> SchemaMergingUtils.merge(dSource, fTarget, highestFieldId, false))
                .isInstanceOf(UnsupportedOperationException.class);
        // DoubleType -> DecimalType
        DataType dRes7 = SchemaMergingUtils.merge(dSource, dcmTarget, highestFieldId, false);
        assertThat(dRes7 instanceof DecimalType).isTrue();
    }
}
