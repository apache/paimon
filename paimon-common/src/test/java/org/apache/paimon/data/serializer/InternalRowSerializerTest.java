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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryArrayWriter;
import org.apache.paimon.data.BinaryMap;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Objects;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link InternalRowSerializer}. */
abstract class InternalRowSerializerTest extends SerializerTestInstance<InternalRow> {

    private final InternalRowSerializer serializer;
    private final InternalRow[] testData;

    InternalRowSerializerTest(InternalRowSerializer serializer, InternalRow[] testData) {
        super(serializer, testData);
        this.serializer = serializer;
        this.testData = testData;
    }

    @Override
    protected boolean deepEquals(InternalRow o1, InternalRow o2) {
        return deepEqualsInternalRow(o1, o2, serializer.duplicate(), serializer.duplicate());
    }

    // ----------------------------------------------------------------------------------------------

    public static BinaryArray createArray(int... ints) {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, ints.length, 4);
        for (int i = 0; i < ints.length; i++) {
            writer.writeInt(i, ints[i]);
        }
        writer.complete();
        return array;
    }

    public static BinaryMap createMap(int[] keys, int[] values) {
        return BinaryMap.valueOf(createArray(keys), createArray(values));
    }

    public static GenericRow createRow(Object f0, Object f1, Object f2, Object f3, Object f4) {
        GenericRow row = new GenericRow(5);
        row.setField(0, f0);
        row.setField(1, f1);
        row.setField(2, f2);
        row.setField(3, f3);
        row.setField(4, f4);
        return row;
    }

    public static boolean deepEqualsInternalRow(
            InternalRow should,
            InternalRow is,
            InternalRowSerializer serializer1,
            InternalRowSerializer serializer2) {
        return deepEqualsInternalRow(should, is, serializer1, serializer2, false);
    }

    private static boolean deepEqualsInternalRow(
            InternalRow should,
            InternalRow is,
            InternalRowSerializer serializer1,
            InternalRowSerializer serializer2,
            boolean checkClass) {
        if (should.getFieldCount() != is.getFieldCount()) {
            return false;
        }
        if (checkClass && (should.getClass() != is.getClass() || !should.equals(is))) {
            return false;
        }

        BinaryRow row1 = serializer1.toBinaryRow(should);
        BinaryRow row2 = serializer2.toBinaryRow(is);

        return Objects.equals(row1, row2);
    }

    private void checkDeepEquals(InternalRow should, InternalRow is, boolean checkClass) {
        boolean equals =
                deepEqualsInternalRow(
                        should, is, serializer.duplicate(), serializer.duplicate(), checkClass);
        assertThat(equals).isTrue();
    }

    @Test
    protected void testCopy() {
        for (InternalRow row : testData) {
            checkDeepEquals(row, serializer.copy(row), true);
        }

        for (InternalRow row : testData) {
            checkDeepEquals(row, serializer.copy(row), true);
        }

        for (InternalRow row : testData) {
            checkDeepEquals(row, serializer.copy(serializer.toBinaryRow(row)), false);
        }

        for (InternalRow row : testData) {
            checkDeepEquals(row, serializer.copy(serializer.toBinaryRow(row)), false);
        }

        for (InternalRow row : testData) {
            checkDeepEquals(row, serializer.copy(serializer.toBinaryRow(row)), false);
        }
    }

    @Test
    void testWrongCopy() {
        assertThatThrownBy(() -> serializer.copy(new GenericRow(serializer.getArity() + 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    static final class SimpleInternalRowSerializerTest extends InternalRowSerializerTest {
        public SimpleInternalRowSerializerTest() {
            super(getRowSerializer(), getData());
        }

        private static InternalRow[] getData() {
            GenericRow row1 = new GenericRow(2);
            row1.setField(0, 1);
            row1.setField(1, fromString("a"));

            GenericRow row2 = new GenericRow(2);
            row2.setField(0, 2);
            row2.setField(1, null);

            return new InternalRow[] {row1, row2};
        }

        private static InternalRowSerializer getRowSerializer() {
            return new InternalRowSerializer(DataTypes.INT(), DataTypes.STRING());
        }
    }

    static final class LargeInternalRowSerializerTest extends InternalRowSerializerTest {
        public LargeInternalRowSerializerTest() {
            super(getRowSerializer(), getData());
        }

        private static InternalRow[] getData() {
            GenericRow row = new GenericRow(13);
            row.setField(0, 2);
            row.setField(1, null);
            row.setField(3, null);
            row.setField(4, null);
            row.setField(5, null);
            row.setField(6, null);
            row.setField(7, null);
            row.setField(8, null);
            row.setField(9, null);
            row.setField(10, null);
            row.setField(11, null);
            row.setField(12, fromString("Test"));

            return new InternalRow[] {row};
        }

        private static InternalRowSerializer getRowSerializer() {
            return new InternalRowSerializer(
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.INT(),
                    DataTypes.STRING());
        }
    }

    static final class InternalRowSerializerWithComplexTypesTest extends InternalRowSerializerTest {
        public InternalRowSerializerWithComplexTypesTest() {
            super(getRowSerializer(), getData());
        }

        private static InternalRow[] getData() {
            return new GenericRow[] {
                createRow(null, null, null, null, null),
                createRow(0, null, null, null, null),
                createRow(0, 0.0, null, null, null),
                createRow(0, 0.0, fromString("a"), null, null),
                createRow(1, 0.0, fromString("a"), null, null),
                createRow(1, 1.0, fromString("a"), null, null),
                createRow(1, 1.0, fromString("b"), null, null),
                createRow(
                        1,
                        1.0,
                        fromString("b"),
                        createArray(1),
                        createMap(new int[] {1}, new int[] {1})),
                createRow(
                        1,
                        1.0,
                        fromString("b"),
                        createArray(1, 2),
                        createMap(new int[] {1, 4}, new int[] {1, 2})),
                createRow(
                        1,
                        1.0,
                        fromString("b"),
                        createArray(1, 2, 3),
                        createMap(new int[] {1, 5}, new int[] {1, 3})),
                createRow(
                        1,
                        1.0,
                        fromString("b"),
                        createArray(1, 2, 3, 4),
                        createMap(new int[] {1, 6}, new int[] {1, 4})),
                createRow(
                        1,
                        1.0,
                        fromString("b"),
                        createArray(1, 2, 3, 4, 5),
                        createMap(new int[] {1, 7}, new int[] {1, 5})),
                createRow(
                        1,
                        1.0,
                        fromString("b"),
                        createArray(1, 2, 3, 4, 5, 6),
                        createMap(new int[] {1, 8}, new int[] {1, 6}))
            };
        }

        private static InternalRowSerializer getRowSerializer() {
            return new InternalRowSerializer(
                    DataTypes.INT(),
                    DataTypes.DOUBLE(),
                    DataTypes.STRING(),
                    DataTypes.ARRAY(DataTypes.INT()),
                    DataTypes.MAP(DataTypes.INT(), DataTypes.INT()));
        }
    }

    static final class InternalRowSerializerWithNestedInternalRowTest
            extends InternalRowSerializerTest {

        private static final DataType NESTED_DATA_TYPE =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "ri", DataTypes.INT()),
                        DataTypes.FIELD(1, "rs", DataTypes.STRING()),
                        DataTypes.FIELD(2, "rb", DataTypes.BIGINT()));

        public InternalRowSerializerWithNestedInternalRowTest() {
            super(getRowSerializer(), getData());
        }

        private static InternalRow[] getData() {
            final DataType outerDataType =
                    DataTypes.ROW(
                            DataTypes.FIELD(0, "i", DataTypes.INT()),
                            DataTypes.FIELD(1, "r", NESTED_DATA_TYPE),
                            DataTypes.FIELD(2, "s", DataTypes.STRING()));

            final InternalRowSerializer outerSerializer =
                    (InternalRowSerializer) InternalSerializers.<InternalRow>create(outerDataType);

            final GenericRow outerRow1 =
                    GenericRow.of(
                            12,
                            GenericRow.of(34, BinaryString.fromString("56"), 78L),
                            BinaryString.fromString("910"));
            final InternalRow nestedRow1 = outerSerializer.toBinaryRow(outerRow1).getRow(1, 3);

            final GenericRow outerRow2 =
                    GenericRow.of(
                            12, GenericRow.of(null, BinaryString.fromString("56"), 78L), null);
            final InternalRow nestedRow2 = outerSerializer.toBinaryRow(outerRow2).getRow(1, 3);

            return new InternalRow[] {nestedRow1, nestedRow2};
        }

        private static InternalRowSerializer getRowSerializer() {
            return (InternalRowSerializer)
                    InternalSerializers.<InternalRow>create(NESTED_DATA_TYPE);
        }
    }
}
