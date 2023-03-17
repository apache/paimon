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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.data.serializer.InternalRowSerializerTest.createArray;
import static org.apache.paimon.data.serializer.InternalRowSerializerTest.createMap;
import static org.apache.paimon.data.serializer.InternalRowSerializerTest.createRow;
import static org.apache.paimon.data.serializer.InternalRowSerializerTest.deepEqualsInternalRow;

/** Test for {@link RowCompactedSerializer}. */
abstract class RowCompactedSerializerTest extends SerializerTestInstance<InternalRow> {

    private final RowCompactedSerializer serializer;

    RowCompactedSerializerTest(RowCompactedSerializer serializer, InternalRow[] testData) {
        super(serializer, testData);
        this.serializer = serializer;
    }

    @Override
    protected boolean deepEquals(InternalRow o1, InternalRow o2) {
        return deepEqualsInternalRow(
                o1,
                o2,
                new InternalRowSerializer(serializer.rowType()),
                new InternalRowSerializer(serializer.rowType()));
    }

    static final class SimpleTest extends RowCompactedSerializerTest {
        public SimpleTest() {
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

        private static RowCompactedSerializer getRowSerializer() {
            return new RowCompactedSerializer(RowType.of(DataTypes.INT(), DataTypes.STRING()));
        }
    }

    static final class LargeTest extends RowCompactedSerializerTest {
        public LargeTest() {
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

        private static RowCompactedSerializer getRowSerializer() {
            return new RowCompactedSerializer(
                    RowType.of(
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
                            DataTypes.STRING()));
        }
    }

    static final class ComplexTypesTest extends RowCompactedSerializerTest {
        public ComplexTypesTest() {
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

        private static RowCompactedSerializer getRowSerializer() {
            return new RowCompactedSerializer(
                    RowType.of(
                            DataTypes.INT(),
                            DataTypes.DOUBLE(),
                            DataTypes.STRING(),
                            DataTypes.ARRAY(DataTypes.INT()),
                            DataTypes.MAP(DataTypes.INT(), DataTypes.INT())));
        }
    }

    static final class NestedInternalRowTest extends RowCompactedSerializerTest {

        private static final RowType NESTED_DATA_TYPE =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "ri", DataTypes.INT()),
                        DataTypes.FIELD(1, "rs", DataTypes.STRING()),
                        DataTypes.FIELD(2, "rb", DataTypes.BIGINT()));

        public NestedInternalRowTest() {
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

        private static RowCompactedSerializer getRowSerializer() {
            return new RowCompactedSerializer(NESTED_DATA_TYPE);
        }
    }
}
