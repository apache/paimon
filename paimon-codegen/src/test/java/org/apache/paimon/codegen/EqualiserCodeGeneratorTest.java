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

package org.apache.paimon.codegen;

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalArraySerializer;
import org.apache.paimon.data.serializer.InternalMapSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.InternalVectorSerializer;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static org.apache.paimon.utils.TypeUtils.castFromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link EqualiserCodeGenerator}. */
public class EqualiserCodeGeneratorTest {

    private static final Map<DataTypeRoot, GeneratedData> TEST_DATA = new HashMap<>();

    static {
        TEST_DATA.put(
                DataTypeRoot.CHAR,
                new GeneratedData(
                        DataTypes.CHAR(1),
                        Pair.of(BinaryString.fromString("1"), BinaryString.fromString("2"))));
        TEST_DATA.put(
                DataTypeRoot.VARCHAR,
                new GeneratedData(
                        DataTypes.VARCHAR(1),
                        Pair.of(BinaryString.fromString("3"), BinaryString.fromString("4"))));
        TEST_DATA.put(
                DataTypeRoot.BOOLEAN, new GeneratedData(DataTypes.BOOLEAN(), Pair.of(true, false)));
        TEST_DATA.put(
                DataTypeRoot.BINARY,
                new GeneratedData(DataTypes.BINARY(1), Pair.of("5".getBytes(), "6".getBytes())));
        TEST_DATA.put(
                DataTypeRoot.VARBINARY,
                new GeneratedData(DataTypes.VARBINARY(1), Pair.of("7".getBytes(), "8".getBytes())));
        TEST_DATA.put(
                DataTypeRoot.DECIMAL,
                new GeneratedData(
                        DataTypes.DECIMAL(10, 0),
                        Pair.of(
                                Decimal.fromUnscaledLong(9, 10, 0),
                                Decimal.fromUnscaledLong(10, 10, 0))));
        TEST_DATA.put(
                DataTypeRoot.TINYINT,
                new GeneratedData(DataTypes.TINYINT(), Pair.of((byte) 11, (byte) 12)));
        TEST_DATA.put(
                DataTypeRoot.SMALLINT,
                new GeneratedData(DataTypes.SMALLINT(), Pair.of((short) 13, (short) 14)));
        TEST_DATA.put(DataTypeRoot.INTEGER, new GeneratedData(DataTypes.INT(), Pair.of(15, 16)));
        TEST_DATA.put(
                DataTypeRoot.BIGINT, new GeneratedData(DataTypes.BIGINT(), Pair.of(17L, 18L)));
        TEST_DATA.put(
                DataTypeRoot.FLOAT, new GeneratedData(DataTypes.FLOAT(), Pair.of(19.0f, 20.0f)));
        TEST_DATA.put(
                DataTypeRoot.DOUBLE, new GeneratedData(DataTypes.DOUBLE(), Pair.of(21.0d, 22.0d)));
        TEST_DATA.put(
                DataTypeRoot.DATE,
                new GeneratedData(
                        DataTypes.DATE(),
                        Pair.of(
                                castFromString("2023-05-23 09:30:00.0", DataTypes.DATE()),
                                castFromString("2023-05-24 09:30:00.0", DataTypes.DATE()))));
        TEST_DATA.put(
                DataTypeRoot.TIME_WITHOUT_TIME_ZONE,
                new GeneratedData(
                        DataTypes.TIME(),
                        Pair.of(
                                castFromString("09:30:00.0", DataTypes.TIME()),
                                castFromString("10:30:00.0", DataTypes.TIME()))));
        TEST_DATA.put(
                DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                new GeneratedData(
                        DataTypes.TIMESTAMP_MILLIS(),
                        Pair.of(
                                Timestamp.fromEpochMillis(1684814400000L),
                                Timestamp.fromEpochMillis(1684814500000L))));
        TEST_DATA.put(
                DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                new GeneratedData(
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                        Pair.of(
                                Timestamp.fromEpochMillis(1684814600000L),
                                Timestamp.fromEpochMillis(1684814700000L))));
        TEST_DATA.put(
                DataTypeRoot.ARRAY,
                new GeneratedData(
                        DataTypes.ARRAY(new VarCharType(1)),
                        Pair.of(
                                castFromString("[1,2,3]", DataTypes.ARRAY(new VarCharType())),
                                castFromString("[4,5,6]", DataTypes.ARRAY(new VarCharType()))),
                        new InternalArraySerializer(DataTypes.VARCHAR(1))));
        TEST_DATA.put(
                DataTypeRoot.VECTOR,
                new GeneratedData(
                        DataTypes.VECTOR(3, DataTypes.FLOAT()),
                        Pair.of(
                                castFromString(
                                        "[1.1,2.2,3.3]", DataTypes.VECTOR(3, DataTypes.FLOAT())),
                                castFromString(
                                        "[4.4,5.5,6.6]", DataTypes.VECTOR(3, DataTypes.FLOAT()))),
                        new InternalVectorSerializer(DataTypes.FLOAT(), 3)));
        TEST_DATA.put(
                DataTypeRoot.MULTISET,
                new GeneratedData(
                        DataTypes.MULTISET(new IntType()),
                        Pair.of(
                                new GenericMap(
                                        new HashMap<Integer, Integer>() {
                                            {
                                                put(23, 23);
                                                put(24, 24);
                                            }
                                        }),
                                new GenericMap(
                                        new HashMap<Integer, Integer>() {
                                            {
                                                put(23, 23);
                                                put(25, 25);
                                            }
                                        })),
                        new InternalMapSerializer(DataTypes.INT(), DataTypes.INT())));
        TEST_DATA.put(
                DataTypeRoot.MAP,
                new GeneratedData(
                        DataTypes.MAP(new IntType(), new IntType()),
                        Pair.of(
                                new GenericMap(
                                        new HashMap<Integer, Integer>() {
                                            {
                                                put(26, 27);
                                                put(28, 29);
                                            }
                                        }),
                                new GenericMap(
                                        new HashMap<Integer, Integer>() {
                                            {
                                                put(26, 27);
                                                put(28, 30);
                                            }
                                        })),
                        new InternalMapSerializer(DataTypes.INT(), DataTypes.INT())));
        TEST_DATA.put(
                DataTypeRoot.ROW,
                new GeneratedData(
                        DataTypes.ROW(DataTypes.INT(), DataTypes.VARCHAR(2)),
                        Pair.of(
                                GenericRow.of(31, BinaryString.fromString("32")),
                                GenericRow.of(31, BinaryString.fromString("33"))),
                        new InternalRowSerializer(DataTypes.INT(), DataTypes.VARCHAR(2))));
        TEST_DATA.put(
                DataTypeRoot.VARIANT,
                new GeneratedData(
                        DataTypes.VARIANT(),
                        Pair.of(
                                GenericVariant.fromJson("{\"age\":27,\"city\":\"Beijing\"}"),
                                GenericVariant.fromJson("{\"age\":27,\"city\":\"Hangzhou\"}"))));
        TEST_DATA.put(
                DataTypeRoot.BLOB,
                new GeneratedData(
                        DataTypes.BLOB(),
                        Pair.of(
                                new BlobData(new byte[] {1, 2, 3}),
                                new BlobData(new byte[] {4, 5, 6}))));
    }

    @ParameterizedTest
    @EnumSource(DataTypeRoot.class)
    public void testSingleField(DataTypeRoot dataTypeRoot) {
        GeneratedData testData = TEST_DATA.get(dataTypeRoot);
        if (testData == null) {
            throw new UnsupportedOperationException("Unsupported type: " + dataTypeRoot);
        }

        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(new DataType[] {testData.dataType})
                        .generateRecordEqualiser("singleFieldEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());
        Function<Object, BinaryRow> func =
                o -> {
                    BinaryRow row = new BinaryRow(1);
                    BinaryRowWriter writer = new BinaryRowWriter(row);
                    BinaryWriter.write(writer, 0, o, testData.dataType, testData.serializer);
                    writer.complete();
                    return row;
                };
        assertBoolean(equaliser, func, testData.left(), testData.left(), true);
        assertBoolean(equaliser, func, testData.left(), testData.right(), false);
    }

    @RepeatedTest(100)
    public void testProjection() {
        GeneratedData field0 = TEST_DATA.get(DataTypeRoot.INTEGER);
        GeneratedData field1 = TEST_DATA.get(DataTypeRoot.VARCHAR);
        GeneratedData field2 = TEST_DATA.get(DataTypeRoot.BIGINT);

        RecordEqualiser equaliser =
                new EqualiserCodeGenerator(
                                new DataType[] {field0.dataType, field1.dataType, field2.dataType},
                                new int[] {1, 2})
                        .generateRecordEqualiser("projectionFieldEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        boolean result =
                equaliser.equals(
                        GenericRow.of(field0.left(), field1.left(), field2.left()),
                        GenericRow.of(field0.right(), field1.right(), field2.right()));
        boolean expected =
                Objects.equals(
                        GenericRow.of(field1.left(), field2.left()),
                        GenericRow.of(field1.right(), field2.right()));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testFloatingPointEqualiserMatchesJdkComparison() {
        RecordEqualiser doubleEqualiser =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.DOUBLE()})
                        .generateRecordEqualiser("doubleFieldEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());
        double[] doubles = {
            Double.NEGATIVE_INFINITY, -1.0d, -0.0d, 0.0d, 1.0d, Double.POSITIVE_INFINITY, Double.NaN
        };
        for (double a : doubles) {
            for (double b : doubles) {
                assertFloatingPointEqualiser(
                        doubleEqualiser, DataTypes.DOUBLE(), a, b, Double.compare(a, b) == 0);
            }
        }
        assertFloatingPointEqualiser(
                doubleEqualiser,
                DataTypes.DOUBLE(),
                Double.longBitsToDouble(0x7ff8000000000001L),
                Double.NaN,
                true);

        RecordEqualiser floatEqualiser =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.FLOAT()})
                        .generateRecordEqualiser("floatFieldEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());
        float[] floats = {
            Float.NEGATIVE_INFINITY, -1.0f, -0.0f, 0.0f, 1.0f, Float.POSITIVE_INFINITY, Float.NaN
        };
        for (float a : floats) {
            for (float b : floats) {
                assertFloatingPointEqualiser(
                        floatEqualiser, DataTypes.FLOAT(), a, b, Float.compare(a, b) == 0);
            }
        }
        assertFloatingPointEqualiser(
                floatEqualiser,
                DataTypes.FLOAT(),
                Float.intBitsToFloat(0x7fc00001),
                Float.NaN,
                true);

        RecordEqualiser doubleArrayEqualiser =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.ARRAY(DataTypes.DOUBLE())})
                        .generateRecordEqualiser("doubleArrayFieldEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());
        assertFloatingPointEqualiser(
                doubleArrayEqualiser,
                DataTypes.ARRAY(DataTypes.DOUBLE()),
                BinaryArray.fromPrimitiveArray(
                        new double[] {Double.longBitsToDouble(0x7ff8000000000001L)}),
                BinaryArray.fromPrimitiveArray(new double[] {Double.NaN}),
                true);

        RecordEqualiser floatArrayEqualiser =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.ARRAY(DataTypes.FLOAT())})
                        .generateRecordEqualiser("floatArrayFieldEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());
        assertFloatingPointEqualiser(
                floatArrayEqualiser,
                DataTypes.ARRAY(DataTypes.FLOAT()),
                BinaryArray.fromPrimitiveArray(new float[] {Float.intBitsToFloat(0x7fc00001)}),
                BinaryArray.fromPrimitiveArray(new float[] {Float.NaN}),
                true);

        DataType doubleArrayType = DataTypes.ARRAY(DataTypes.DOUBLE());
        RecordEqualiser nestedDoubleArrayEqualiser =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.ARRAY(doubleArrayType)})
                        .generateRecordEqualiser("nestedDoubleArrayFieldEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());
        assertFloatingPointEqualiser(
                nestedDoubleArrayEqualiser,
                DataTypes.ARRAY(doubleArrayType),
                new GenericArray(
                        new Object[] {
                            BinaryArray.fromPrimitiveArray(
                                    new double[] {Double.longBitsToDouble(0x7ff8000000000001L)})
                        }),
                new GenericArray(
                        new Object[] {BinaryArray.fromPrimitiveArray(new double[] {Double.NaN})}),
                true);

        DataType doubleRowType = DataTypes.ROW(DataTypes.DOUBLE());
        RecordEqualiser doubleRowArrayEqualiser =
                new EqualiserCodeGenerator(new DataType[] {DataTypes.ARRAY(doubleRowType)})
                        .generateRecordEqualiser("doubleRowArrayFieldEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());
        assertFloatingPointEqualiser(
                doubleRowArrayEqualiser,
                DataTypes.ARRAY(doubleRowType),
                new GenericArray(
                        new Object[] {GenericRow.of(Double.longBitsToDouble(0x7ff8000000000001L))}),
                new GenericArray(new Object[] {GenericRow.of(Double.NaN)}),
                true);

        DataType doubleRowMapType = DataTypes.MAP(doubleRowType, DataTypes.INT());
        RecordEqualiser doubleRowMapEqualiser =
                new EqualiserCodeGenerator(new DataType[] {doubleRowMapType})
                        .generateRecordEqualiser("doubleRowMapFieldEquals")
                        .newInstance(Thread.currentThread().getContextClassLoader());
        Map<Object, Object> leftMap = new HashMap<>();
        leftMap.put(GenericRow.of(Double.longBitsToDouble(0x7ff8000000000001L)), 1);
        Map<Object, Object> rightMap = new HashMap<>();
        rightMap.put(GenericRow.of(Double.NaN), 1);
        assertFloatingPointEqualiser(
                doubleRowMapEqualiser,
                doubleRowMapType,
                new GenericMap(leftMap),
                new GenericMap(rightMap),
                true);
    }

    private static void assertFloatingPointEqualiser(
            RecordEqualiser equaliser,
            DataType dataType,
            Object left,
            Object right,
            boolean equal) {
        Function<Object, BinaryRow> toBinaryRow =
                value -> {
                    BinaryRow row = new BinaryRow(1);
                    BinaryRowWriter writer = new BinaryRowWriter(row);
                    Serializer<?> serializer = InternalSerializers.create(dataType);
                    BinaryWriter.write(writer, 0, value, dataType, serializer);
                    writer.complete();
                    return row;
                };
        assertThat(equaliser.equals(GenericRow.of(left), GenericRow.of(right)))
                .as("equals(generic %s, generic %s)", left, right)
                .isEqualTo(equal);
        assertThat(equaliser.equals(toBinaryRow.apply(left), GenericRow.of(right)))
                .as("equals(binary %s, generic %s)", left, right)
                .isEqualTo(equal);
        assertThat(equaliser.equals(GenericRow.of(left), toBinaryRow.apply(right)))
                .as("equals(generic %s, binary %s)", left, right)
                .isEqualTo(equal);
        assertThat(equaliser.equals(toBinaryRow.apply(left), toBinaryRow.apply(right)))
                .as("equals(binary %s, binary %s)", left, right)
                .isEqualTo(equal);
    }

    @RepeatedTest(100)
    public void testManyFields() {
        int size = 499;
        GeneratedData[] generatedData = new GeneratedData[size];
        ThreadLocalRandom random = ThreadLocalRandom.current();
        DataTypeRoot[] dataTypeRoots = DataTypeRoot.values();
        for (int i = 0; i < size; i++) {
            int index = random.nextInt(0, dataTypeRoots.length);
            GeneratedData testData = TEST_DATA.get(dataTypeRoots[index]);
            if (testData == null) {
                throw new UnsupportedOperationException(
                        "Unsupported type: " + dataTypeRoots[index]);
            }
            generatedData[i] = testData;
        }

        final RecordEqualiser equaliser =
                new EqualiserCodeGenerator(
                                Arrays.stream(generatedData)
                                        .map(d -> d.dataType)
                                        .toArray(DataType[]::new))
                        .generateRecordEqualiser("ManyFields")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        Object[] fields1 = new Object[size];
        Object[] fields2 = new Object[size];
        boolean equal = true;
        for (int i = 0; i < size; i++) {
            boolean randomEqual = random.nextBoolean();
            fields1[i] = generatedData[i].left();
            fields2[i] = randomEqual ? generatedData[i].left() : generatedData[i].right();
            equal &= randomEqual;
        }
        assertThat(equaliser.equals(GenericRow.of(fields1), GenericRow.of(fields1))).isTrue();
        assertThat(equaliser.equals(GenericRow.of(fields1), GenericRow.of(fields2)))
                .isEqualTo(equal);
    }

    private static <T> void assertBoolean(
            RecordEqualiser equaliser,
            Function<T, BinaryRow> toBinaryRow,
            T o1,
            T o2,
            boolean bool) {
        assertThat(equaliser.equals(GenericRow.of(o1), GenericRow.of(o2))).isEqualTo(bool);
        assertThat(equaliser.equals(toBinaryRow.apply(o1), GenericRow.of(o2))).isEqualTo(bool);
        assertThat(equaliser.equals(GenericRow.of(o1), toBinaryRow.apply(o2))).isEqualTo(bool);
        assertThat(equaliser.equals(toBinaryRow.apply(o1), toBinaryRow.apply(o2))).isEqualTo(bool);
    }

    private static class GeneratedData {
        public final DataType dataType;
        public final Pair<Object, Object> data;
        public final Serializer<?> serializer;

        public GeneratedData(DataType dataType, Pair<Object, Object> data) {
            this(dataType, data, null);
        }

        public GeneratedData(
                DataType dataType, Pair<Object, Object> data, Serializer<?> serializer) {
            this.dataType = dataType;
            this.data = data;
            this.serializer = serializer;
        }

        public Object left() {
            return data.getLeft();
        }

        public Object right() {
            return data.getRight();
        }
    }
}
