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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalArraySerializer;
import org.apache.paimon.data.serializer.InternalMapSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.RepeatedTest;
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
