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

package org.apache.paimon.arrow.vector;

import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.arrow.ArrowFieldTypeConversion;
import org.apache.paimon.arrow.converter.Arrow2PaimonVectorConverter;
import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.arrow.writer.ArrowFieldWriterFactoryVisitor;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.paimon.arrow.vector.ArrowFormatWriter}. */
public class ArrowFormatWriterTest {

    private static final Random RND = ThreadLocalRandom.current();
    private static final boolean[] NULLABLE;
    private static final RowType PRIMITIVE_TYPE;

    static {
        int cnt = 18;
        NULLABLE = new boolean[cnt];
        for (int i = 0; i < cnt; i++) {
            NULLABLE[i] = RND.nextBoolean();
        }

        List<DataField> dataFields = new ArrayList<>();
        dataFields.add(new DataField(0, "char", DataTypes.CHAR(10).copy(NULLABLE[0])));
        dataFields.add(new DataField(1, "varchar", DataTypes.VARCHAR(20).copy(NULLABLE[1])));
        dataFields.add(new DataField(2, "boolean", DataTypes.BOOLEAN().copy(NULLABLE[2])));
        dataFields.add(new DataField(3, "binary", DataTypes.BINARY(10).copy(NULLABLE[3])));
        dataFields.add(new DataField(4, "varbinary", DataTypes.VARBINARY(20).copy(NULLABLE[4])));
        dataFields.add(new DataField(5, "decimal1", DataTypes.DECIMAL(2, 2).copy(NULLABLE[5])));
        dataFields.add(new DataField(6, "decimal2", DataTypes.DECIMAL(38, 2).copy(NULLABLE[6])));
        dataFields.add(new DataField(7, "decimal3", DataTypes.DECIMAL(10, 1).copy(NULLABLE[7])));
        dataFields.add(new DataField(8, "tinyint", DataTypes.TINYINT().copy(NULLABLE[8])));
        dataFields.add(new DataField(9, "smallint", DataTypes.SMALLINT().copy(NULLABLE[9])));
        dataFields.add(new DataField(10, "int", DataTypes.INT().copy(NULLABLE[10])));
        dataFields.add(new DataField(11, "bigint", DataTypes.BIGINT().copy(NULLABLE[11])));
        dataFields.add(new DataField(12, "float", DataTypes.FLOAT().copy(NULLABLE[12])));
        dataFields.add(new DataField(13, "double", DataTypes.DOUBLE().copy(NULLABLE[13])));
        dataFields.add(new DataField(14, "date", DataTypes.DATE().copy(NULLABLE[14])));
        dataFields.add(new DataField(15, "timestamp3", DataTypes.TIMESTAMP(3).copy(NULLABLE[15])));
        dataFields.add(new DataField(16, "timestamp6", DataTypes.TIMESTAMP(6).copy(NULLABLE[16])));
        dataFields.add(
                new DataField(
                        17,
                        "timestampLZ9",
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9).copy(NULLABLE[17])));
        PRIMITIVE_TYPE = new RowType(dataFields);
    }

    @Test
    public void testWrite() {
        try (ArrowFormatWriter writer = new ArrowFormatWriter(PRIMITIVE_TYPE, 4096, true)) {
            List<InternalRow> list = new ArrayList<>();
            List<InternalRow.FieldGetter> fieldGetters = new ArrayList<>();

            for (int i = 0; i < PRIMITIVE_TYPE.getFieldCount(); i++) {
                fieldGetters.add(InternalRow.createFieldGetter(PRIMITIVE_TYPE.getTypeAt(i), i));
            }
            for (int i = 0; i < 1000; i++) {
                list.add(GenericRow.of(randomRowValues(null)));
            }

            list.forEach(writer::write);

            writer.flush();
            VectorSchemaRoot vectorSchemaRoot = writer.getVectorSchemaRoot();

            ArrowBatchReader arrowBatchReader = new ArrowBatchReader(PRIMITIVE_TYPE, true);
            Iterable<InternalRow> rows = arrowBatchReader.readBatch(vectorSchemaRoot);

            Iterator<InternalRow> iterator = rows.iterator();
            for (int i = 0; i < 1000; i++) {
                InternalRow actual = iterator.next();
                InternalRow expectec = list.get(i);

                for (InternalRow.FieldGetter fieldGetter : fieldGetters) {
                    assertThat(fieldGetter.getFieldOrNull(actual))
                            .isEqualTo(fieldGetter.getFieldOrNull(expectec));
                }
            }
            vectorSchemaRoot.close();
        }
    }

    @Test
    public void testReadWithSchemaMessUp() {
        try (ArrowFormatWriter writer = new ArrowFormatWriter(PRIMITIVE_TYPE, 4096, true)) {
            List<InternalRow> list = new ArrayList<>();
            List<InternalRow.FieldGetter> fieldGetters = new ArrayList<>();

            for (int i = 0; i < PRIMITIVE_TYPE.getFieldCount(); i++) {
                fieldGetters.add(InternalRow.createFieldGetter(PRIMITIVE_TYPE.getTypeAt(i), i));
            }
            for (int i = 0; i < 1000; i++) {
                list.add(GenericRow.of(randomRowValues(null)));
            }

            list.forEach(writer::write);

            writer.flush();
            VectorSchemaRoot vectorSchemaRoot = writer.getVectorSchemaRoot();

            // mess up the fields
            List<FieldVector> vectors = vectorSchemaRoot.getFieldVectors();
            FieldVector vector0 = vectors.get(0);
            for (int i = 0; i < vectors.size() - 1; i++) {
                vectors.set(i, vectors.get(i + 1));
            }
            vectors.set(vectors.size() - 1, vector0);

            ArrowBatchReader arrowBatchReader = new ArrowBatchReader(PRIMITIVE_TYPE, true);
            Iterable<InternalRow> rows = arrowBatchReader.readBatch(new VectorSchemaRoot(vectors));

            Iterator<InternalRow> iterator = rows.iterator();
            for (int i = 0; i < 1000; i++) {
                InternalRow actual = iterator.next();
                InternalRow expectec = list.get(i);

                for (InternalRow.FieldGetter fieldGetter : fieldGetters) {
                    assertThat(fieldGetter.getFieldOrNull(actual))
                            .isEqualTo(fieldGetter.getFieldOrNull(expectec));
                }
            }
            vectorSchemaRoot.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testWriteWithMemoryLimit(boolean limitMemory) {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "f0", DataTypes.BYTES()),
                                new DataField(1, "f1", DataTypes.BYTES())));
        Long memoryLimit = limitMemory ? 100 * 1024 * 1024L : null;
        try (ArrowFormatWriter writer = new ArrowFormatWriter(rowType, 4096, true, memoryLimit)) {

            GenericRow genericRow = new GenericRow(2);
            genericRow.setField(0, randomBytes(1024 * 1024, 1024 * 1024));
            genericRow.setField(1, randomBytes(1024 * 1024, 1024 * 1024));

            // normal write
            for (int i = 0; i < 200; i++) {
                boolean success = writer.write(genericRow);
                if (!success) {
                    writer.flush();
                    writer.reset();
                    writer.write(genericRow);
                }
            }
            writer.reset();

            if (limitMemory) {
                for (int i = 0; i < 64; i++) {
                    assertThat(writer.write(genericRow)).isTrue();
                }
                assertThat(writer.write(genericRow)).isFalse();
            }
            writer.reset();

            // Write batch records
            for (int i = 0; i < 2000; i++) {
                boolean success = writer.write(genericRow);
                if (!success) {
                    writer.flush();
                    writer.reset();
                    writer.write(genericRow);
                }
            }

            if (limitMemory) {
                assertThat(writer.memoryUsed()).isLessThan(memoryLimit);
                assertThat(writer.getAllocator().getAllocatedMemory())
                        .isGreaterThan(memoryLimit)
                        .isLessThan(2 * memoryLimit);
            }
        }
    }

    @Test
    public void testArrowBundleRecords() {
        try (ArrowFormatWriter writer = new ArrowFormatWriter(PRIMITIVE_TYPE, 4096, true)) {
            List<InternalRow> list = new ArrayList<>();
            List<InternalRow.FieldGetter> fieldGetters = new ArrayList<>();

            for (int i = 0; i < PRIMITIVE_TYPE.getFieldCount(); i++) {
                fieldGetters.add(InternalRow.createFieldGetter(PRIMITIVE_TYPE.getTypeAt(i), i));
            }
            for (int i = 0; i < 1000; i++) {
                list.add(GenericRow.of(randomRowValues(null)));
            }

            list.forEach(writer::write);

            writer.flush();
            VectorSchemaRoot vectorSchemaRoot = writer.getVectorSchemaRoot();

            Iterator<InternalRow> iterator =
                    new ArrowBundleRecords(vectorSchemaRoot, PRIMITIVE_TYPE, true).iterator();
            for (int i = 0; i < 1000; i++) {
                InternalRow actual = iterator.next();
                InternalRow expectec = list.get(i);

                for (InternalRow.FieldGetter fieldGetter : fieldGetters) {
                    assertThat(fieldGetter.getFieldOrNull(actual))
                            .isEqualTo(fieldGetter.getFieldOrNull(expectec));
                }
            }
            vectorSchemaRoot.close();
        }
    }

    @Test
    public void testCWriter() {
        try (ArrowFormatCWriter writer = new ArrowFormatCWriter(PRIMITIVE_TYPE, 4096, true)) {
            writeAndCheck(writer);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testWriteWithExternalAllocator(boolean allocationFailed) {
        long maxAllocation = allocationFailed ? 1024L : Long.MAX_VALUE;
        try (RootAllocator rootAllocator = new RootAllocator();
                BufferAllocator allocator =
                        rootAllocator.newChildAllocator("paimonWriter", 0, maxAllocation);
                ArrowFormatCWriter writer =
                        new ArrowFormatCWriter(PRIMITIVE_TYPE, 4096, true, allocator)) {
            writeAndCheck(writer);
        } catch (OutOfMemoryException e) {
            if (!allocationFailed) {
                throw e;
            }
        }
    }

    @Test
    public void testArrowFormatCWriterWithEmptySchema() {
        RowType emptyschema = new RowType(new ArrayList<>());

        try (RootAllocator rootAllocator = new RootAllocator();
                BufferAllocator allocator =
                        rootAllocator.newChildAllocator("paimonWriter", 0, Long.MAX_VALUE);
                ArrowFormatCWriter writer =
                        new ArrowFormatCWriter(emptyschema, 4096, true, allocator)) {
            for (int i = 0; i < 100; i++) {
                writer.write(GenericRow.of());
            }
            writer.flush();
            ArrowCStruct cStruct = writer.toCStruct();
            assertThat(cStruct).isNotNull();
            writer.release();
        }
    }

    @Test
    public void testWriteArrayMapTwice() {
        try (ArrowFormatWriter arrowFormatWriter =
                new ArrowFormatWriter(
                        RowType.of(
                                DataTypes.ARRAY(
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))),
                        1,
                        true)) {
            writeAndCheckArrayMap(arrowFormatWriter);
            writeAndCheckArrayMap(arrowFormatWriter);
        }
    }

    @Test
    public void testCustomArrowFormatCWriter() {
        // Create custom field type visitor that converts decimals to binary
        ArrowFieldTypeConversion.ArrowFieldTypeVisitor customFieldTypeVisitor =
                new CustomDecimalArrowConversion.CustomArrowFieldTypeFactory();

        // Create custom field writer factory visitor for decimal to binary conversion
        ArrowFieldWriterFactoryVisitor customFieldWriterVisitor =
                new CustomDecimalArrowConversion.CustomArrowFieldWriterFactory();

        // Create custom vector converter visitor for binary to decimal conversion
        Arrow2PaimonVectorConverter.Arrow2PaimonVectorConvertorVisitor customConverterVisitor =
                new CustomDecimalArrowConversion.CustomArrow2PaimonVectorConvertorVisitor();

        try (RootAllocator allocator = new RootAllocator()) {
            // Create writer with custom visitors
            try (ArrowFormatCWriter writer =
                    new ArrowFormatCWriter(
                            new ArrowFormatWriter(
                                    PRIMITIVE_TYPE,
                                    4096,
                                    true,
                                    allocator,
                                    null,
                                    customFieldTypeVisitor,
                                    customFieldWriterVisitor))) {
                writeAndCheckCustom(writer, customConverterVisitor);
            }
        }
    }

    private void writeAndCheckArrayMap(ArrowFormatWriter arrowFormatWriter) {
        GenericRow genericRow = new GenericRow(1);
        Map<BinaryString, BinaryString> map = new HashMap<>();
        map.put(BinaryString.fromString("a"), BinaryString.fromString("b"));
        map.put(BinaryString.fromString("c"), BinaryString.fromString("d"));
        GenericArray array = new GenericArray(new Object[] {new GenericMap(map)});
        genericRow.setField(0, array);
        arrowFormatWriter.write(genericRow);
        arrowFormatWriter.flush();

        VectorSchemaRoot vsr = arrowFormatWriter.getVectorSchemaRoot();
        ListVector listVector = (ListVector) vsr.getVector(0);
        MapVector mapVector = (MapVector) listVector.getDataVector();
        assertThat(mapVector.getValueCount()).isEqualTo(1);
        VarCharVector keyVector =
                (VarCharVector) mapVector.getDataVector().getChildrenFromFields().get(0);
        assertThat(keyVector.getValueCount()).isEqualTo(2);
        assertThat(new String(keyVector.get(0))).isEqualTo("a");
        assertThat(new String(keyVector.get(1))).isEqualTo("c");
        VarCharVector valueVector =
                (VarCharVector) mapVector.getDataVector().getChildrenFromFields().get(1);
        assertThat(valueVector.getValueCount()).isEqualTo(2);
        assertThat(new String(valueVector.get(0))).isEqualTo("b");
        assertThat(new String(valueVector.get(1))).isEqualTo("d");
        arrowFormatWriter.reset();
    }

    @Test
    public void testWriteMapArrayTwice() {
        try (ArrowFormatWriter arrowFormatWriter =
                new ArrowFormatWriter(
                        RowType.of(
                                DataTypes.MAP(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT()))),
                        1,
                        true)) {
            writeAndCheckMapArray(arrowFormatWriter);
            writeAndCheckMapArray(arrowFormatWriter);
        }
    }

    private void writeAndCheckMapArray(ArrowFormatWriter arrowFormatWriter) {
        GenericRow genericRow = new GenericRow(1);
        GenericArray array1 = new GenericArray(new Object[] {1, 2});
        GenericArray array2 = new GenericArray(new Object[] {3, 4});
        Map<Integer, GenericArray> map = new HashMap<>();
        map.put(1, array1);
        map.put(2, array2);
        GenericMap genericMap = new GenericMap(map);
        genericRow.setField(0, genericMap);
        arrowFormatWriter.write(genericRow);
        arrowFormatWriter.flush();

        VectorSchemaRoot vsr = arrowFormatWriter.getVectorSchemaRoot();
        MapVector mapVector = (MapVector) vsr.getVector(0);
        IntVector keyVector = (IntVector) mapVector.getDataVector().getChildrenFromFields().get(0);
        assertThat(keyVector.getValueCount()).isEqualTo(2);
        assertThat(keyVector.get(0)).isEqualTo(1);
        assertThat(keyVector.get(1)).isEqualTo(2);
        ListVector valueVector =
                (ListVector) mapVector.getDataVector().getChildrenFromFields().get(1);
        assertThat(valueVector.getValueCount()).isEqualTo(2);
        IntVector innerValueVector = (IntVector) valueVector.getDataVector();
        assertThat(innerValueVector.getValueCount()).isEqualTo(4);
        assertThat(innerValueVector.get(0)).isEqualTo(1);
        assertThat(innerValueVector.get(1)).isEqualTo(2);
        assertThat(innerValueVector.get(2)).isEqualTo(3);
        assertThat(innerValueVector.get(3)).isEqualTo(4);
        arrowFormatWriter.reset();
    }

    @Test
    public void testWriteRowArrayTwice() {
        try (ArrowFormatWriter arrowFormatWriter =
                new ArrowFormatWriter(
                        RowType.of(DataTypes.ROW(DataTypes.ARRAY(DataTypes.INT()))), 1, true)) {
            writeAndCheckRowArray(arrowFormatWriter);
            writeAndCheckRowArray(arrowFormatWriter);
        }
    }

    private void writeAndCheckRowArray(ArrowFormatWriter arrowFormatWriter) {
        GenericRow genericRow = new GenericRow(1);
        GenericRow innerRow = new GenericRow(1);
        GenericArray array = new GenericArray(new Object[] {1, 2});
        innerRow.setField(0, array);
        genericRow.setField(0, innerRow);
        arrowFormatWriter.write(genericRow);
        arrowFormatWriter.flush();

        VectorSchemaRoot vsr = arrowFormatWriter.getVectorSchemaRoot();
        assertThat(vsr.getRowCount()).isEqualTo(1);
        StructVector structVector = (StructVector) vsr.getVector(0);
        ListVector listVector = (ListVector) structVector.getChildrenFromFields().get(0);
        assertThat(listVector.getValueCount()).isEqualTo(1);
        IntVector dataVector = (IntVector) listVector.getDataVector();
        assertThat(dataVector.getValueCount()).isEqualTo(2);
        assertThat(dataVector.get(0)).isEqualTo(1);
        assertThat(dataVector.get(1)).isEqualTo(2);
        arrowFormatWriter.reset();
    }

    private void writeAndCheck(ArrowFormatCWriter writer) {
        List<InternalRow> list = new ArrayList<>();
        List<InternalRow.FieldGetter> fieldGetters = new ArrayList<>();

        for (int i = 0; i < PRIMITIVE_TYPE.getFieldCount(); i++) {
            fieldGetters.add(InternalRow.createFieldGetter(PRIMITIVE_TYPE.getTypeAt(i), i));
        }
        for (int i = 0; i < 1000; i++) {
            list.add(GenericRow.of(randomRowValues(null)));
        }

        list.forEach(writer::write);

        writer.flush();
        VectorSchemaRoot vectorSchemaRoot = writer.getVectorSchemaRoot();

        ArrowBatchReader arrowBatchReader = new ArrowBatchReader(PRIMITIVE_TYPE, true);
        Iterable<InternalRow> rows = arrowBatchReader.readBatch(vectorSchemaRoot);

        Iterator<InternalRow> iterator = rows.iterator();
        for (int i = 0; i < 1000; i++) {
            InternalRow actual = iterator.next();
            InternalRow expectec = list.get(i);

            for (InternalRow.FieldGetter fieldGetter : fieldGetters) {
                assertThat(fieldGetter.getFieldOrNull(actual))
                        .isEqualTo(fieldGetter.getFieldOrNull(expectec));
            }
        }
        vectorSchemaRoot.close();
    }

    private void writeAndCheckCustom(
            ArrowFormatCWriter writer,
            Arrow2PaimonVectorConverter.Arrow2PaimonVectorConvertorVisitor visitor) {
        List<InternalRow> list = new ArrayList<>();
        List<InternalRow.FieldGetter> fieldGetters = new ArrayList<>();

        for (int i = 0; i < PRIMITIVE_TYPE.getFieldCount(); i++) {
            fieldGetters.add(InternalRow.createFieldGetter(PRIMITIVE_TYPE.getTypeAt(i), i));
        }
        for (int i = 0; i < 1000; i++) {
            list.add(GenericRow.of(randomRowValues(null)));
        }

        list.forEach(writer::write);

        writer.flush();
        VectorSchemaRoot vectorSchemaRoot = writer.getVectorSchemaRoot();

        ArrowBatchReader arrowBatchReader = new ArrowBatchReader(PRIMITIVE_TYPE, true, visitor);
        Iterable<InternalRow> rows = arrowBatchReader.readBatch(vectorSchemaRoot);

        Iterator<InternalRow> iterator = rows.iterator();
        for (int i = 0; i < 1000; i++) {
            InternalRow actual = iterator.next();
            InternalRow expectec = list.get(i);

            for (InternalRow.FieldGetter fieldGetter : fieldGetters) {
                assertThat(fieldGetter.getFieldOrNull(actual))
                        .isEqualTo(fieldGetter.getFieldOrNull(expectec));
            }
        }
        vectorSchemaRoot.close();
    }

    private Object[] randomRowValues(boolean[] nullable) {
        Object[] values = new Object[18];
        values[0] = BinaryString.fromString(StringUtils.getRandomString(RND, 10, 10));
        values[1] = BinaryString.fromString(StringUtils.getRandomString(RND, 1, 20));
        values[2] = RND.nextBoolean();
        values[3] = randomBytes(10, 10);
        values[4] = randomBytes(1, 20);
        values[5] = Decimal.fromBigDecimal(new BigDecimal("0.22"), 2, 2);
        values[6] = Decimal.fromBigDecimal(new BigDecimal("12312455.22"), 38, 2);
        values[7] = Decimal.fromBigDecimal(new BigDecimal("12455.1"), 10, 1);
        values[8] = (byte) RND.nextInt(Byte.MAX_VALUE);
        values[9] = (short) RND.nextInt(Short.MAX_VALUE);
        values[10] = RND.nextInt();
        values[11] = RND.nextLong();
        values[12] = RND.nextFloat();
        values[13] = RND.nextDouble();
        values[14] = RND.nextInt();
        values[15] = Timestamp.fromEpochMillis(RND.nextInt(1000));
        values[16] = Timestamp.fromEpochMillis(RND.nextInt(1000), RND.nextInt(1000) * 1000);
        values[17] = Timestamp.fromEpochMillis(RND.nextInt(1000), RND.nextInt(1000_000));

        for (int i = 0; i < 18; i++) {
            if (nullable != null && nullable[i] && RND.nextBoolean()) {
                values[i] = null;
            }
        }

        return values;
    }

    private byte[] randomBytes(int minLength, int maxLength) {
        int len = RND.nextInt(maxLength - minLength + 1) + minLength;
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) RND.nextInt(10);
        }
        return bytes;
    }
}
