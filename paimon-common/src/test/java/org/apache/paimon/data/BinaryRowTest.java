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

package org.apache.paimon.data;

import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalArraySerializer;
import org.apache.paimon.data.serializer.InternalMapSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.paimon.data.BinaryString.fromBytes;
import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.data.MapDataUtil.convertToJavaMap;
import static org.assertj.core.api.Assertions.assertThat;

/** Test of {@link BinaryRow} and {@link BinaryRowWriter}. */
public class BinaryRowTest {

    @Test
    public void testBasic() {
        // consider header 1 byte.
        assertThat(new BinaryRow(0).getFixedLengthPartSize()).isEqualTo(8);
        assertThat(new BinaryRow(1).getFixedLengthPartSize()).isEqualTo(16);
        assertThat(new BinaryRow(65).getFixedLengthPartSize()).isEqualTo(536);
        assertThat(new BinaryRow(128).getFixedLengthPartSize()).isEqualTo(1048);

        MemorySegment segment = MemorySegment.wrap(new byte[100]);
        BinaryRow row = new BinaryRow(2);
        row.pointTo(segment, 10, 48);
        assertThat(segment).isSameAs(row.getSegments()[0]);
        row.setInt(0, 5);
        row.setDouble(1, 5.8D);
    }

    @Test
    public void testSetAndGet() throws IOException, ClassNotFoundException {
        MemorySegment segment = MemorySegment.wrap(new byte[100]);
        BinaryRow row = new BinaryRow(9);
        row.pointTo(segment, 20, 80);
        row.setNullAt(0);
        row.setInt(1, 11);
        row.setLong(2, 22);
        row.setDouble(3, 33);
        row.setBoolean(4, true);
        row.setShort(5, (short) 55);
        row.setByte(6, (byte) 66);
        row.setFloat(7, 77f);

        Consumer<BinaryRow> assertConsumer =
                assertRow -> {
                    assertThat((long) assertRow.getDouble(3)).isEqualTo(33L);
                    assertThat(assertRow.getInt(1)).isEqualTo(11);
                    assertThat(assertRow.isNullAt(0)).isTrue();
                    assertThat(assertRow.getShort(5)).isEqualTo((short) 55);
                    assertThat(assertRow.getLong(2)).isEqualTo(22L);
                    assertThat(assertRow.getBoolean(4)).isTrue();
                    assertThat(assertRow.getByte(6)).isEqualTo((byte) 66);
                    assertThat(assertRow.getFloat(7)).isEqualTo(77f);
                };

        assertConsumer.accept(row);

        // test serializable
        assertConsumer.accept(InstantiationUtil.clone(row));
    }

    @Test
    public void testWriter() {
        int arity = 13;
        BinaryRow row = new BinaryRow(arity);
        BinaryRowWriter writer = new BinaryRowWriter(row, 20);

        writer.writeString(0, fromString("1"));
        writer.writeString(3, fromString("1234567"));
        writer.writeString(5, fromString("12345678"));
        writer.writeString(9, fromString("啦啦啦啦啦我是快乐的粉刷匠"));

        writer.writeBoolean(1, true);
        writer.writeByte(2, (byte) 99);
        writer.writeDouble(6, 87.1d);
        writer.writeFloat(7, 26.1f);
        writer.writeInt(8, 88);
        writer.writeLong(10, 284);
        writer.writeShort(11, (short) 292);
        writer.setNullAt(12);

        writer.complete();

        assertTestWriterRow(row);
        assertTestWriterRow(row.copy());

        // test copy from var segments.
        int subSize = row.getFixedLengthPartSize() + 10;
        MemorySegment subMs1 = MemorySegment.wrap(new byte[subSize]);
        MemorySegment subMs2 = MemorySegment.wrap(new byte[subSize]);
        row.getSegments()[0].copyTo(0, subMs1, 0, subSize);
        row.getSegments()[0].copyTo(subSize, subMs2, 0, row.getSizeInBytes() - subSize);

        BinaryRow toCopy = new BinaryRow(arity);
        toCopy.pointTo(new MemorySegment[] {subMs1, subMs2}, 0, row.getSizeInBytes());
        assertThat(toCopy).isEqualTo(row);
        assertTestWriterRow(toCopy);
        assertTestWriterRow(toCopy.copy(new BinaryRow(arity)));
    }

    @Test
    public void testWriteString() {
        {
            // litter byte[]
            BinaryRow row = new BinaryRow(1);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            char[] chars = new char[2];
            chars[0] = 0xFFFF;
            chars[1] = 0;
            writer.writeString(0, fromString(new String(chars)));
            writer.complete();

            String str = row.getString(0).toString();
            assertThat(str.charAt(0)).isEqualTo(chars[0]);
            assertThat(str.charAt(1)).isEqualTo(chars[1]);
        }

        {
            // big byte[]
            String str = "啦啦啦啦啦我是快乐的粉刷匠";
            BinaryRow row = new BinaryRow(2);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.writeString(0, fromString(str));
            writer.writeString(1, fromBytes(str.getBytes(StandardCharsets.UTF_8)));
            writer.complete();

            assertThat(row.getString(0).toString()).isEqualTo(str);
            assertThat(row.getString(1).toString()).isEqualTo(str);
        }
    }

    @Test
    public void testPagesSer() throws IOException {
        MemorySegment[] memorySegments = new MemorySegment[5];
        ArrayList<MemorySegment> memorySegmentList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            memorySegments[i] = MemorySegment.wrap(new byte[64]);
            memorySegmentList.add(memorySegments[i]);
        }

        {
            // multi memorySegments
            String str = "啦啦啦啦啦我是快乐的粉刷匠，啦啦啦啦啦我是快乐的粉刷匠，" + "啦啦啦啦啦我是快乐的粉刷匠。";
            BinaryRow row = new BinaryRow(1);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.writeString(0, fromString(str));
            writer.complete();

            RandomAccessOutputView out = new RandomAccessOutputView(memorySegments, 64);
            BinaryRowSerializer serializer = new BinaryRowSerializer(1);
            serializer.serializeToPages(row, out);

            BinaryRow mapRow = serializer.createInstance();
            mapRow =
                    serializer.mapFromPages(
                            mapRow, new RandomAccessInputView(memorySegmentList, 64));
            writer.reset();
            writer.writeString(0, mapRow.getString(0));
            writer.complete();
            assertThat(row.getString(0).toString()).isEqualTo(str);

            BinaryRow deserRow =
                    serializer.deserializeFromPages(
                            new RandomAccessInputView(memorySegmentList, 64));
            writer.reset();
            writer.writeString(0, deserRow.getString(0));
            writer.complete();
            assertThat(row.getString(0).toString()).isEqualTo(str);
        }

        {
            // multi memorySegments
            String str1 = "啦啦啦啦啦我是快乐的粉刷匠，啦啦啦啦啦我是快乐的粉刷匠，" + "啦啦啦啦啦我是快乐的粉刷匠。";
            String str2 = "啦啦啦啦啦我是快乐的粉刷匠。";
            BinaryRow row = new BinaryRow(2);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.writeString(0, fromString(str1));
            writer.writeString(1, fromString(str2));
            writer.complete();

            RandomAccessOutputView out = new RandomAccessOutputView(memorySegments, 64);
            out.skipBytesToWrite(40);
            BinaryRowSerializer serializer = new BinaryRowSerializer(2);
            serializer.serializeToPages(row, out);

            RandomAccessInputView in = new RandomAccessInputView(memorySegmentList, 64);
            in.skipBytesToRead(40);
            BinaryRow mapRow = serializer.createInstance();
            mapRow = serializer.mapFromPages(mapRow, in);
            writer.reset();
            writer.writeString(0, mapRow.getString(0));
            writer.writeString(1, mapRow.getString(1));
            writer.complete();
            assertThat(row.getString(0).toString()).isEqualTo(str1);
            assertThat(row.getString(1).toString()).isEqualTo(str2);

            in = new RandomAccessInputView(memorySegmentList, 64);
            in.skipBytesToRead(40);
            BinaryRow deserRow = serializer.deserializeFromPages(in);
            writer.reset();
            writer.writeString(0, deserRow.getString(0));
            writer.writeString(1, deserRow.getString(1));
            writer.complete();
            assertThat(row.getString(0).toString()).isEqualTo(str1);
            assertThat(row.getString(1).toString()).isEqualTo(str2);
        }
    }

    private void assertTestWriterRow(BinaryRow row) {
        assertThat(row.getString(0).toString()).isEqualTo("1");
        assertThat(row.getInt(8)).isEqualTo(88);
        assertThat(row.getShort(11)).isEqualTo((short) 292);
        assertThat(row.getLong(10)).isEqualTo(284);
        assertThat(row.getByte(2)).isEqualTo((byte) 99);
        assertThat(row.getDouble(6)).isEqualTo(87.1d);
        assertThat(row.getFloat(7)).isEqualTo(26.1f);
        assertThat(row.getBoolean(1)).isTrue();
        assertThat(row.getString(3).toString()).isEqualTo("1234567");
        assertThat(row.getString(5).toString()).isEqualTo("12345678");
        assertThat(row.getString(9).toString()).isEqualTo("啦啦啦啦啦我是快乐的粉刷匠");
        assertThat(row.getString(9).hashCode()).isEqualTo(fromString("啦啦啦啦啦我是快乐的粉刷匠").hashCode());
        assertThat(row.isNullAt(12)).isTrue();
    }

    @Test
    public void testReuseWriter() {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, fromString("01234567"));
        writer.writeString(1, fromString("012345678"));
        writer.complete();
        assertThat(row.getString(0).toString()).isEqualTo("01234567");
        assertThat(row.getString(1).toString()).isEqualTo("012345678");

        writer.reset();
        writer.writeString(0, fromString("1"));
        writer.writeString(1, fromString("0123456789"));
        writer.complete();
        assertThat(row.getString(0).toString()).isEqualTo("1");
        assertThat(row.getString(1).toString()).isEqualTo("0123456789");
    }

    @Test
    public void anyNullTest() {
        {
            BinaryRow row = new BinaryRow(3);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            assertThat(row.anyNull()).isFalse();

            // test header should not compute by anyNull
            row.setRowKind(RowKind.UPDATE_BEFORE);
            assertThat(row.anyNull()).isFalse();

            writer.setNullAt(2);
            assertThat(row.anyNull()).isTrue();

            writer.setNullAt(0);
            assertThat(row.anyNull(new int[] {0, 1, 2})).isTrue();
            assertThat(row.anyNull(new int[] {1})).isFalse();

            writer.setNullAt(1);
            assertThat(row.anyNull()).isTrue();
        }

        int numFields = 80;
        for (int i = 0; i < numFields; i++) {
            BinaryRow row = new BinaryRow(numFields);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            row.setRowKind(RowKind.DELETE);
            assertThat(row.anyNull()).isFalse();
            writer.setNullAt(i);
            assertThat(row.anyNull()).isTrue();
        }
    }

    @Test
    public void testSingleSegmentBinaryRowHashCode() {
        final Random rnd = new Random(System.currentTimeMillis());
        // test hash stabilization
        BinaryRow row = new BinaryRow(13);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < 99; i++) {
            writer.reset();
            writer.writeString(0, fromString("" + rnd.nextInt()));
            writer.writeString(3, fromString("01234567"));
            writer.writeString(5, fromString("012345678"));
            writer.writeString(9, fromString("啦啦啦啦啦我是快乐的粉刷匠"));
            writer.writeBoolean(1, true);
            writer.writeByte(2, (byte) 99);
            writer.writeDouble(6, 87.1d);
            writer.writeFloat(7, 26.1f);
            writer.writeInt(8, 88);
            writer.writeLong(10, 284);
            writer.writeShort(11, (short) 292);
            writer.setNullAt(12);
            writer.complete();
            BinaryRow copy = row.copy();
            assertThat(copy.hashCode()).isEqualTo(row.hashCode());
        }

        // test hash distribution
        int count = 999999;
        Set<Integer> hashCodes = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            row.setInt(8, i);
            hashCodes.add(row.hashCode());
        }
        assertThat(hashCodes).hasSize(count);
        hashCodes.clear();
        row = new BinaryRow(1);
        writer = new BinaryRowWriter(row);
        for (int i = 0; i < count; i++) {
            writer.reset();
            writer.writeString(0, fromString("啦啦啦啦啦我是快乐的粉刷匠" + i));
            writer.complete();
            hashCodes.add(row.hashCode());
        }
        assertThat(hashCodes.size()).isGreaterThan((int) (count * 0.997));
    }

    @Test
    public void testHeaderSize() {
        assertThat(BinaryRow.calculateBitSetWidthInBytes(56)).isEqualTo(8);
        assertThat(BinaryRow.calculateBitSetWidthInBytes(57)).isEqualTo(16);
        assertThat(BinaryRow.calculateBitSetWidthInBytes(120)).isEqualTo(16);
        assertThat(BinaryRow.calculateBitSetWidthInBytes(121)).isEqualTo(24);
    }

    @Test
    public void testHeader() {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);

        writer.writeInt(0, 10);
        writer.setNullAt(1);
        writer.writeRowKind(RowKind.UPDATE_BEFORE);
        writer.complete();

        BinaryRow newRow = row.copy();
        assertThat(newRow).isEqualTo(row);
        assertThat(newRow.getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE);

        newRow.setRowKind(RowKind.DELETE);
        assertThat(newRow.getRowKind()).isEqualTo(RowKind.DELETE);
    }

    @Test
    public void testDecimal() {
        // 1.compact
        {
            int precision = 4;
            int scale = 2;
            BinaryRow row = new BinaryRow(2);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.writeDecimal(0, Decimal.fromUnscaledLong(5, precision, scale), precision);
            writer.setNullAt(1);
            writer.complete();

            assertThat(row.getDecimal(0, precision, scale).toString()).isEqualTo("0.05");
            assertThat(row.isNullAt(1)).isTrue();
            row.setDecimal(0, Decimal.fromUnscaledLong(6, precision, scale), precision);
            assertThat(row.getDecimal(0, precision, scale).toString()).isEqualTo("0.06");
        }

        // 2.not compact
        {
            int precision = 25;
            int scale = 5;
            Decimal decimal1 = Decimal.fromBigDecimal(BigDecimal.valueOf(5.55), precision, scale);
            Decimal decimal2 = Decimal.fromBigDecimal(BigDecimal.valueOf(6.55), precision, scale);

            BinaryRow row = new BinaryRow(2);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.writeDecimal(0, decimal1, precision);
            writer.writeDecimal(1, null, precision);
            writer.complete();

            assertThat(row.getDecimal(0, precision, scale).toString()).isEqualTo("5.55000");
            assertThat(row.isNullAt(1)).isTrue();
            row.setDecimal(0, decimal2, precision);
            assertThat(row.getDecimal(0, precision, scale).toString()).isEqualTo("6.55000");
        }
    }

    @Test
    public void testNested() {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeRow(
                0,
                GenericRow.of(fromString("1"), 1),
                new InternalRowSerializer(RowType.of(VarCharType.STRING_TYPE, new IntType())));
        writer.setNullAt(1);
        writer.complete();

        InternalRow nestedRow = row.getRow(0, 2);
        assertThat(nestedRow.getString(0).toString()).isEqualTo("1");
        assertThat(nestedRow.getInt(1)).isEqualTo(1);
        assertThat(row.isNullAt(1)).isTrue();
    }

    @Test
    public void testBinary() {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        byte[] bytes1 = new byte[] {1, -1, 5};
        byte[] bytes2 = new byte[] {1, -1, 5, 5, 1, 5, 1, 5};
        writer.writeBinary(0, bytes1);
        writer.writeBinary(1, bytes2);
        writer.complete();

        assertThat(row.getBinary(0)).isEqualTo(bytes1);
        assertThat(row.getBinary(1)).isEqualTo(bytes2);
    }

    @Test
    public void testBinaryArray() {
        // 1. array test
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter arrayWriter =
                new BinaryArrayWriter(
                        array, 3, BinaryArray.calculateFixLengthPartSize(DataTypes.INT()));

        arrayWriter.writeInt(0, 6);
        arrayWriter.setNullInt(1);
        arrayWriter.writeInt(2, 666);
        arrayWriter.complete();

        assertThat(6).isEqualTo(array.getInt(0));
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(666).isEqualTo(array.getInt(2));

        // 2. test write array to binary row
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter rowWriter = new BinaryRowWriter(row);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());
        rowWriter.writeArray(0, array, serializer);
        rowWriter.complete();

        BinaryArray array2 = (BinaryArray) row.getArray(0);
        assertThat(array2).isEqualTo(array);
        assertThat(array2.getInt(0)).isEqualTo(6);
        assertThat(array2.isNullAt(1)).isTrue();
        assertThat(array2.getInt(2)).isEqualTo(666);
    }

    @Test
    public void testGenericArray() {
        // 1. array test
        Integer[] javaArray = {6, null, 666};
        GenericArray array = new GenericArray(javaArray);

        assertThat(6).isEqualTo(array.getInt(0));
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(666).isEqualTo(array.getInt(2));

        // 2. test write array to binary row
        BinaryRow row2 = new BinaryRow(1);
        BinaryRowWriter writer2 = new BinaryRowWriter(row2);
        InternalArraySerializer serializer = new InternalArraySerializer(DataTypes.INT());
        writer2.writeArray(0, array, serializer);
        writer2.complete();

        InternalArray array2 = row2.getArray(0);
        assertThat(array2.getInt(0)).isEqualTo(6);
        assertThat(array2.isNullAt(1)).isTrue();
        assertThat(array2.getInt(2)).isEqualTo(666);
    }

    @Test
    public void testBinaryMap() {
        BinaryArray array1 = new BinaryArray();
        BinaryArrayWriter writer1 =
                new BinaryArrayWriter(
                        array1, 4, BinaryArray.calculateFixLengthPartSize(DataTypes.INT()));
        writer1.writeInt(0, 6);
        writer1.writeInt(1, 5);
        writer1.writeInt(2, 666);
        writer1.writeInt(3, 0);
        writer1.complete();

        BinaryArray array2 = new BinaryArray();
        BinaryArrayWriter writer2 =
                new BinaryArrayWriter(
                        array2, 4, BinaryArray.calculateFixLengthPartSize(DataTypes.STRING()));
        writer2.writeString(0, fromString("6"));
        writer2.writeString(1, fromString("5"));
        writer2.writeString(2, fromString("666"));
        writer2.setNullAt(3, DataTypes.STRING());
        writer2.complete();

        BinaryMap binaryMap = BinaryMap.valueOf(array1, array2);

        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter rowWriter = new BinaryRowWriter(row);
        InternalMapSerializer serializer =
                new InternalMapSerializer(DataTypes.STRING(), DataTypes.INT());
        rowWriter.writeMap(0, binaryMap, serializer);
        rowWriter.complete();

        BinaryMap map = (BinaryMap) row.getMap(0);
        BinaryArray key = map.keyArray();
        BinaryArray value = map.valueArray();

        assertThat(map).isEqualTo(binaryMap);
        assertThat(key).isEqualTo(array1);
        assertThat(value).isEqualTo(array2);

        assertThat(key.getInt(1)).isEqualTo(5);
        assertThat(value.getString(1)).isEqualTo(fromString("5"));
        assertThat(key.getInt(3)).isEqualTo(0);
        assertThat(value.isNullAt(3)).isTrue();
    }

    @Test
    public void testGenericMap() {
        Map<Object, Object> javaMap = new HashMap<>();
        javaMap.put(6, fromString("6"));
        javaMap.put(5, fromString("5"));
        javaMap.put(666, fromString("666"));
        javaMap.put(0, null);

        GenericMap genericMap = new GenericMap(javaMap);

        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter rowWriter = new BinaryRowWriter(row);
        InternalMapSerializer serializer =
                new InternalMapSerializer(DataTypes.INT(), DataTypes.STRING());
        rowWriter.writeMap(0, genericMap, serializer);
        rowWriter.complete();

        Map<Object, Object> map =
                convertToJavaMap(row.getMap(0), DataTypes.INT(), DataTypes.STRING());
        assertThat(map.get(6)).isEqualTo(fromString("6"));
        assertThat(map.get(5)).isEqualTo(fromString("5"));
        assertThat(map.get(666)).isEqualTo(fromString("666"));
        assertThat(map).containsKey(0);
        assertThat(map.get(0)).isNull();
    }

    @Test
    public void testSerializeVariousSize() throws IOException {
        // in this test, we are going to start serializing from the i-th byte (i in 0...`segSize`)
        // and the size of the row we're going to serialize is j bytes
        // (j in `rowFixLength` to the maximum length we can write)

        int segSize = 64;
        int segTotalNumber = 3;

        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        Random random = new Random();
        byte[] bytes = new byte[1024];
        random.nextBytes(bytes);
        writer.writeBinary(0, bytes);
        writer.complete();

        MemorySegment[] memorySegments = new MemorySegment[segTotalNumber];
        Map<MemorySegment, Integer> msIndex = new HashMap<>();
        for (int i = 0; i < segTotalNumber; i++) {
            memorySegments[i] = MemorySegment.wrap(new byte[segSize]);
            msIndex.put(memorySegments[i], i);
        }

        BinaryRowSerializer serializer = new BinaryRowSerializer(1);

        int rowSizeInt = 4;
        // note that as there is only one field in the row, the fixed-length part is 16 bytes
        // (header + 1 field)
        int rowFixLength = 16;
        for (int i = 0; i < segSize; i++) {
            // this is the maximum row size we can serialize
            // if we are going to serialize from the i-th byte of the input view
            int maxRowSize = (segSize * segTotalNumber) - i - rowSizeInt;
            if (segSize - i < rowFixLength + rowSizeInt) {
                // oops, we can't write the whole fixed-length part in the first segment
                // because the remaining space is too small, so we have to start serializing from
                // the second segment.
                // when serializing, we need to first write the length of the row,
                // then write the fixed-length part of the row.
                maxRowSize -= segSize - i;
            }
            for (int j = rowFixLength; j < maxRowSize; j++) {
                // ok, now we're going to serialize a row of j bytes
                testSerialize(row, memorySegments, msIndex, serializer, i, j);
            }
        }
    }

    private void testSerialize(
            BinaryRow row,
            MemorySegment[] memorySegments,
            Map<MemorySegment, Integer> msIndex,
            BinaryRowSerializer serializer,
            int position,
            int rowSize)
            throws IOException {
        RandomAccessOutputView out = new RandomAccessOutputView(memorySegments, 64);
        out.skipBytesToWrite(position);
        row.setTotalSize(rowSize);

        // this `row` contains random bytes, and now we're going to serialize `rowSize` bytes
        // (not including the row header) of the contents
        serializer.serializeToPages(row, out);

        // let's see how many segments we have written
        int segNumber = msIndex.get(out.getCurrentSegment()) + 1;
        int lastSegSize = out.getCurrentPositionInSegment();

        // now deserialize from the written segments
        ArrayList<MemorySegment> segments =
                new ArrayList<>(Arrays.asList(memorySegments).subList(0, segNumber));
        RandomAccessInputView input = new RandomAccessInputView(segments, 64, lastSegSize);
        input.skipBytesToRead(position);
        BinaryRow mapRow = serializer.createInstance();
        mapRow = serializer.mapFromPages(mapRow, input);

        assertThat(mapRow).isEqualTo(row);
    }

    @Test
    public void testZeroOutPaddingString() {

        Random random = new Random();
        byte[] bytes = new byte[1024];

        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);

        writer.reset();
        random.nextBytes(bytes);
        writer.writeBinary(0, bytes);
        writer.reset();
        writer.writeString(0, fromString("wahahah"));
        writer.complete();
        int hash1 = row.hashCode();

        writer.reset();
        random.nextBytes(bytes);
        writer.writeBinary(0, bytes);
        writer.reset();
        writer.writeString(0, fromString("wahahah"));
        writer.complete();
        int hash2 = row.hashCode();

        assertThat(hash2).isEqualTo(hash1);
    }

    @Test
    public void testHashAndCopy() throws IOException {
        MemorySegment[] segments = new MemorySegment[3];
        for (int i = 0; i < 3; i++) {
            segments[i] = MemorySegment.wrap(new byte[64]);
        }
        RandomAccessOutputView out = new RandomAccessOutputView(segments, 64);
        BinaryRowSerializer serializer = new BinaryRowSerializer(2);

        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(
                0, fromString("hahahahahahahahahahahahahahahahahahahhahahahahahahahahah"));
        writer.writeString(
                1, fromString("hahahahahahahahahahahahahahahahahahahhahahahahahahahahaa"));
        writer.complete();
        serializer.serializeToPages(row, out);

        ArrayList<MemorySegment> segmentList = new ArrayList<>(Arrays.asList(segments));
        RandomAccessInputView input = new RandomAccessInputView(segmentList, 64, 64);

        BinaryRow mapRow = serializer.createInstance();
        mapRow = serializer.mapFromPages(mapRow, input);
        assertThat(mapRow).isEqualTo(row);
        assertThat(mapRow.getString(0)).isEqualTo(row.getString(0));
        assertThat(mapRow.getString(1)).isEqualTo(row.getString(1));
        assertThat(mapRow.getString(1)).isNotEqualTo(row.getString(0));

        // test if the hash code before and after serialization are the same
        assertThat(mapRow.hashCode()).isEqualTo(row.hashCode());
        assertThat(mapRow.getString(0).hashCode()).isEqualTo(row.getString(0).hashCode());
        assertThat(mapRow.getString(1).hashCode()).isEqualTo(row.getString(1).hashCode());

        // test if the copy method produce a row with the same contents
        assertThat(mapRow.copy()).isEqualTo(row.copy());
        assertThat(((BinaryString) mapRow.getString(0)).copy())
                .isEqualTo(((BinaryString) row.getString(0)).copy());
        assertThat(((BinaryString) mapRow.getString(1)).copy())
                .isEqualTo(((BinaryString) row.getString(1)).copy());
    }

    @Test
    public void testSerializerPages() throws IOException {
        // Boundary tests
        BinaryRow row24 = DataFormatTestUtil.get24BytesBinaryRow();
        BinaryRow row160 = DataFormatTestUtil.get160BytesBinaryRow();
        testSerializerPagesInternal(row24, row160);
        testSerializerPagesInternal(row24, DataFormatTestUtil.getMultiSeg160BytesBinaryRow(row160));
    }

    private void testSerializerPagesInternal(BinaryRow row24, BinaryRow row160) throws IOException {
        BinaryRowSerializer serializer = new BinaryRowSerializer(2);

        // 1. test middle row with just on the edge1
        {
            MemorySegment[] segments = new MemorySegment[4];
            for (int i = 0; i < segments.length; i++) {
                segments[i] = MemorySegment.wrap(new byte[64]);
            }
            RandomAccessOutputView out = new RandomAccessOutputView(segments, segments[0].size());
            serializer.serializeToPages(row24, out);
            serializer.serializeToPages(row160, out);
            serializer.serializeToPages(row24, out);

            RandomAccessInputView in =
                    new RandomAccessInputView(
                            new ArrayList<>(Arrays.asList(segments)),
                            segments[0].size(),
                            out.getCurrentPositionInSegment());

            BinaryRow retRow = new BinaryRow(2);
            List<BinaryRow> rets = new ArrayList<>();
            while (true) {
                try {
                    retRow = serializer.mapFromPages(retRow, in);
                } catch (EOFException e) {
                    break;
                }
                rets.add(retRow.copy());
            }
            assertThat(rets.get(0)).isEqualTo(row24);
            assertThat(rets.get(1)).isEqualTo(row160);
            assertThat(rets.get(2)).isEqualTo(row24);
        }

        // 2. test middle row with just on the edge2
        {
            MemorySegment[] segments = new MemorySegment[7];
            for (int i = 0; i < segments.length; i++) {
                segments[i] = MemorySegment.wrap(new byte[64]);
            }
            RandomAccessOutputView out = new RandomAccessOutputView(segments, segments[0].size());
            serializer.serializeToPages(row24, out);
            serializer.serializeToPages(row160, out);
            serializer.serializeToPages(row160, out);

            RandomAccessInputView in =
                    new RandomAccessInputView(
                            new ArrayList<>(Arrays.asList(segments)),
                            segments[0].size(),
                            out.getCurrentPositionInSegment());

            BinaryRow retRow = new BinaryRow(2);
            List<BinaryRow> rets = new ArrayList<>();
            while (true) {
                try {
                    retRow = serializer.mapFromPages(retRow, in);
                } catch (EOFException e) {
                    break;
                }
                rets.add(retRow.copy());
            }
            assertThat(rets.get(0)).isEqualTo(row24);
            assertThat(rets.get(1)).isEqualTo(row160);
            assertThat(rets.get(2)).isEqualTo(row160);
        }

        // 3. test last row with just on the edge
        {
            MemorySegment[] segments = new MemorySegment[3];
            for (int i = 0; i < segments.length; i++) {
                segments[i] = MemorySegment.wrap(new byte[64]);
            }
            RandomAccessOutputView out = new RandomAccessOutputView(segments, segments[0].size());
            serializer.serializeToPages(row24, out);
            serializer.serializeToPages(row160, out);

            RandomAccessInputView in =
                    new RandomAccessInputView(
                            new ArrayList<>(Arrays.asList(segments)),
                            segments[0].size(),
                            out.getCurrentPositionInSegment());

            BinaryRow retRow = new BinaryRow(2);
            List<BinaryRow> rets = new ArrayList<>();
            while (true) {
                try {
                    retRow = serializer.mapFromPages(retRow, in);
                } catch (EOFException e) {
                    break;
                }
                rets.add(retRow.copy());
            }
            assertThat(rets.get(0)).isEqualTo(row24);
            assertThat(rets.get(1)).isEqualTo(row160);
        }
    }

    @Test
    public void testTimestampData() {
        // 1. compact
        {
            final int precision = 3;
            BinaryRow row = new BinaryRow(2);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.writeTimestamp(0, Timestamp.fromEpochMillis(123L), precision);
            writer.setNullAt(1);
            writer.complete();

            assertThat(row.getTimestamp(0, 3).toString()).isEqualTo("1970-01-01T00:00:00.123");
            assertThat(row.isNullAt(1)).isTrue();
            row.setTimestamp(0, Timestamp.fromEpochMillis(-123L), precision);
            assertThat(row.getTimestamp(0, 3).toString()).isEqualTo("1969-12-31T23:59:59.877");
        }

        // 2. not compact
        {
            final int precision = 9;
            Timestamp timestamp1 =
                    Timestamp.fromLocalDateTime(LocalDateTime.of(1969, 1, 1, 0, 0, 0, 123456789));
            Timestamp timestamp2 =
                    Timestamp.fromSQLTimestamp(
                            java.sql.Timestamp.valueOf("1970-01-01 00:00:00.123456789"));
            BinaryRow row = new BinaryRow(2);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.writeTimestamp(0, timestamp1, precision);
            writer.writeTimestamp(1, null, precision);
            writer.complete();

            // the size of row should be 8 + (8 + 8) * 2
            // (8 bytes nullBits, 8 bytes fixed-length part and 8 bytes variable-length part for
            // each timestamp(9))
            assertThat(row.getSizeInBytes()).isEqualTo(40);

            assertThat(row.getTimestamp(0, precision).toString())
                    .isEqualTo("1969-01-01T00:00:00.123456789");
            assertThat(row.isNullAt(1)).isTrue();
            row.setTimestamp(0, timestamp2, precision);
            assertThat(row.getTimestamp(0, precision).toString())
                    .isEqualTo("1970-01-01T00:00:00.123456789");
        }
    }

    @Test
    public void testNestedRowWithBinaryRowEquals() {
        BinaryRow nestedBinaryRow = new BinaryRow(2);
        {
            BinaryRowWriter writer = new BinaryRowWriter(nestedBinaryRow);
            writer.writeInt(0, 42);
            DataType innerType =
                    DataTypes.ROW(
                            DataTypes.FIELD(0, "f0", DataTypes.STRING()),
                            DataTypes.FIELD(1, "f1", DataTypes.DOUBLE()));
            InternalRowSerializer innerSerializer =
                    (InternalRowSerializer) (Serializer<?>) InternalSerializers.create(innerType);
            writer.writeRow(
                    1, GenericRow.of(BinaryString.fromString("Test"), 12.345), innerSerializer);
            writer.complete();
        }

        BinaryRow innerBinaryRow = new BinaryRow(2);
        {
            BinaryRowWriter writer = new BinaryRowWriter(innerBinaryRow);
            writer.writeString(0, BinaryString.fromString("Test"));
            writer.writeDouble(1, 12.345);
            writer.complete();
        }

        assertThat(nestedBinaryRow.getRow(1, 2)).isEqualTo(innerBinaryRow);
        assertThat(innerBinaryRow).isEqualTo(nestedBinaryRow.getRow(1, 2));
    }
}
