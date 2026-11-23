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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnarArray;
import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.utils.InternalRowUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** A test for the {@link InternalArraySerializer}. */
class InternalArraySerializerTest extends SerializerTestBase<InternalArray> {

    @Override
    protected InternalArraySerializer createSerializer() {
        return new InternalArraySerializer(DataTypes.STRING());
    }

    @Override
    protected boolean deepEquals(InternalArray array1, InternalArray array2) {
        if (array1.size() != array2.size()) {
            return false;
        }
        for (int i = 0; i < array1.size(); i++) {
            if (!array1.isNullAt(i) || !array2.isNullAt(i)) {
                if (array1.isNullAt(i) || array2.isNullAt(i)) {
                    return false;
                } else {
                    if (!array1.getString(i).equals(array2.getString(i))) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    protected InternalArray[] getTestData() {
        return new InternalArray[] {
            new GenericArray(
                    new BinaryString[] {
                        BinaryString.fromString("11"), null, BinaryString.fromString("ke")
                    }),
            createArray("11", "haa"),
            copyNewOffset(createArray("11", "haa")),
            createArray("11", "haa", "ke"),
            createArray("11", "haa", "ke"),
            createArray("11", "lele", "haa", "ke"),
            createColumnarArray("11", "lele", "haa", "ke"),
            createCustomTypeArray("11", "lele", "haa", "ke"),
        };
    }

    @Override
    protected InternalArray[] getSerializableTestData() {
        InternalArray[] testData = getTestData();
        return Arrays.copyOfRange(testData, 0, testData.length - 1);
    }

    @Test
    void testCopyColumnarBlobArrayPreservesReader(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        FileIO fileIO = LocalFileIO.create();
        byte[] payload = "blob-payload".getBytes(StandardCharsets.UTF_8);
        Path path = new Path(tempDir.resolve("blob.data").toUri());
        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            out.write(payload);
        }

        byte[] descriptor = new BlobDescriptor(path.toString(), 0, payload.length).serialize();
        HeapBytesVector vector = new HeapBytesVector(2);
        vector.putByteArray(0, descriptor, 0, descriptor.length);
        vector.setNullAt(1);
        ColumnarArray array = new ColumnarArray(vector, 0, 2);
        array.setFileIO(fileIO);

        InternalArray copied = new InternalArraySerializer(DataTypes.BLOB()).copy(array);

        assertThat(copied).isInstanceOf(GenericArray.class);
        assertThat(copied.getBlob(0).toData()).isEqualTo(payload);
        assertThat(copied.isNullAt(1)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("intBackedElementTypes")
    void testCopyPrimitiveIntBackedArray(DataType elementType) throws Exception {
        int[] values =
                elementType instanceof TimeType
                        ? new int[] {0, 1, 86_399_999}
                        : new int[] {-1, 0, 20_000};
        int[] expected = values.clone();
        GenericArray array = new GenericArray(values);
        InternalArraySerializer serializer = new InternalArraySerializer(elementType);

        InternalArray copied = serializer.copy(array);
        values[0] = 123;
        serializer.copy(new GenericArray(new int[] {456}));
        assertThat(copied.toIntArray()).containsExactly(expected);
        assertThat(serializer.copy(new GenericArray(new int[0])).size()).isZero();
        assertThat(
                        serializer
                                .deserializeFromBytes(serializer.serializeToBytes(copied))
                                .toIntArray())
                .containsExactly(expected);
    }

    @ParameterizedTest
    @MethodSource("intBackedElementTypes")
    void testCopyArrayProducedByInternalRowUtils(DataType elementType) {
        InternalArray array =
                InternalRowUtils.copyArray(
                        new GenericArray(new Integer[] {0, 1, 2}), elementType.notNull());
        assertThat(array).isInstanceOf(GenericArray.class);
        assertThat(((GenericArray) array).isPrimitiveArray()).isTrue();
        assertThat(new InternalArraySerializer(elementType).copy(array).toIntArray())
                .containsExactly(0, 1, 2);
    }

    @ParameterizedTest
    @MethodSource("intBackedElementTypes")
    void testCopyRowWithPrimitiveIntBackedArray(DataType elementType) {
        int[] values = {0, 1, 2};
        InternalRow row = GenericRow.of(new GenericArray(values));
        row.setRowKind(RowKind.DELETE);
        InternalRowSerializer serializer = new InternalRowSerializer(new ArrayType(elementType));

        InternalRow copied = serializer.copy(row);
        InternalRow copiedWithReuse = serializer.copyRowData(row, new GenericRow(1));
        values[0] = 123;

        assertThat(copied.getArray(0).toIntArray()).containsExactly(0, 1, 2);
        assertThat(copiedWithReuse.getArray(0).toIntArray()).containsExactly(0, 1, 2);
        assertThat(copied.getRowKind()).isEqualTo(RowKind.DELETE);
        assertThat(copiedWithReuse.getRowKind()).isEqualTo(RowKind.DELETE);
    }

    private static Stream<DataType> intBackedElementTypes() {
        return Stream.of(
                DataTypes.INT(),
                DataTypes.INT().notNull(),
                DataTypes.DATE(),
                DataTypes.DATE().notNull(),
                DataTypes.TIME(),
                DataTypes.TIME().notNull());
    }

    static BinaryArray copyNewOffset(BinaryArray array) {
        BinaryArray newArray = new BinaryArray();
        byte[] bytes = array.toBytes();
        byte[] newBytes = new byte[bytes.length + 10];
        System.arraycopy(bytes, 0, newBytes, 10, bytes.length);
        newArray.pointTo(MemorySegment.wrap(newBytes), 10, bytes.length);
        return newArray;
    }

    static BinaryArray createArray(String... vs) {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, vs.length, 8);
        for (int i = 0; i < vs.length; i++) {
            writer.writeString(i, BinaryString.fromString(vs[i]));
        }
        writer.complete();
        return array;
    }

    private static ColumnarArray createColumnarArray(String... vs) {
        HeapBytesVector vector = new HeapBytesVector(vs.length);
        for (String v : vs) {
            vector.fill(v.getBytes(StandardCharsets.UTF_8));
        }
        return new ColumnarArray(vector, 0, vs.length);
    }

    static InternalArray createCustomTypeArray(String... vs) {
        BinaryArray array = createArray(vs);
        Object customArrayData =
                Proxy.newProxyInstance(
                        InternalArraySerializerTest.class.getClassLoader(),
                        new Class[] {InternalArray.class},
                        (proxy, method, args) -> method.invoke(array, args));
        return (InternalArray) customArrayData;
    }
}
