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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.columnar.ColumnarArray;
import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.data.serializer.InternalMapSerializer.convertToJavaMap;
import static org.apache.paimon.types.DataTypes.BLOB;
import static org.apache.paimon.types.DataTypes.INT;
import static org.apache.paimon.types.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link InternalMapSerializer}. */
public class InternalMapSerializerTest extends SerializerTestBase<InternalMap> {

    @Override
    protected Serializer<InternalMap> createSerializer() {
        return new InternalMapSerializer(INT(), STRING());
    }

    @Override
    protected boolean deepEquals(InternalMap t1, InternalMap t2) {
        // Better is more proper to compare the maps after changing
        // them to Java maps
        // instead of binary maps. For example, consider the
        // following two maps:
        // {1: 'a', 2: 'b', 3: 'c'} and {3: 'c', 2: 'b', 1: 'a'}
        // These are actually the same maps, but their key / value
        // order will be
        // different when stored as binary maps, and the equalsTo
        // method of binary
        // map will return false.
        return convertToJavaMap(t1, INT(), STRING()).equals(convertToJavaMap(t2, INT(), STRING()));
    }

    @Override
    protected InternalMap[] getTestData() {
        Map<Object, Object> first = new HashMap<>();
        first.put(1, BinaryString.fromString(""));
        return new InternalMap[] {
            new GenericMap(first),
            BinaryMap.valueOf(
                    createArray(1, 2), InternalArraySerializerTest.createArray("11", "haa")),
            BinaryMap.valueOf(
                    createArray(1, 3, 4),
                    InternalArraySerializerTest.createArray("11", "haa", "ke")),
            BinaryMap.valueOf(
                    createArray(1, 4, 2),
                    InternalArraySerializerTest.createArray("11", "haa", "ke")),
            BinaryMap.valueOf(
                    createArray(1, 5, 6, 7),
                    InternalArraySerializerTest.createArray("11", "lele", "haa", "ke")),
            new CustomMapData(first)
        };
    }

    @Override
    protected InternalMap[] getSerializableTestData() {
        InternalMap[] testData = getTestData();
        return Arrays.copyOfRange(testData, 0, testData.length - 1);
    }

    @Test
    void testCopyColumnarBlobMapPreservesReader(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        FileIO fileIO = LocalFileIO.create();
        byte[] payload = "blob-payload".getBytes(StandardCharsets.UTF_8);
        Path path = new Path(tempDir.resolve("blob.data").toUri());
        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            out.write(payload);
        }

        byte[] descriptor = new BlobDescriptor(path.toString(), 0, payload.length).serialize();
        HeapBytesVector values = new HeapBytesVector(2);
        values.putByteArray(0, descriptor, 0, descriptor.length);
        values.setNullAt(1);
        ColumnarArray valueArray = new ColumnarArray(values, 0, 2);
        valueArray.setFileIO(fileIO);
        InternalMap map =
                new InternalMap() {
                    @Override
                    public int size() {
                        return 2;
                    }

                    @Override
                    public InternalArray keyArray() {
                        return new GenericArray(new Object[] {1, 2});
                    }

                    @Override
                    public InternalArray valueArray() {
                        return valueArray;
                    }
                };

        InternalMap copied = new InternalMapSerializer(INT(), BLOB()).copy(map);

        assertThat(copied).isInstanceOf(GenericMap.class);
        assertThat(copied.keyArray().getInt(0)).isEqualTo(1);
        assertThat(copied.valueArray().getBlob(0).toData()).isEqualTo(payload);
        assertThat(copied.valueArray().isNullAt(1)).isTrue();
    }

    private static BinaryArray createArray(int... vs) {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, vs.length, 4);
        for (int i = 0; i < vs.length; i++) {
            writer.writeInt(i, vs[i]);
        }
        writer.complete();
        return array;
    }

    /** A simple custom implementation for {@link InternalMap}. */
    public static class CustomMapData implements InternalMap {

        private final Map<?, ?> map;

        public CustomMapData(Map<?, ?> map) {
            this.map = map;
        }

        public Object get(Object key) {
            return map.get(key);
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public InternalArray keyArray() {
            Object[] keys = map.keySet().toArray();
            return new GenericArray(keys);
        }

        @Override
        public InternalArray valueArray() {
            Object[] values = map.values().toArray();
            return new GenericArray(values);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof CustomMapData)) {
                return false;
            }
            return map.equals(((CustomMapData) o).map);
        }

        @Override
        public int hashCode() {
            return Objects.hash(map);
        }
    }
}
