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
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.columnar.ColumnarArray;
import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.DataTypes;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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
