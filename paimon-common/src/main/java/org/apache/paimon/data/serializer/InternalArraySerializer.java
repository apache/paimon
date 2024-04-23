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
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.DataType;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

/** Serializer for {@link InternalArray}. */
public class InternalArraySerializer implements Serializer<InternalArray> {
    private static final long serialVersionUID = 1L;

    private final DataType eleType;
    private final Serializer<Object> eleSer;
    private final InternalArray.ElementGetter elementGetter;

    private transient BinaryArray reuseArray;
    private transient BinaryArrayWriter reuseWriter;

    public InternalArraySerializer(DataType eleType) {
        this(eleType, InternalSerializers.create(eleType));
    }

    private InternalArraySerializer(DataType eleType, Serializer<Object> eleSer) {
        this.eleType = eleType;
        this.eleSer = eleSer;
        this.elementGetter = InternalArray.createElementGetter(eleType);
    }

    @Override
    public InternalArraySerializer duplicate() {
        return new InternalArraySerializer(eleType, eleSer.duplicate());
    }

    @Override
    public InternalArray copy(InternalArray from) {
        if (from instanceof GenericArray) {
            return copyGenericArray((GenericArray) from);
        } else if (from instanceof BinaryArray) {
            return ((BinaryArray) from).copy();
        } else {
            return toBinaryArray(from).copy();
        }
    }

    private GenericArray copyGenericArray(GenericArray array) {
        if (array.isPrimitiveArray()) {
            switch (eleType.getTypeRoot()) {
                case BOOLEAN:
                    return new GenericArray(Arrays.copyOf(array.toBooleanArray(), array.size()));
                case TINYINT:
                    return new GenericArray(Arrays.copyOf(array.toByteArray(), array.size()));
                case SMALLINT:
                    return new GenericArray(Arrays.copyOf(array.toShortArray(), array.size()));
                case INTEGER:
                    return new GenericArray(Arrays.copyOf(array.toIntArray(), array.size()));
                case BIGINT:
                    return new GenericArray(Arrays.copyOf(array.toLongArray(), array.size()));
                case FLOAT:
                    return new GenericArray(Arrays.copyOf(array.toFloatArray(), array.size()));
                case DOUBLE:
                    return new GenericArray(Arrays.copyOf(array.toDoubleArray(), array.size()));
                default:
                    throw new RuntimeException("Unknown type: " + eleType);
            }
        } else {
            Object[] objectArray = array.toObjectArray();
            Object[] newArray =
                    (Object[]) Array.newInstance(InternalRow.getDataClass(eleType), array.size());
            for (int i = 0; i < array.size(); i++) {
                if (objectArray[i] != null) {
                    newArray[i] = eleSer.copy(objectArray[i]);
                }
            }
            return new GenericArray(newArray);
        }
    }

    @Override
    public void serialize(InternalArray record, DataOutputView target) throws IOException {
        BinaryArray binaryArray = toBinaryArray(record);
        target.writeInt(binaryArray.getSizeInBytes());
        MemorySegmentUtils.copyToView(
                binaryArray.getSegments(),
                binaryArray.getOffset(),
                binaryArray.getSizeInBytes(),
                target);
    }

    public BinaryArray toBinaryArray(InternalArray from) {
        if (from instanceof BinaryArray) {
            return (BinaryArray) from;
        }

        int numElements = from.size();
        if (reuseArray == null) {
            reuseArray = new BinaryArray();
        }
        if (reuseWriter == null || reuseWriter.getNumElements() != numElements) {
            reuseWriter =
                    new BinaryArrayWriter(
                            reuseArray,
                            numElements,
                            BinaryArray.calculateFixLengthPartSize(eleType));
        } else {
            reuseWriter.reset();
        }

        for (int i = 0; i < numElements; i++) {
            if (from.isNullAt(i)) {
                reuseWriter.setNullAt(i, eleType);
            } else {
                BinaryWriter.write(
                        reuseWriter, i, elementGetter.getElementOrNull(from, i), eleType, eleSer);
            }
        }
        reuseWriter.complete();

        return reuseArray;
    }

    @Override
    public InternalArray deserialize(DataInputView source) throws IOException {
        return deserializeReuse(new BinaryArray(), source);
    }

    private BinaryArray deserializeReuse(BinaryArray reuse, DataInputView source)
            throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, bytes.length);
        return reuse;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InternalArraySerializer that = (InternalArraySerializer) o;

        return eleType.equals(that.eleType);
    }

    @Override
    public int hashCode() {
        return eleType.hashCode();
    }
}
