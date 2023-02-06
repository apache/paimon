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

package org.apache.flink.table.store.data;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.store.memory.MemorySegment;
import org.apache.flink.table.store.memory.MemorySegmentUtils;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.utils.InstantiationUtil;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

/** Serializer for {@link InternalArray}. */
@Internal
public class ArrayDataSerializer extends TypeSerializer<InternalArray> {
    private static final long serialVersionUID = 1L;

    private final DataType eleType;
    private final TypeSerializer<Object> eleSer;
    private final InternalArray.ElementGetter elementGetter;

    private transient BinaryArray reuseArray;
    private transient BinaryArrayWriter reuseWriter;

    public ArrayDataSerializer(DataType eleType) {
        this(eleType, InternalSerializers.create(eleType));
    }

    private ArrayDataSerializer(DataType eleType, TypeSerializer<Object> eleSer) {
        this.eleType = eleType;
        this.eleSer = eleSer;
        this.elementGetter = InternalArray.createElementGetter(eleType);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<InternalArray> duplicate() {
        return new ArrayDataSerializer(eleType, eleSer.duplicate());
    }

    @Override
    public InternalArray createInstance() {
        return new BinaryArray();
    }

    @Override
    public InternalArray copy(InternalArray from) {
        if (from instanceof GenericArray) {
            return copyGenericArray((GenericArray) from);
        } else if (from instanceof BinaryArray) {
            return ((BinaryArray) from).copy();
        } else {
            return toBinaryArray(from);
        }
    }

    @Override
    public InternalArray copy(InternalArray from, InternalArray reuse) {
        return copy(from);
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
    public int getLength() {
        return -1;
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

    @Override
    public InternalArray deserialize(InternalArray reuse, DataInputView source) throws IOException {
        return deserializeReuse(
                reuse instanceof BinaryArray ? (BinaryArray) reuse : new BinaryArray(), source);
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
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        target.writeInt(length);
        target.write(source, length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ArrayDataSerializer that = (ArrayDataSerializer) o;

        return eleType.equals(that.eleType);
    }

    @Override
    public int hashCode() {
        return eleType.hashCode();
    }

    @VisibleForTesting
    public TypeSerializer getEleSer() {
        return eleSer;
    }

    @Override
    public TypeSerializerSnapshot<InternalArray> snapshotConfiguration() {
        return new ArrayDataSerializerSnapshot(eleType, eleSer);
    }

    /** {@link TypeSerializerSnapshot} for {@link ArrayDataSerializer}. */
    public static final class ArrayDataSerializerSnapshot
            implements TypeSerializerSnapshot<InternalArray> {
        private static final int CURRENT_VERSION = 3;

        private DataType previousType;
        private TypeSerializer previousEleSer;

        @SuppressWarnings("unused")
        public ArrayDataSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        ArrayDataSerializerSnapshot(DataType eleType, TypeSerializer eleSer) {
            this.previousType = eleType;
            this.previousEleSer = eleSer;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            DataOutputViewStream outStream = new DataOutputViewStream(out);
            InstantiationUtil.serializeObject(outStream, previousType);
            InstantiationUtil.serializeObject(outStream, previousEleSer);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            try {
                DataInputViewStream inStream = new DataInputViewStream(in);
                this.previousType =
                        InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
                this.previousEleSer =
                        InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

        @Override
        public TypeSerializer<InternalArray> restoreSerializer() {
            return new ArrayDataSerializer(previousType, previousEleSer);
        }

        @Override
        public TypeSerializerSchemaCompatibility<InternalArray> resolveSchemaCompatibility(
                TypeSerializer<InternalArray> newSerializer) {
            if (!(newSerializer instanceof ArrayDataSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            ArrayDataSerializer newArrayDataSerializer = (ArrayDataSerializer) newSerializer;
            if (!previousType.equals(newArrayDataSerializer.eleType)
                    || !previousEleSer.equals(newArrayDataSerializer.eleSer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            } else {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
        }
    }
}
