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

import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.DataType;

import java.io.IOException;

/** Serializer for {@link InternalVector}. */
public class InternalVectorSerializer implements Serializer<InternalVector> {
    private static final long serialVersionUID = 1L;

    private final DataType eleType;
    private final Serializer<Object> eleSer;
    private final int length;

    public InternalVectorSerializer(DataType eleType, int length) {
        this(eleType, InternalSerializers.create(eleType), length);
    }

    private InternalVectorSerializer(DataType eleType, Serializer<Object> eleSer, int length) {
        this.eleType = eleType;
        this.eleSer = eleSer;
        this.length = length;
    }

    @Override
    public InternalVectorSerializer duplicate() {
        return new InternalVectorSerializer(eleType, eleSer.duplicate(), length);
    }

    @Override
    public InternalVector copy(InternalVector from) {
        if (from instanceof BinaryVector) {
            return ((BinaryVector) from).copy();
        } else {
            return toBinaryVector(from).copy();
        }
    }

    @Override
    public void serialize(InternalVector record, DataOutputView target) throws IOException {
        if (record.size() != length) {
            throw new IOException("Invalid size to serialize: " + record.size());
        }
        BinaryVector binaryVector = toBinaryVector(record);
        target.writeInt(binaryVector.getSizeInBytes());
        MemorySegmentUtils.copyToView(
                binaryVector.getSegments(),
                binaryVector.getOffset(),
                binaryVector.getSizeInBytes(),
                target);
    }

    @Override
    public InternalVector deserialize(DataInputView source) throws IOException {
        int sizeInBytes = source.readInt();
        byte[] bytes = new byte[sizeInBytes];
        source.readFully(bytes);
        BinaryVector vector = new BinaryVector(length);
        vector.pointTo(MemorySegment.wrap(bytes), 0, bytes.length);
        return vector;
    }

    public BinaryVector toBinaryVector(InternalVector from) {
        if (from instanceof BinaryVector) {
            return (BinaryVector) from;
        } else {
            return BinaryVector.fromInternalArray(from, eleType);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InternalVectorSerializer that = (InternalVectorSerializer) o;

        return eleType.equals(that.eleType) && length == that.length;
    }

    @Override
    public int hashCode() {
        return eleType.hashCode();
    }
}
