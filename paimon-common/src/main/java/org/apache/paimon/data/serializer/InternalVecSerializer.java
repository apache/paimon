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

import org.apache.paimon.data.ArrayBasedVec;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalVec;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.types.DataType;

import java.io.IOException;

/** Serializer for {@link InternalVec}. */
public class InternalVecSerializer implements Serializer<InternalVec> {
    private static final long serialVersionUID = 1L;

    private final DataType eleType;
    private final int length;
    private final InternalArraySerializer arraySer;
    private final Serializer<Object> eleSer;

    public InternalVecSerializer(DataType eleType, int length) {
        this(
                eleType,
                length,
                new InternalArraySerializer(eleType),
                InternalSerializers.create(eleType));
    }

    private InternalVecSerializer(
            DataType eleType,
            int len,
            InternalArraySerializer arraySer,
            Serializer<Object> eleSer) {
        this.eleType = eleType;
        this.length = len;
        this.arraySer = arraySer;
        this.eleSer = eleSer;
    }

    @Override
    public InternalVecSerializer duplicate() {
        return new InternalVecSerializer(eleType, length, arraySer.duplicate(), eleSer.duplicate());
    }

    @Override
    public InternalVec copy(InternalVec from) {
        if (from instanceof ArrayBasedVec) {
            return ArrayBasedVec.from(arraySer.copy(((ArrayBasedVec) from).getInnerArray()));
        } else {
            return ArrayBasedVec.from(arraySer.copy(from));
        }
    }

    @Override
    public void serialize(InternalVec record, DataOutputView target) throws IOException {
        if (record.size() != length) {
            throw new IOException("Invalid size to serialize: " + record.size());
        }
        if (record instanceof ArrayBasedVec) {
            arraySer.serialize(((ArrayBasedVec) record).getInnerArray(), target);
        } else {
            arraySer.serialize(record, target);
        }
    }

    @Override
    public InternalVec deserialize(DataInputView source) throws IOException {
        InternalArray array = arraySer.deserialize(source);
        if (array.size() != length) {
            throw new IOException("Invalid size to deserialize: " + array.size());
        }
        return ArrayBasedVec.from(array);
    }

    public BinaryArray toBinaryArray(InternalVec from) {
        if (from instanceof ArrayBasedVec) {
            return arraySer.toBinaryArray(((ArrayBasedVec) from).getInnerArray());
        } else {
            return arraySer.toBinaryArray(from);
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

        InternalVecSerializer that = (InternalVecSerializer) o;

        return eleType.equals(that.eleType) && length == that.length;
    }

    @Override
    public int hashCode() {
        return eleType.hashCode();
    }
}
