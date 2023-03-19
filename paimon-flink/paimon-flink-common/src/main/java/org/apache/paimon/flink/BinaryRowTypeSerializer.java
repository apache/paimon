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

package org.apache.paimon.flink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.memory.MemorySegment;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/** A {@link TypeSerializer} to serialize {@link BinaryRow}. */
public final class BinaryRowTypeSerializer extends TypeSerializer<BinaryRow> {

    private static final long serialVersionUID = 1L;

    private final BinaryRowSerializer serializer;

    public BinaryRowTypeSerializer(int numFields) {
        this.serializer = new BinaryRowSerializer(numFields);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public BinaryRowTypeSerializer duplicate() {
        return new BinaryRowTypeSerializer(serializer.getArity());
    }

    @Override
    public BinaryRow createInstance() {
        return serializer.createInstance();
    }

    @Override
    public BinaryRow copy(BinaryRow from) {
        return serializer.copy(from);
    }

    @Override
    public BinaryRow copy(BinaryRow from, BinaryRow reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(BinaryRow record, DataOutputView target) throws IOException {
        target.writeInt(record.getSizeInBytes());
        target.write(record.toBytes());
    }

    @Override
    public BinaryRow deserialize(DataInputView source) throws IOException {
        return deserialize(createInstance(), source);
    }

    @Override
    public BinaryRow deserialize(BinaryRow reuse, DataInputView source) throws IOException {
        int len = source.readInt();
        byte[] bytes = new byte[len];
        source.readFully(bytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, len);
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
        BinaryRowTypeSerializer that = (BinaryRowTypeSerializer) o;
        return Objects.equals(serializer, that.serializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serializer);
    }

    @Override
    public TypeSerializerSnapshot<BinaryRow> snapshotConfiguration() {
        return new BinaryRowTypeSerializerSnapshot(serializer.getArity());
    }

    /**
     * {@link TypeSerializerSnapshot} for {@link BinaryRow}. It checks the compatibility of
     * numFields without checking type.
     */
    public static class BinaryRowTypeSerializerSnapshot
            implements TypeSerializerSnapshot<BinaryRow> {

        private int numFields;

        public BinaryRowTypeSerializerSnapshot() {}

        private BinaryRowTypeSerializerSnapshot(int numFields) {
            this.numFields = numFields;
        }

        @Override
        public int getCurrentVersion() {
            return 0;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeInt(numFields);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            this.numFields = in.readInt();
        }

        @Override
        public TypeSerializer<BinaryRow> restoreSerializer() {
            return new BinaryRowTypeSerializer(numFields);
        }

        @Override
        public TypeSerializerSchemaCompatibility<BinaryRow> resolveSchemaCompatibility(
                TypeSerializer<BinaryRow> newSerializer) {
            if (!(newSerializer instanceof BinaryRowTypeSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            BinaryRowTypeSerializer other = (BinaryRowTypeSerializer) newSerializer;

            if (numFields == other.serializer.getArity()) {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            } else {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
        }
    }
}
