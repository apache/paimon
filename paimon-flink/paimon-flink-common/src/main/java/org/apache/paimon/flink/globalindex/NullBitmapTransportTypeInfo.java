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

package org.apache.paimon.flink.globalindex;

import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.utils.InternalTypeSerializer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/** Exchange-only encoding: absent bitmap payloads occupy no bytes beyond the original schema. */
final class NullBitmapTransportTypeInfo extends TypeInformation<RowData> {

    private static final long serialVersionUID = 1L;
    private final RowType originalType;

    NullBitmapTransportTypeInfo(RowType originalType) {
        this.originalType = originalType;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<RowData> getTypeClass() {
        return RowData.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    // Both entry points are needed across supported Flink versions.
    public TypeSerializer<RowData> createSerializer(ExecutionConfig config) {
        return new Serializer(originalType);
    }

    public TypeSerializer<RowData> createSerializer(SerializerConfig config) {
        return new Serializer(originalType);
    }

    @Override
    public boolean canEqual(Object other) {
        return other instanceof NullBitmapTransportTypeInfo;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof NullBitmapTransportTypeInfo
                && originalType.equals(((NullBitmapTransportTypeInfo) other).originalType);
    }

    @Override
    public int hashCode() {
        return originalType.hashCode();
    }

    @Override
    public String toString() {
        return "NullBitmapTransport<" + originalType + ">";
    }

    static final class Serializer extends InternalTypeSerializer<RowData> {

        private static final long serialVersionUID = 1L;
        private final RowType originalType;
        private final RowDataSerializer rawSerializer;
        private final RowDataSerializer bitmapSerializer;

        Serializer(RowType originalType) {
            this.originalType = originalType;
            org.apache.flink.table.types.logical.RowType rawType =
                    LogicalTypeConversion.toLogicalType(originalType);
            rawSerializer = new RowDataSerializer(rawType);
            bitmapSerializer =
                    new RowDataSerializer(
                            LogicalTypeConversion.toLogicalType(
                                    RowType.of(
                                            originalType.getTypeAt(0),
                                            originalType.getTypeAt(1),
                                            originalType.getTypeAt(2),
                                            DataTypes.BYTES())));
        }

        @Override
        public TypeSerializer<RowData> duplicate() {
            return new Serializer(originalType);
        }

        @Override
        public RowData createInstance() {
            return new GenericRowData(3);
        }

        @Override
        public RowData copy(RowData row) {
            return row.getArity() == 4 ? bitmapSerializer.copy(row) : rawSerializer.copy(row);
        }

        @Override
        public void serialize(RowData row, DataOutputView target) throws IOException {
            boolean bitmap = row.getArity() == 4;
            BinaryRowData binary =
                    bitmap ? bitmapSerializer.toBinaryRow(row) : rawSerializer.toBinaryRow(row);
            int length = binary.getSizeInBytes();
            // Binary rows have a positive length; its sign distinguishes the optional payload.
            target.writeInt(bitmap ? -length : length);
            BinarySegmentUtils.copyToView(binary.getSegments(), binary.getOffset(), length, target);
        }

        @Override
        public RowData deserialize(DataInputView source) throws IOException {
            int encodedLength = source.readInt();
            boolean bitmap = encodedLength < 0;
            int length = decodedLength(encodedLength);
            byte[] bytes = new byte[length];
            source.readFully(bytes);
            BinaryRowData binary = new BinaryRowData(bitmap ? 4 : 3);
            binary.pointTo(MemorySegmentFactory.wrap(bytes), 0, length);
            return binary;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            int length = source.readInt();
            int bytes = decodedLength(length);
            target.writeInt(length);
            target.write(source, bytes);
        }

        private static int decodedLength(int length) throws IOException {
            int minimum = BinaryRowData.calculateFixPartSizeInBytes(length < 0 ? 4 : 3);
            if (length == Integer.MIN_VALUE || Math.abs(length) < minimum) {
                throw new IOException("Invalid null bitmap transport length: " + length);
            }
            return Math.abs(length);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Serializer
                    && originalType.equals(((Serializer) other).originalType);
        }

        @Override
        public int hashCode() {
            return originalType.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<RowData> snapshotConfiguration() {
            return new Snapshot(originalType);
        }
    }

    /** Snapshot keeps the optional-payload encoding when Flink restores the exchange serializer. */
    public static final class Snapshot implements TypeSerializerSnapshot<RowData> {

        private RowType originalType;

        public Snapshot() {}

        private Snapshot(RowType originalType) {
            this.originalType = originalType;
        }

        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            byte[] bytes = InstantiationUtil.serializeObject(originalType);
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        @Override
        public void readSnapshot(int version, DataInputView in, ClassLoader classLoader)
                throws IOException {
            if (version != 1) {
                throw new IOException(
                        "Unsupported null bitmap serializer snapshot version: " + version);
            }
            int length = in.readInt();
            if (length <= 0) {
                throw new IOException("Invalid null bitmap serializer snapshot length: " + length);
            }
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            try {
                originalType = InstantiationUtil.deserializeObject(bytes, classLoader);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

        @Override
        public TypeSerializer<RowData> restoreSerializer() {
            return new Serializer(originalType);
        }

        // Flink 1.x compatibility entry point.
        public TypeSerializerSchemaCompatibility<RowData> resolveSchemaCompatibility(
                TypeSerializer<RowData> serializer) {
            return restoreSerializer().equals(serializer)
                    ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                    : TypeSerializerSchemaCompatibility.incompatible();
        }

        public TypeSerializerSchemaCompatibility<RowData> resolveSchemaCompatibility(
                TypeSerializerSnapshot<RowData> oldSnapshot) {
            return oldSnapshot instanceof Snapshot
                            && originalType.equals(((Snapshot) oldSnapshot).originalType)
                    ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                    : TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
