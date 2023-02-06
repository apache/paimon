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
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.utils.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;

/** Serializer for {@link InternalRow}. */
@Internal
public class RowDataSerializer extends AbstractRowDataSerializer<InternalRow> {
    private static final long serialVersionUID = 1L;

    private final BinaryRowSerializer binarySerializer;
    private final DataType[] types;
    private final TypeSerializer[] fieldSerializers;
    private final InternalRow.FieldGetter[] fieldGetters;

    private transient BinaryRow reuseRow;
    private transient BinaryRowWriter reuseWriter;

    public RowDataSerializer(RowType rowType) {
        this(
                rowType.getFieldTypes().toArray(new DataType[0]),
                rowType.getFieldTypes().stream()
                        .map(InternalSerializers::create)
                        .toArray(TypeSerializer[]::new));
    }

    public RowDataSerializer(DataType... types) {
        this(
                types,
                Arrays.stream(types)
                        .map(InternalSerializers::create)
                        .toArray(TypeSerializer[]::new));
    }

    public RowDataSerializer(DataType[] types, TypeSerializer<?>[] fieldSerializers) {
        this.types = types;
        this.fieldSerializers = fieldSerializers;
        this.binarySerializer = new BinaryRowSerializer(types.length);
        this.fieldGetters =
                IntStream.range(0, types.length)
                        .mapToObj(i -> InternalRow.createFieldGetter(types[i], i))
                        .toArray(InternalRow.FieldGetter[]::new);
    }

    @Override
    public TypeSerializer<InternalRow> duplicate() {
        TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];
        for (int i = 0; i < fieldSerializers.length; i++) {
            duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
        }
        return new RowDataSerializer(types, duplicateFieldSerializers);
    }

    @Override
    public InternalRow createInstance() {
        // default use binary row to deserializer
        return new BinaryRow(types.length);
    }

    @Override
    public void serialize(InternalRow row, DataOutputView target) throws IOException {
        binarySerializer.serialize(toBinaryRow(row), target);
    }

    @Override
    public InternalRow deserialize(DataInputView source) throws IOException {
        return binarySerializer.deserialize(source);
    }

    @Override
    public InternalRow deserialize(InternalRow reuse, DataInputView source) throws IOException {
        if (reuse instanceof BinaryRow) {
            return binarySerializer.deserialize((BinaryRow) reuse, source);
        } else {
            return binarySerializer.deserialize(source);
        }
    }

    @Override
    public InternalRow copy(InternalRow from) {
        if (from.getFieldCount() != types.length) {
            throw new IllegalArgumentException(
                    "Row arity: "
                            + from.getFieldCount()
                            + ", but serializer arity: "
                            + types.length);
        }
        if (from instanceof BinaryRow) {
            return ((BinaryRow) from).copy();
        } else if (from instanceof NestedRow) {
            return ((NestedRow) from).copy();
        } else {
            return copyRowData(from, new GenericRow(from.getFieldCount()));
        }
    }

    @Override
    public InternalRow copy(InternalRow from, InternalRow reuse) {
        if (from.getFieldCount() != types.length || reuse.getFieldCount() != types.length) {
            throw new IllegalArgumentException(
                    "Row arity: "
                            + from.getFieldCount()
                            + ", reuse Row arity: "
                            + reuse.getFieldCount()
                            + ", but serializer arity: "
                            + types.length);
        }
        if (from instanceof BinaryRow) {
            return reuse instanceof BinaryRow
                    ? ((BinaryRow) from).copy((BinaryRow) reuse)
                    : ((BinaryRow) from).copy();
        } else if (from instanceof NestedRow) {
            return reuse instanceof NestedRow
                    ? ((NestedRow) from).copy(reuse)
                    : ((NestedRow) from).copy();
        } else {
            return copyRowData(from, reuse);
        }
    }

    @SuppressWarnings("unchecked")
    private InternalRow copyRowData(InternalRow from, InternalRow reuse) {
        GenericRow ret;
        if (reuse instanceof GenericRow) {
            ret = (GenericRow) reuse;
        } else {
            ret = new GenericRow(from.getFieldCount());
        }
        ret.setRowKind(from.getRowKind());
        for (int i = 0; i < from.getFieldCount(); i++) {
            if (!from.isNullAt(i)) {
                ret.setField(i, fieldSerializers[i].copy((fieldGetters[i].getFieldOrNull(from))));
            } else {
                ret.setField(i, null);
            }
        }
        return ret;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        binarySerializer.copy(source, target);
    }

    @Override
    public int getArity() {
        return types.length;
    }

    /** Convert {@link InternalRow} into {@link BinaryRow}. TODO modify it to code gen. */
    @Override
    public BinaryRow toBinaryRow(InternalRow row) {
        if (row instanceof BinaryRow) {
            return (BinaryRow) row;
        }
        if (reuseRow == null) {
            reuseRow = new BinaryRow(types.length);
            reuseWriter = new BinaryRowWriter(reuseRow);
        }
        reuseWriter.reset();
        reuseWriter.writeRowKind(row.getRowKind());
        for (int i = 0; i < types.length; i++) {
            if (row.isNullAt(i)) {
                reuseWriter.setNullAt(i);
            } else {
                BinaryWriter.write(
                        reuseWriter,
                        i,
                        fieldGetters[i].getFieldOrNull(row),
                        types[i],
                        fieldSerializers[i]);
            }
        }
        reuseWriter.complete();
        return reuseRow;
    }

    @Override
    public int serializeToPages(InternalRow row, AbstractPagedOutputView target)
            throws IOException {
        return binarySerializer.serializeToPages(toBinaryRow(row), target);
    }

    @Override
    public InternalRow deserializeFromPages(AbstractPagedInputView source) throws IOException {
        throw new UnsupportedOperationException("Not support!");
    }

    @Override
    public InternalRow deserializeFromPages(InternalRow reuse, AbstractPagedInputView source)
            throws IOException {
        throw new UnsupportedOperationException("Not support!");
    }

    @Override
    public InternalRow mapFromPages(InternalRow reuse, AbstractPagedInputView source)
            throws IOException {
        if (reuse instanceof BinaryRow) {
            return binarySerializer.mapFromPages((BinaryRow) reuse, source);
        } else {
            throw new UnsupportedOperationException("Not support!");
        }
    }

    @Override
    public void skipRecordFromPages(AbstractPagedInputView source) throws IOException {
        binarySerializer.skipRecordFromPages(source);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RowDataSerializer) {
            RowDataSerializer other = (RowDataSerializer) obj;
            return Arrays.equals(fieldSerializers, other.fieldSerializers);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fieldSerializers);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public TypeSerializerSnapshot<InternalRow> snapshotConfiguration() {
        return new RowDataSerializerSnapshot(types, fieldSerializers);
    }

    /** {@link TypeSerializerSnapshot} for {@link BinaryRowSerializer}. */
    public static final class RowDataSerializerSnapshot
            implements TypeSerializerSnapshot<InternalRow> {
        private static final int CURRENT_VERSION = 3;

        private DataType[] previousTypes;
        private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

        @SuppressWarnings("unused")
        public RowDataSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        RowDataSerializerSnapshot(DataType[] types, TypeSerializer[] serializers) {
            this.previousTypes = types;
            this.nestedSerializersSnapshotDelegate =
                    new NestedSerializersSnapshotDelegate(serializers);
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeInt(previousTypes.length);
            DataOutputViewStream stream = new DataOutputViewStream(out);
            for (DataType previousType : previousTypes) {
                InstantiationUtil.serializeObject(stream, previousType);
            }
            nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(out);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            int length = in.readInt();
            DataInputViewStream stream = new DataInputViewStream(in);
            previousTypes = new DataType[length];
            for (int i = 0; i < length; i++) {
                try {
                    previousTypes[i] =
                            InstantiationUtil.deserializeObject(stream, userCodeClassLoader);
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
            }
            this.nestedSerializersSnapshotDelegate =
                    NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
                            in, userCodeClassLoader);
        }

        @Override
        public RowDataSerializer restoreSerializer() {
            return new RowDataSerializer(
                    previousTypes,
                    nestedSerializersSnapshotDelegate.getRestoredNestedSerializers());
        }

        @Override
        public TypeSerializerSchemaCompatibility<InternalRow> resolveSchemaCompatibility(
                TypeSerializer<InternalRow> newSerializer) {
            if (!(newSerializer instanceof RowDataSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            RowDataSerializer newRowSerializer = (RowDataSerializer) newSerializer;
            if (!Arrays.equals(previousTypes, newRowSerializer.types)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            CompositeTypeSerializerUtil.IntermediateCompatibilityResult<InternalRow>
                    intermediateResult =
                            CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                                    newRowSerializer.fieldSerializers,
                                    nestedSerializersSnapshotDelegate
                                            .getNestedSerializerSnapshots());

            if (intermediateResult.isCompatibleWithReconfiguredSerializer()) {
                RowDataSerializer reconfiguredCompositeSerializer = restoreSerializer();
                return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                        reconfiguredCompositeSerializer);
            }

            return intermediateResult.getFinalResult();
        }
    }
}
