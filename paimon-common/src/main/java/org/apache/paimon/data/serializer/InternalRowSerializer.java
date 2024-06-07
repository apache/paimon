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

import org.apache.paimon.data.AbstractPagedInputView;
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.data.BinaryWriter.ValueSetter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.data.NestedRow;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.Arrays;

/** Serializer for {@link InternalRow}. */
public class InternalRowSerializer extends AbstractRowDataSerializer<InternalRow> {

    private static final long serialVersionUID = 1L;

    private final BinaryRowSerializer binarySerializer;
    private final DataType[] types;
    private final Serializer[] fieldSerializers;
    private final FieldGetter[] fieldGetters;
    private final ValueSetter[] valueSetters;

    private transient BinaryRow reuseRow;
    private transient BinaryRowWriter reuseWriter;

    public InternalRowSerializer(RowType rowType) {
        this(
                rowType.getFieldTypes().toArray(new DataType[0]),
                rowType.getFieldTypes().stream()
                        .map(InternalSerializers::create)
                        .toArray(Serializer[]::new));
    }

    public InternalRowSerializer(DataType... types) {
        this(
                types,
                Arrays.stream(types).map(InternalSerializers::create).toArray(Serializer[]::new));
    }

    public InternalRowSerializer(DataType[] types, Serializer<?>[] fieldSerializers) {
        this.types = types;
        this.fieldSerializers = fieldSerializers;
        this.binarySerializer = new BinaryRowSerializer(types.length);
        this.fieldGetters = new FieldGetter[types.length];
        this.valueSetters = new ValueSetter[types.length];
        for (int i = 0; i < types.length; i++) {
            DataType type = types[i];
            fieldGetters[i] = InternalRow.createFieldGetter(type, i);
            valueSetters[i] = BinaryWriter.createValueSetter(type, fieldSerializers[i]);
        }
    }

    @Override
    public InternalRowSerializer duplicate() {
        Serializer<?>[] duplicateFieldSerializers = new Serializer[fieldSerializers.length];
        for (int i = 0; i < fieldSerializers.length; i++) {
            duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
        }
        return new InternalRowSerializer(types, duplicateFieldSerializers);
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

    @SuppressWarnings("unchecked")
    public InternalRow copyRowData(InternalRow from, InternalRow reuse) {
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
    public int getArity() {
        return types.length;
    }

    public DataType[] fieldTypes() {
        return types;
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
            Object field = fieldGetters[i].getFieldOrNull(row);
            if (field == null) {
                reuseWriter.setNullAt(i);
            } else {
                valueSetters[i].setValue(reuseWriter, i, field);
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
        if (obj instanceof InternalRowSerializer) {
            InternalRowSerializer other = (InternalRowSerializer) obj;
            return Arrays.equals(fieldSerializers, other.fieldSerializers);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fieldSerializers);
    }
}
