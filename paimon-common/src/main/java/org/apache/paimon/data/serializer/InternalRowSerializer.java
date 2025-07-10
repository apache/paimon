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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.NestedRow;
import org.apache.paimon.data.RowHelper;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.Arrays;

/** Serializer for {@link InternalRow}. */
public class InternalRowSerializer extends AbstractRowDataSerializer<InternalRow> {

    private static final long serialVersionUID = 1L;

    private final DataType[] types;
    private final BinaryRowSerializer binarySerializer;
    private final RowHelper rowHelper;

    public InternalRowSerializer(RowType rowType) {
        this(rowType.getFieldTypes().toArray(new DataType[0]));
    }

    public InternalRowSerializer(DataType... types) {
        this.types = types;
        this.binarySerializer = new BinaryRowSerializer(types.length);
        this.rowHelper = new RowHelper(Arrays.asList(types));
    }

    @Override
    public InternalRowSerializer duplicate() {
        return new InternalRowSerializer(types);
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
                Object field = rowHelper.fieldGetter(i).getFieldOrNull(from);
                ret.setField(i, rowHelper.serializer(i).copy(field));
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
        rowHelper.copyInto(row);
        return rowHelper.reuseRow();
    }

    @Override
    public InternalRow createReuseInstance() {
        return binarySerializer.createReuseInstance();
    }

    @Override
    public int serializeToPages(InternalRow row, AbstractPagedOutputView target)
            throws IOException {
        return binarySerializer.serializeToPages(toBinaryRow(row), target);
    }

    @Override
    public InternalRow deserializeFromPages(AbstractPagedInputView source) {
        throw new UnsupportedOperationException("Not support!");
    }

    @Override
    public InternalRow deserializeFromPages(InternalRow reuse, AbstractPagedInputView source) {
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
            return Arrays.equals(types, other.types);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(types);
    }
}
