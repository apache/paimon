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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarBinaryType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An {@link InternalRow} wrapper that converts BLOB fields to their BlobDescriptor serialized
 * bytes.
 *
 * <p>When the lookup cache stores blob descriptors instead of full blob data, this wrapper
 * intercepts access to BLOB fields and returns the serialized BlobDescriptor bytes via {@link
 * #getBinary(int)}. This allows the lookup cache to store only lightweight references (~100-150
 * bytes) instead of the full blob content (which can be megabytes per record).
 *
 * <p>Non-blob fields are delegated to the wrapped row unchanged.
 */
public class BlobAsDescriptorRow implements InternalRow {

    private final InternalRow wrapped;
    private final Set<Integer> blobFieldPositions;

    public BlobAsDescriptorRow(InternalRow wrapped, Set<Integer> blobFieldPositions) {
        this.wrapped = wrapped;
        this.blobFieldPositions = blobFieldPositions;
    }

    /**
     * Returns the set of field positions that are BLOB type in the given row type.
     *
     * @param rowType the row type to inspect
     * @return set of positions with BLOB type fields, empty if none
     */
    public static Set<Integer> blobFieldPositions(RowType rowType) {
        Set<Integer> positions = new HashSet<>();
        List<DataType> fieldTypes = rowType.getFieldTypes();
        for (int i = 0; i < fieldTypes.size(); i++) {
            if (fieldTypes.get(i).getTypeRoot() == DataTypeRoot.BLOB) {
                positions.add(i);
            }
        }
        return positions;
    }

    /**
     * Creates a new RowType where BLOB fields are replaced with VARBINARY. This is used to create
     * the correct serializer that uses BinarySerializer instead of BlobSerializer for cached
     * values.
     *
     * @param rowType the original row type
     * @param blobPositions the positions of BLOB fields
     * @return a new RowType with BLOB fields replaced by VARBINARY
     */
    public static RowType replaceBlobWithVarBinary(RowType rowType, Set<Integer> blobPositions) {
        if (blobPositions.isEmpty()) {
            return rowType;
        }
        List<DataType> newTypes = new ArrayList<>(rowType.getFieldTypes());
        for (int pos : blobPositions) {
            newTypes.set(pos, new VarBinaryType(VarBinaryType.MAX_LENGTH));
        }
        RowType.Builder builder = RowType.builder();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            builder.field(rowType.getFields().get(i).name(), newTypes.get(i));
        }
        return builder.build();
    }

    @Override
    public int getFieldCount() {
        return wrapped.getFieldCount();
    }

    @Override
    public RowKind getRowKind() {
        return wrapped.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        wrapped.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        return wrapped.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return wrapped.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return wrapped.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return wrapped.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return wrapped.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return wrapped.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return wrapped.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return wrapped.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return wrapped.getString(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return wrapped.getDecimal(pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return wrapped.getTimestamp(pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        if (blobFieldPositions.contains(pos)) {
            // Convert blob to descriptor bytes for caching
            Blob blob = wrapped.getBlob(pos);
            if (blob == null) {
                return null;
            }
            return blob.toDescriptor().serialize();
        }
        return wrapped.getBinary(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return wrapped.getArray(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return wrapped.getMap(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return wrapped.getRow(pos, numFields);
    }

    @Override
    public Blob getBlob(int pos) {
        return wrapped.getBlob(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        return wrapped.getVariant(pos);
    }

    @Override
    public InternalVector getVector(int pos) {
        return wrapped.getVector(pos);
    }
}
