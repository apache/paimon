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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobView;
import org.apache.paimon.data.BlobViewResolver;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

import java.util.Set;

/**
 * {@link InternalRow} wrapper that resolves {@link BlobView} when {@link #getBlob(int)} is called.
 */
class BlobViewResolvingRow implements InternalRow {

    private final InternalRow wrapped;
    private final Set<Integer> blobViewFields;
    private final BlobViewResolver resolver;

    BlobViewResolvingRow(
            InternalRow wrapped, Set<Integer> blobViewFields, BlobViewResolver resolver) {
        this.wrapped = wrapped;
        this.blobViewFields = blobViewFields;
        this.resolver = resolver;
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
        return wrapped.getBinary(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        return wrapped.getVariant(pos);
    }

    @Override
    public Blob getBlob(int pos) {
        Blob blob = wrapped.getBlob(pos);
        if (blobViewFields.contains(pos) && blob instanceof BlobView) {
            BlobView blobView = (BlobView) blob;
            if (!blobView.isResolved()) {
                resolver.resolve(blobView);
            }
        }
        return blob;
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return wrapped.getRow(pos, numFields);
    }

    @Override
    public InternalArray getArray(int pos) {
        return wrapped.getArray(pos);
    }

    @Override
    public InternalVector getVector(int pos) {
        return wrapped.getVector(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return wrapped.getMap(pos);
    }
}
