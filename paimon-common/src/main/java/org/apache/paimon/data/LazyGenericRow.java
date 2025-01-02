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

package org.apache.paimon.data;

import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

import java.util.function.Supplier;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A {@link InternalRow} which lazy init fields. */
public class LazyGenericRow implements InternalRow {

    /** The kind of change that a row describes in a changelog. */
    private RowKind kind;

    /** The array to store field value suppliers. */
    private final Supplier<Object>[] suppliers;

    /** The array to store the actual internal format values. */
    private final Object[] fields;

    /** The array to store field initialize status. */
    private final boolean[] initialized;

    public LazyGenericRow(Supplier<Object>[] suppliers) {
        this(RowKind.INSERT, suppliers);
    }

    public LazyGenericRow(RowKind rowKind, Supplier<Object>[] suppliers) {
        this.kind = rowKind;
        this.suppliers = suppliers;
        this.fields = new Object[suppliers.length];
        this.initialized = new boolean[suppliers.length];
    }

    /**
     * Returns the field value at the given position.
     *
     * <p>Note: The returned value is in internal data structure. See {@link InternalRow} for more
     * information about internal data structures.
     *
     * <p>The returned field value can be null for representing nullability.
     */
    public Object getField(int pos) {
        if (!initialized[pos]) {
            fields[pos] = suppliers[pos].get();
            initialized[pos] = true;
        }
        return fields[pos];
    }

    @Override
    public int getFieldCount() {
        return fields.length;
    }

    @Override
    public RowKind getRowKind() {
        return kind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        checkNotNull(kind);
        this.kind = kind;
    }

    @Override
    public boolean isNullAt(int pos) {
        return getField(pos) == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        return (boolean) getField(pos);
    }

    @Override
    public byte getByte(int pos) {
        return (byte) getField(pos);
    }

    @Override
    public short getShort(int pos) {
        return (short) getField(pos);
    }

    @Override
    public int getInt(int pos) {
        return (int) getField(pos);
    }

    @Override
    public long getLong(int pos) {
        return (long) getField(pos);
    }

    @Override
    public float getFloat(int pos) {
        return (float) getField(pos);
    }

    @Override
    public double getDouble(int pos) {
        return (double) getField(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return (BinaryString) getField(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return (Decimal) getField(pos);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return (Timestamp) getField(pos);
    }

    @Override
    public byte[] getBinary(int pos) {
        return (byte[]) getField(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        return (Variant) getField(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return (InternalArray) getField(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return (InternalMap) getField(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return (InternalRow) getField(pos);
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException(
                "This row do not support equals, please compare fields one by one!");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException(
                "This row do not support equals, please compare fields one by one!");
    }
}
