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

package org.apache.paimon.casting;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * An implementation of {@link InternalRow} which provides a casted view of the underlying {@link
 * InternalRow}.
 *
 * <p>It reads data from underlying {@link InternalRow} according to source logical type and casts
 * it with specific {@link CastExecutor}.
 *
 * <p>Note: This class supports only top-level castings, not nested castings.
 */
public class CastedRow implements InternalRow {

    private final CastFieldGetter[] castMapping;

    private InternalRow row;

    protected CastedRow(CastFieldGetter[] castMapping) {
        this.castMapping = checkNotNull(castMapping);
    }

    /**
     * Replaces the underlying {@link InternalRow} backing this {@link CastedRow}.
     *
     * <p>This method replaces the row data in place and does not return a new object. This is done
     * for performance reasons.
     */
    public CastedRow replaceRow(InternalRow row) {
        this.row = row;
        return this;
    }

    @Override
    public int getFieldCount() {
        return row.getFieldCount();
    }

    @Override
    public RowKind getRowKind() {
        return row.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        row.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        return row.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public byte getByte(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public short getShort(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public int getInt(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public long getLong(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public float getFloat(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public double getDouble(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public BinaryString getString(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public byte[] getBinary(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public InternalArray getArray(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public InternalMap getMap(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * Create an empty {@link CastedRow} starting from a {@code casting} array.
     *
     * @see CastFieldGetter
     * @see CastedRow
     */
    public static CastedRow from(CastFieldGetter[] castMapping) {
        return new CastedRow(castMapping);
    }
}
