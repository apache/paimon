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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * An implementation of {@link InternalRow} which is backed by two concatenated {@link InternalRow}.
 *
 * <p>This implementation is mutable to allow for performant changes in hot code paths.
 *
 * @since 0.4.0
 */
@Public
public class JoinedRow implements InternalRow {

    private RowKind rowKind = RowKind.INSERT;
    private InternalRow row1;
    private InternalRow row2;

    /**
     * Creates a new {@link JoinedRow} of kind {@link RowKind#INSERT}, but without backing rows.
     *
     * <p>Note that it must be ensured that the backing rows are set to non-{@code null} values
     * before accessing data from this {@link JoinedRow}.
     */
    public JoinedRow() {}

    /**
     * Creates a new {@link JoinedRow} of kind {@link RowKind#INSERT} backed by row1 and row2.
     *
     * <p>Note that it must be ensured that the backing rows are set to non-{@code null} values
     * before accessing data from this {@link JoinedRow}.
     *
     * @param row1 the first row
     * @param row2 the second row
     */
    public JoinedRow(@Nullable InternalRow row1, @Nullable InternalRow row2) {
        this(RowKind.INSERT, row1, row2);
    }

    /**
     * Creates a new {@link JoinedRow} of kind {@link RowKind#INSERT} backed by row1 and row2.
     *
     * <p>Note that it must be ensured that the backing rows are set to non-{@code null} values
     * before accessing data from this {@link JoinedRow}.
     *
     * @param row1 the first row
     * @param row2 the second row
     */
    public JoinedRow(RowKind rowKind, @Nullable InternalRow row1, @Nullable InternalRow row2) {
        this.rowKind = rowKind;
        this.row1 = row1;
        this.row2 = row2;
    }

    public static JoinedRow join(InternalRow row1, InternalRow row2) {
        return new JoinedRow(row1, row2);
    }

    /**
     * Replaces the {@link InternalRow} backing this {@link JoinedRow}.
     *
     * <p>This method replaces the backing rows in place and does not return a new object. This is
     * done for performance reasons.
     */
    public JoinedRow replace(InternalRow row1, InternalRow row2) {
        this.row1 = row1;
        this.row2 = row2;
        return this;
    }

    public InternalRow row1() {
        return row1;
    }

    public InternalRow row2() {
        return row2;
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public int getFieldCount() {
        return row1.getFieldCount() + row2.getFieldCount();
    }

    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        this.rowKind = kind;
    }

    @Override
    public boolean isNullAt(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.isNullAt(pos);
        } else {
            return row2.isNullAt(pos - row1.getFieldCount());
        }
    }

    @Override
    public boolean getBoolean(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getBoolean(pos);
        } else {
            return row2.getBoolean(pos - row1.getFieldCount());
        }
    }

    @Override
    public byte getByte(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getByte(pos);
        } else {
            return row2.getByte(pos - row1.getFieldCount());
        }
    }

    @Override
    public short getShort(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getShort(pos);
        } else {
            return row2.getShort(pos - row1.getFieldCount());
        }
    }

    @Override
    public int getInt(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getInt(pos);
        } else {
            return row2.getInt(pos - row1.getFieldCount());
        }
    }

    @Override
    public long getLong(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getLong(pos);
        } else {
            return row2.getLong(pos - row1.getFieldCount());
        }
    }

    @Override
    public float getFloat(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getFloat(pos);
        } else {
            return row2.getFloat(pos - row1.getFieldCount());
        }
    }

    @Override
    public double getDouble(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getDouble(pos);
        } else {
            return row2.getDouble(pos - row1.getFieldCount());
        }
    }

    @Override
    public BinaryString getString(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getString(pos);
        } else {
            return row2.getString(pos - row1.getFieldCount());
        }
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        if (pos < row1.getFieldCount()) {
            return row1.getDecimal(pos, precision, scale);
        } else {
            return row2.getDecimal(pos - row1.getFieldCount(), precision, scale);
        }
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        if (pos < row1.getFieldCount()) {
            return row1.getTimestamp(pos, precision);
        } else {
            return row2.getTimestamp(pos - row1.getFieldCount(), precision);
        }
    }

    @Override
    public byte[] getBinary(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getBinary(pos);
        } else {
            return row2.getBinary(pos - row1.getFieldCount());
        }
    }

    @Override
    public Variant getVariant(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getVariant(pos);
        } else {
            return row2.getVariant(pos - row1.getFieldCount());
        }
    }

    @Override
    public InternalArray getArray(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getArray(pos);
        } else {
            return row2.getArray(pos - row1.getFieldCount());
        }
    }

    @Override
    public InternalMap getMap(int pos) {
        if (pos < row1.getFieldCount()) {
            return row1.getMap(pos);
        } else {
            return row2.getMap(pos - row1.getFieldCount());
        }
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        if (pos < row1.getFieldCount()) {
            return row1.getRow(pos, numFields);
        } else {
            return row2.getRow(pos - row1.getFieldCount(), numFields);
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
        JoinedRow that = (JoinedRow) o;
        return Objects.equals(rowKind, that.rowKind)
                && Objects.equals(this.row1, that.row1)
                && Objects.equals(this.row2, that.row2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowKind, row1, row2);
    }

    @Override
    public String toString() {
        return rowKind.shortString() + "{" + "row1=" + row1 + ", row2=" + row2 + '}';
    }
}
