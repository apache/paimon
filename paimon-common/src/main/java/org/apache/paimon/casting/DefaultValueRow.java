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

/**
 * An implementation of {@link InternalRow} which provides a default value for the underlying {@link
 * InternalRow}.
 */
public class DefaultValueRow implements InternalRow {

    private InternalRow row;

    private final InternalRow defaultValueRow;

    private DefaultValueRow(InternalRow defaultValueRow) {
        this.defaultValueRow = defaultValueRow;
    }

    public DefaultValueRow replaceRow(InternalRow row) {
        this.row = row;
        return this;
    }

    public InternalRow defaultValueRow() {
        return defaultValueRow;
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
        return row.isNullAt(pos) && defaultValueRow.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getBoolean(pos);
        }
        return defaultValueRow.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getByte(pos);
        }
        return defaultValueRow.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getShort(pos);
        }
        return defaultValueRow.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getInt(pos);
        }
        return defaultValueRow.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getLong(pos);
        }
        return defaultValueRow.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getFloat(pos);
        }
        return defaultValueRow.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getDouble(pos);
        }
        return defaultValueRow.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getString(pos);
        }
        return defaultValueRow.getString(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        if (!row.isNullAt(pos)) {
            return row.getDecimal(pos, precision, scale);
        }
        return defaultValueRow.getDecimal(pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        if (!row.isNullAt(pos)) {
            return row.getTimestamp(pos, precision);
        }
        return defaultValueRow.getTimestamp(pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getBinary(pos);
        }
        return defaultValueRow.getBinary(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getArray(pos);
        }
        return defaultValueRow.getArray(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getMap(pos);
        }
        return defaultValueRow.getMap(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        if (!row.isNullAt(pos)) {
            return row.getRow(pos, numFields);
        }
        return defaultValueRow.getRow(pos, numFields);
    }

    public static DefaultValueRow from(InternalRow defaultValueRow) {
        return new DefaultValueRow(defaultValueRow);
    }
}
