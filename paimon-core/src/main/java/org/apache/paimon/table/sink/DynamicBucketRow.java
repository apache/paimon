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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

import java.io.Serializable;

/** An {@link InternalRow} wraps another {@link InternalRow} with bucket. */
public class DynamicBucketRow implements InternalRow, Serializable {

    private final InternalRow row;
    private final int bucket;

    public DynamicBucketRow(InternalRow row, int bucket) {
        this.row = row;
        this.bucket = bucket;
    }

    public int bucket() {
        return bucket;
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
        return row.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return row.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return row.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return row.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return row.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return row.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return row.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return row.getString(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return row.getDecimal(pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return row.getTimestamp(pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBinary(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return row.getArray(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return row.getMap(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return row.getRow(pos, numFields);
    }
}
