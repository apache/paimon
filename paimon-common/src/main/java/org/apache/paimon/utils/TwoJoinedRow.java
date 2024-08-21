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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

/** An implementation of {@link InternalRow} which provides a row the fixed partition value. */
public class TwoJoinedRow implements InternalRow {

    // This map is a demonstration of how the index column mapping to partition row and outer row.
    // If the value of map[i] is minus, that means the row located in partition row, otherwise, the
    // row is located in outer row.
    // To avoid conflict of map[0] = 0, (because 0 is equals to -0), add every index by 1, that
    // means, if map[0] = -1, then the 0 row is located in partition row (Math.abs(-1) - 1), if
    // map[0] = 1, mean it is located in (Math.abs(1) - 1) in the outer row.
    private final int[] map;

    protected InternalRow mainRow;
    protected InternalRow secondRow;

    protected TwoJoinedRow(int[] map) {
        this.map = map;
    }

    /**
     * Replaces the underlying {@link InternalRow} backing this {@link TwoJoinedRow}.
     *
     * <p>This method replaces the row data in place and does not return a new object. This is done
     * for performance reasons.
     */
    public TwoJoinedRow replaceMainRow(InternalRow row) {
        this.mainRow = row;
        return this;
    }

    public TwoJoinedRow replaceSecondRow(InternalRow row) {
        this.secondRow = row;
        return this;
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public int getFieldCount() {
        return map.length - 1;
    }

    @Override
    public RowKind getRowKind() {
        return mainRow.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        mainRow.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        return inPartitionRow(pos)
                ? secondRow.isNullAt(getRealIndex(pos))
                : mainRow.isNullAt(getRealIndex(pos));
    }

    @Override
    public boolean getBoolean(int pos) {
        return inPartitionRow(pos)
                ? secondRow.getBoolean(getRealIndex(pos))
                : mainRow.getBoolean(getRealIndex(pos));
    }

    @Override
    public byte getByte(int pos) {
        return inPartitionRow(pos)
                ? secondRow.getByte(getRealIndex(pos))
                : mainRow.getByte(getRealIndex(pos));
    }

    @Override
    public short getShort(int pos) {
        return inPartitionRow(pos)
                ? secondRow.getShort(getRealIndex(pos))
                : mainRow.getShort(getRealIndex(pos));
    }

    @Override
    public int getInt(int pos) {
        return inPartitionRow(pos)
                ? secondRow.getInt(getRealIndex(pos))
                : mainRow.getInt(getRealIndex(pos));
    }

    @Override
    public long getLong(int pos) {
        return inPartitionRow(pos)
                ? secondRow.getLong(getRealIndex(pos))
                : mainRow.getLong(getRealIndex(pos));
    }

    @Override
    public float getFloat(int pos) {
        return inPartitionRow(pos)
                ? secondRow.getFloat(getRealIndex(pos))
                : mainRow.getFloat(getRealIndex(pos));
    }

    @Override
    public double getDouble(int pos) {
        return inPartitionRow(pos)
                ? secondRow.getDouble(getRealIndex(pos))
                : mainRow.getDouble(getRealIndex(pos));
    }

    @Override
    public BinaryString getString(int pos) {
        return inPartitionRow(pos)
                ? secondRow.getString(getRealIndex(pos))
                : mainRow.getString(getRealIndex(pos));
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return inPartitionRow(pos)
                ? secondRow.getDecimal(getRealIndex(pos), precision, scale)
                : mainRow.getDecimal(getRealIndex(pos), precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return inPartitionRow(pos)
                ? secondRow.getTimestamp(getRealIndex(pos), precision)
                : mainRow.getTimestamp(getRealIndex(pos), precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return inPartitionRow(pos)
                ? secondRow.getBinary(getRealIndex(pos))
                : mainRow.getBinary(getRealIndex(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        return inPartitionRow(pos)
                ? secondRow.getArray(getRealIndex(pos))
                : mainRow.getArray(getRealIndex(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return inPartitionRow(pos)
                ? secondRow.getMap(getRealIndex(pos))
                : mainRow.getMap(getRealIndex(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return inPartitionRow(pos)
                ? secondRow.getRow(getRealIndex(pos), numFields)
                : mainRow.getRow(getRealIndex(pos), numFields);
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException("Projected row data cannot be compared");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Projected row data cannot be hashed");
    }

    public static TwoJoinedRow from(int[] map) {
        return new TwoJoinedRow(map);
    }

    public boolean inPartitionRow(int i) {
        return map[i] < 0;
    }

    public int getRealIndex(int i) {
        return Math.abs(map[i]) - 1;
    }
}
