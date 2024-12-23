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

package org.apache.paimon;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

/** An implementation of {@link InternalRow} which provides a row the fixed partition value. */
public class PartitionSettedRow implements InternalRow {

    private final BinaryRow partition;
    private final PartitionInfo partitionInfo;

    protected InternalRow row;

    protected PartitionSettedRow(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
        this.partition = partitionInfo.getPartitionRow();
    }

    /**
     * Replaces the underlying {@link InternalRow} backing this {@link PartitionSettedRow}.
     *
     * <p>This method replaces the row data in place and does not return a new object. This is done
     * for performance reasons.
     */
    public PartitionSettedRow replaceRow(InternalRow row) {
        this.row = row;
        return this;
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public int getFieldCount() {
        return partitionInfo.size();
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
        return partitionInfo.inPartitionRow(pos)
                ? partition.isNullAt(partitionInfo.getRealIndex(pos))
                : row.isNullAt(partitionInfo.getRealIndex(pos));
    }

    @Override
    public boolean getBoolean(int pos) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getBoolean(partitionInfo.getRealIndex(pos))
                : row.getBoolean(partitionInfo.getRealIndex(pos));
    }

    @Override
    public byte getByte(int pos) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getByte(partitionInfo.getRealIndex(pos))
                : row.getByte(partitionInfo.getRealIndex(pos));
    }

    @Override
    public short getShort(int pos) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getShort(partitionInfo.getRealIndex(pos))
                : row.getShort(partitionInfo.getRealIndex(pos));
    }

    @Override
    public int getInt(int pos) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getInt(partitionInfo.getRealIndex(pos))
                : row.getInt(partitionInfo.getRealIndex(pos));
    }

    @Override
    public long getLong(int pos) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getLong(partitionInfo.getRealIndex(pos))
                : row.getLong(partitionInfo.getRealIndex(pos));
    }

    @Override
    public float getFloat(int pos) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getFloat(partitionInfo.getRealIndex(pos))
                : row.getFloat(partitionInfo.getRealIndex(pos));
    }

    @Override
    public double getDouble(int pos) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getDouble(partitionInfo.getRealIndex(pos))
                : row.getDouble(partitionInfo.getRealIndex(pos));
    }

    @Override
    public BinaryString getString(int pos) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getString(partitionInfo.getRealIndex(pos))
                : row.getString(partitionInfo.getRealIndex(pos));
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getDecimal(partitionInfo.getRealIndex(pos), precision, scale)
                : row.getDecimal(partitionInfo.getRealIndex(pos), precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getTimestamp(partitionInfo.getRealIndex(pos), precision)
                : row.getTimestamp(partitionInfo.getRealIndex(pos), precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getBinary(partitionInfo.getRealIndex(pos))
                : row.getBinary(partitionInfo.getRealIndex(pos));
    }

    @Override
    public Variant getVariant(int pos) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getVariant(partitionInfo.getRealIndex(pos))
                : row.getVariant(partitionInfo.getRealIndex(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getArray(partitionInfo.getRealIndex(pos))
                : row.getArray(partitionInfo.getRealIndex(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getMap(partitionInfo.getRealIndex(pos))
                : row.getMap(partitionInfo.getRealIndex(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return partitionInfo.inPartitionRow(pos)
                ? partition.getRow(partitionInfo.getRealIndex(pos), numFields)
                : row.getRow(partitionInfo.getRealIndex(pos), numFields);
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException("Projected row data cannot be compared");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Projected row data cannot be hashed");
    }

    public static PartitionSettedRow from(PartitionInfo partitionInfo) {
        return new PartitionSettedRow(partitionInfo);
    }
}
