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

package org.apache.paimon.reader;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

/** The row which is made up by several rows. */
public class DataEvolutionRow implements InternalRow {

    private final InternalRow[] rows;
    private final int[] rowOffsets;
    private final int[] fieldOffsets;

    public DataEvolutionRow(int rowNumber, int[] rowOffsets, int[] fieldOffsets) {
        this.rows = new InternalRow[rowNumber];
        this.rowOffsets = rowOffsets;
        this.fieldOffsets = fieldOffsets;
    }

    public int rowNumber() {
        return rows.length;
    }

    public void setRow(int pos, InternalRow row) {
        if (pos >= rows.length) {
            throw new IndexOutOfBoundsException(
                    "Position " + pos + " is out of bounds for rows size " + rows.length);
        } else {
            rows[pos] = row;
        }
    }

    private InternalRow chooseRow(int pos) {
        return rows[(rowOffsets[pos])];
    }

    private int offsetInRow(int pos) {
        return fieldOffsets[pos];
    }

    @Override
    public int getFieldCount() {
        return fieldOffsets.length;
    }

    @Override
    public RowKind getRowKind() {
        return rows[0].getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        rows[0].setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        if (rowOffsets[pos] == -1) {
            return true;
        }
        return chooseRow(pos).isNullAt(offsetInRow(pos));
    }

    @Override
    public boolean getBoolean(int pos) {
        return chooseRow(pos).getBoolean(offsetInRow(pos));
    }

    @Override
    public byte getByte(int pos) {
        return chooseRow(pos).getByte(offsetInRow(pos));
    }

    @Override
    public short getShort(int pos) {
        return chooseRow(pos).getShort(offsetInRow(pos));
    }

    @Override
    public int getInt(int pos) {
        return chooseRow(pos).getInt(offsetInRow(pos));
    }

    @Override
    public long getLong(int pos) {
        return chooseRow(pos).getLong(offsetInRow(pos));
    }

    @Override
    public float getFloat(int pos) {
        return chooseRow(pos).getFloat(offsetInRow(pos));
    }

    @Override
    public double getDouble(int pos) {
        return chooseRow(pos).getDouble(offsetInRow(pos));
    }

    @Override
    public BinaryString getString(int pos) {
        return chooseRow(pos).getString(offsetInRow(pos));
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return chooseRow(pos).getDecimal(offsetInRow(pos), precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return chooseRow(pos).getTimestamp(offsetInRow(pos), precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return chooseRow(pos).getBinary(offsetInRow(pos));
    }

    @Override
    public Variant getVariant(int pos) {
        return chooseRow(pos).getVariant(offsetInRow(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        return chooseRow(pos).getArray(offsetInRow(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return chooseRow(pos).getMap(offsetInRow(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return chooseRow(pos).getRow(offsetInRow(pos), numFields);
    }
}
