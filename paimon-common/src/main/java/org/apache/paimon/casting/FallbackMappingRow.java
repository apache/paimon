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
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

/** Row with fallback mapping row inject in. */
public class FallbackMappingRow implements InternalRow {

    private InternalRow main;
    private InternalRow fallbackRow;
    private final int[] mappings;

    public FallbackMappingRow(int[] mappings) {
        this.mappings = mappings;
    }

    @Override
    public int getFieldCount() {
        return main.getFieldCount();
    }

    @Override
    public RowKind getRowKind() {
        return main.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        main.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        if (mappings[pos] == -1) {
            return main.isNullAt(pos);
        }
        return main.isNullAt(pos) && fallbackRow.isNullAt(mappings[pos]);
    }

    @Override
    public boolean getBoolean(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getBoolean(mappings[pos]);
        }
        return main.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getByte(mappings[pos]);
        }
        return main.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getShort(mappings[pos]);
        }
        return main.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getInt(mappings[pos]);
        }
        return main.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getLong(mappings[pos]);
        }
        return main.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getFloat(mappings[pos]);
        }
        return main.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getDouble(mappings[pos]);
        }
        return main.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getString(mappings[pos]);
        }
        return main.getString(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getDecimal(mappings[pos], precision, scale);
        }
        return main.getDecimal(pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getTimestamp(mappings[pos], precision);
        }
        return main.getTimestamp(pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getBinary(mappings[pos]);
        }
        return main.getBinary(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getVariant(mappings[pos]);
        }
        return main.getVariant(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getArray(mappings[pos]);
        }
        return main.getArray(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getMap(mappings[pos]);
        }
        return main.getMap(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getRow(mappings[pos], numFields);
        }
        return main.getRow(pos, numFields);
    }

    public FallbackMappingRow replace(InternalRow main, InternalRow fallbackRow) {
        this.main = main;
        this.fallbackRow = fallbackRow;
        return this;
    }
}
