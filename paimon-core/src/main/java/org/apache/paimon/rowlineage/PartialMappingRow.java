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

package org.apache.paimon.rowlineage;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

/** Row with row lineage inject in. */
public class PartialMappingRow implements InternalRow {

    private InternalRow main;
    private InternalRow rowLineage;
    private final int[] mappings;

    public PartialMappingRow(int[] mappings) {
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
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.isNullAt(i);
            }
        }
        return main.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getBoolean(i);
            }
        }
        return main.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getByte(i);
            }
        }
        return main.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getShort(i);
            }
        }
        return main.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getInt(i);
            }
        }
        return main.getShort(pos);
    }

    @Override
    public long getLong(int pos) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getLong(i);
            }
        }
        return main.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getFloat(i);
            }
        }
        return main.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getByte(i);
            }
        }
        return main.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getString(i);
            }
        }
        return main.getString(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getDecimal(i, precision, scale);
            }
        }
        return main.getDecimal(pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getTimestamp(i, precision);
            }
        }
        return main.getTimestamp(pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getBinary(i);
            }
        }
        return main.getBinary(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getVariant(i);
            }
        }
        return main.getVariant(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getArray(i);
            }
        }
        return main.getArray(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getMap(i);
            }
        }
        return main.getMap(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        for (int i = 0; i < mappings.length; i++) {
            int mapPos = mappings[i];
            if (pos == mapPos) {
                return rowLineage.getRow(i, numFields);
            }
        }
        return main.getRow(pos, numFields);
    }

    public PartialMappingRow replace(InternalRow main, InternalRow rowLineage) {
        this.main = main;
        this.rowLineage = rowLineage;
        return this;
    }
}
