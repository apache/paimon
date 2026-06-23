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
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

/** The row which is made up by several rows. */
public class DataEvolutionRow implements InternalRow {

    private final InternalRow[] rows;
    private final int[] rowOffsets;
    private final int[] fieldOffsets;

    /**
     * Optional per-top-level-field plan to assemble a nested struct whose sub-fields are spread
     * across several source files (sub-field-level data evolution). {@code null} (or a {@code null}
     * entry) means the field is taken whole from a single source row (the common case).
     */
    private NestedField[] nested;

    private RowKind rowKind;

    public DataEvolutionRow(int rowNumber, int[] rowOffsets, int[] fieldOffsets) {
        this.rows = new InternalRow[rowNumber];
        this.rowOffsets = rowOffsets;
        this.fieldOffsets = fieldOffsets;
    }

    public void setNested(NestedField[] nested) {
        this.nested = nested;
    }

    public int rowNumber() {
        return rows.length;
    }

    public void setRow(int pos, InternalRow row) {
        if (pos >= rows.length) {
            throw new IndexOutOfBoundsException(
                    "Position " + pos + " is out of bounds for rows size " + rows.length);
        } else {
            if (rowKind == null) {
                this.rowKind = row.getRowKind();
            }
            rows[pos] = row;
        }
    }

    private void setRowsAllowNull(InternalRow[] newRows) {
        for (int i = 0; i < newRows.length; i++) {
            this.rows[i] = newRows[i];
            if (rowKind == null && newRows[i] != null) {
                this.rowKind = newRows[i].getRowKind();
            }
        }
        if (rowKind == null) {
            // a composed struct whose every source partial is null still needs a defined kind so
            // getRowKind() never returns null; the kind of an assembled struct value is not
            // meaningful, so default to INSERT
            this.rowKind = RowKind.INSERT;
        }
    }

    public void setRows(InternalRow[] rows) {
        if (rows.length != this.rows.length) {
            throw new IllegalArgumentException(
                    "The length of input rows "
                            + rows.length
                            + " is not equal to the expected length "
                            + this.rows.length);
        }
        for (int i = 0; i < rows.length; i++) {
            setRow(i, rows[i]);
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
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        this.rowKind = kind;
    }

    @Override
    public boolean isNullAt(int pos) {
        if (nested != null && nested[pos] != null) {
            // a composed struct is null only when none of its source files provide it
            NestedField nf = nested[pos];
            for (int k = 0; k < nf.numPartials; k++) {
                InternalRow src = rows[nf.partialReader[k]];
                if (src != null && !src.isNullAt(nf.partialOffset[k])) {
                    return false;
                }
            }
            return true;
        }
        if (rowOffsets[pos] < 0) {
            return true;
        }
        InternalRow row = chooseRow(pos);
        return row == null || row.isNullAt(offsetInRow(pos));
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
    public Blob getBlob(int pos) {
        return chooseRow(pos).getBlob(offsetInRow(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        return chooseRow(pos).getArray(offsetInRow(pos));
    }

    @Override
    public InternalVector getVector(int pos) {
        return chooseRow(pos).getVector(offsetInRow(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return chooseRow(pos).getMap(offsetInRow(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        if (nested != null && nested[pos] != null) {
            NestedField nf = nested[pos];
            InternalRow[] partials = new InternalRow[nf.numPartials];
            for (int k = 0; k < nf.numPartials; k++) {
                InternalRow src = rows[nf.partialReader[k]];
                partials[k] =
                        (src == null || src.isNullAt(nf.partialOffset[k]))
                                ? null
                                : src.getRow(nf.partialOffset[k], nf.partialSize[k]);
            }
            DataEvolutionRow composed =
                    new DataEvolutionRow(nf.numPartials, nf.subRowOffsets, nf.subFieldOffsets);
            composed.setRowsAllowNull(partials);
            return composed;
        }
        return chooseRow(pos).getRow(offsetInRow(pos), numFields);
    }

    /**
     * Plan to assemble one nested struct from sub-fields spread across several source files. A
     * "partial" is the projection of the struct read from a single source file; each output
     * sub-field is sourced from one partial via {@code subRowOffsets}/{@code subFieldOffsets}.
     */
    public static class NestedField {

        final int numPartials;
        // per partial struct: the source reader, the struct's position in that reader's row, and
        // the
        // number of fields of the struct as read from that file
        final int[] partialReader;
        final int[] partialOffset;
        final int[] partialSize;
        // per output sub-field: which partial it comes from, and its offset within that partial
        final int[] subRowOffsets;
        final int[] subFieldOffsets;

        public NestedField(
                int[] partialReader,
                int[] partialOffset,
                int[] partialSize,
                int[] subRowOffsets,
                int[] subFieldOffsets) {
            this.numPartials = partialReader.length;
            this.partialReader = partialReader;
            this.partialOffset = partialOffset;
            this.partialSize = partialSize;
            this.subRowOffsets = subRowOffsets;
            this.subFieldOffsets = subFieldOffsets;
        }
    }
}
