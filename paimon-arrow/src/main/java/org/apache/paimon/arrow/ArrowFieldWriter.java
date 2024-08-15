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

package org.apache.paimon.arrow;

import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.columnar.ColumnVector;

import org.apache.arrow.vector.FieldVector;

import javax.annotation.Nullable;

/** A reusable writer to convert a field into Arrow {@link FieldVector}. */
public abstract class ArrowFieldWriter {

    // reusable
    protected final FieldVector fieldVector;

    public ArrowFieldWriter(FieldVector fieldVector) {
        this.fieldVector = fieldVector;
    }

    /** Reset the state of the writer to write the next batch of fields. */
    public void reset() {
        fieldVector.reset();
    }

    /**
     * Write all data of a {@link ColumnVector}.
     *
     * @param columnVector Which holds the paimon data.
     * @param pickedInColumn Which rows is picked to write. Pick all if null. This is used to adapt
     *     deletion vector.
     * @param startIndex From where to start writing.
     * @param batchRows How many rows to write.
     */
    public void write(
            ColumnVector columnVector,
            @Nullable int[] pickedInColumn,
            int startIndex,
            int batchRows) {
        doWrite(columnVector, pickedInColumn, startIndex, batchRows);
        fieldVector.setValueCount(batchRows);
    }

    protected abstract void doWrite(
            ColumnVector columnVector,
            @Nullable int[] pickedInColumn,
            int startIndex,
            int batchRows);

    /** Get the value from the row at the given position and write to specified row index. */
    public void write(int rowIndex, DataGetters getters, int pos) {
        if (getters.isNullAt(pos)) {
            fieldVector.setNull(rowIndex);
        } else {
            doWrite(rowIndex, getters, pos);
        }
        fieldVector.setValueCount(fieldVector.getValueCount() + 1);
    }

    protected abstract void doWrite(int rowIndex, DataGetters getters, int pos);

    protected int getRowNumber(int startIndex, int currentIndex, @Nullable int[] pickedInColumn) {
        int row = currentIndex + startIndex;
        if (pickedInColumn != null) {
            row = pickedInColumn[row];
        }
        return row;
    }
}
