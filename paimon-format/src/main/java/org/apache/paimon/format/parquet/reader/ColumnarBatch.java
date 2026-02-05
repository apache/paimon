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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.columnar.ArrayColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.data.columnar.MapColumnVector;
import org.apache.paimon.data.columnar.RowColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.VectorizedRowIterator;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.LongIterator;
import org.apache.paimon.utils.UriReader;

import java.util.Arrays;

/** A batch of rows in columnar format. */
public class ColumnarBatch {
    protected final ColumnVector[] columns;

    protected final VectorizedColumnBatch vectorizedColumnBatch;
    protected final ColumnarRowIterator vectorizedRowIterator;

    public ColumnarBatch(Path filePath, ColumnVector[] columns, UriReader uriReader) {
        this.columns = columns;
        this.vectorizedColumnBatch = new VectorizedColumnBatch(columns);
        boolean containsNestedColumn =
                Arrays.stream(columns)
                        .anyMatch(
                                vector ->
                                        vector instanceof MapColumnVector
                                                || vector instanceof RowColumnVector
                                                || vector instanceof ArrayColumnVector);
        ColumnarRow row = new ColumnarRow(vectorizedColumnBatch, uriReader);
        this.vectorizedRowIterator =
                containsNestedColumn
                        ? new ColumnarRowIterator(filePath, row, null)
                        : new VectorizedRowIterator(filePath, row, null);
    }

    /** Reset next record position and return self. */
    public void resetPositions(LongIterator positions) {
        vectorizedRowIterator.reset(positions);
    }

    /** Sets the number of rows in this batch. */
    public void setNumRows(int numRows) {
        this.vectorizedColumnBatch.setNumRows(numRows);
    }

    /** Returns the column at `ordinal`. */
    public ColumnVector column(int ordinal) {
        return columns[ordinal];
    }
}
