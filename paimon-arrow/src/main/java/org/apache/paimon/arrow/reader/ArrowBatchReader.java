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

package org.apache.paimon.arrow.reader;

import org.apache.paimon.arrow.converter.Arrow2PaimonVectorConverter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Iterator;

/** Reader from a {@link VectorSchemaRoot} to paimon rows. */
public class ArrowBatchReader {

    private final VectorizedColumnBatch batch;
    private final Arrow2PaimonVectorConverter[] convertors;

    public ArrowBatchReader(RowType rowType) {
        ColumnVector[] columnVectors = new ColumnVector[rowType.getFieldCount()];
        this.convertors = new Arrow2PaimonVectorConverter[rowType.getFieldCount()];
        this.batch = new VectorizedColumnBatch(columnVectors);

        for (int i = 0; i < columnVectors.length; i++) {
            this.convertors[i] = Arrow2PaimonVectorConverter.construct(rowType.getTypeAt(i));
        }
    }

    public Iterable<InternalRow> readBatch(VectorSchemaRoot vsr) {
        for (int i = 0; i < batch.columns.length; i++) {
            batch.columns[i] = convertors[i].convertVector(vsr.getVector(i));
        }
        int rowCount = vsr.getRowCount();

        batch.setNumRows(vsr.getRowCount());
        ColumnarRow columnarRow = new ColumnarRow(batch);
        return () ->
                new Iterator<InternalRow>() {
                    private int position = 0;

                    @Override
                    public boolean hasNext() {
                        return position < rowCount;
                    }

                    @Override
                    public InternalRow next() {
                        columnarRow.setRowId(position);
                        position++;
                        return columnarRow;
                    }
                };
    }
}
