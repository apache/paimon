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

package org.apache.paimon.arrow.converter;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.types.DataType;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Iterator;

/** Util to convert a {@link VectorSchemaRoot} to paimon rows. */
public class Arrow2PaimonBatchConverter {

    public static Iterable<InternalRow> convet(DataType[] types, VectorSchemaRoot vsr) {
        ColumnVector[] columnVectors = new ColumnVector[types.length];

        for (int i = 0; i < types.length; i++) {
            columnVectors[i] =
                    Arrow2PaimonVectorConvertor.construct(types[i]).convertVector(vsr.getVector(i));
        }

        int rowCount = vsr.getRowCount();
        VectorizedColumnBatch batch = new VectorizedColumnBatch(columnVectors);
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
