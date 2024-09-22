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
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Iterator;
import java.util.List;

/** Reader from a {@link VectorSchemaRoot} to paimon rows. */
public class ArrowBatchReader {

    private final InternalRowSerializer internalRowSerializer;
    private final VectorizedColumnBatch batch;
    private final Arrow2PaimonVectorConverter[] convertors;
    private final RowType projectedRowType;

    public ArrowBatchReader(RowType rowType) {
        this.internalRowSerializer = new InternalRowSerializer(rowType);
        ColumnVector[] columnVectors = new ColumnVector[rowType.getFieldCount()];
        this.convertors = new Arrow2PaimonVectorConverter[rowType.getFieldCount()];
        this.batch = new VectorizedColumnBatch(columnVectors);
        this.projectedRowType = rowType;

        for (int i = 0; i < columnVectors.length; i++) {
            this.convertors[i] = Arrow2PaimonVectorConverter.construct(rowType.getTypeAt(i));
        }
    }

    public Iterable<InternalRow> readBatch(VectorSchemaRoot vsr) {
        int[] mapping = new int[projectedRowType.getFieldCount()];
        Schema arrowSchema = vsr.getSchema();
        List<DataField> dataFields = projectedRowType.getFields();
        for (int i = 0; i < dataFields.size(); ++i) {
            try {
                Field field = arrowSchema.findField(dataFields.get(i).name().toLowerCase());
                int idx = arrowSchema.getFields().indexOf(field);
                mapping[i] = idx;
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(e);
            }
        }

        for (int i = 0; i < batch.columns.length; i++) {
            batch.columns[i] = convertors[i].convertVector(vsr.getVector(mapping[i]));
        }

        int rowCount = vsr.getRowCount();
        batch.setNumRows(vsr.getRowCount());
        final ColumnarRow columnarRow = new ColumnarRow(batch);
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
                        // when pk merge, the last value will be referenced by maxKey. If reader
                        // close, the maxKey will be useless. We must avoid this situation
                        return position == rowCount
                                ? internalRowSerializer.toBinaryRow(columnarRow)
                                : columnarRow;
                    }
                };
    }
}
