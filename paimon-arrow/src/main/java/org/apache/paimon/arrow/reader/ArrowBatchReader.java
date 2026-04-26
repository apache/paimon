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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.StringUtils.toLowerCaseIfNeed;

/** Reader from a {@link VectorSchemaRoot} to paimon rows. */
public class ArrowBatchReader {

    private final VectorizedColumnBatch batch;
    private final Arrow2PaimonVectorConverter[] convertors;
    private final RowType projectedRowType;
    private final boolean caseSensitive;

    public ArrowBatchReader(RowType rowType, boolean caseSensitive) {
        this(
                rowType,
                caseSensitive,
                Arrow2PaimonVectorConverter.Arrow2PaimonVectorConvertorVisitor.INSTANCE);
    }

    public ArrowBatchReader(
            RowType rowType,
            boolean caseSensitive,
            Arrow2PaimonVectorConverter.Arrow2PaimonVectorConvertorVisitor visitor) {
        ColumnVector[] columnVectors = new ColumnVector[rowType.getFieldCount()];
        this.convertors = new Arrow2PaimonVectorConverter[rowType.getFieldCount()];
        this.batch = new VectorizedColumnBatch(columnVectors);
        this.projectedRowType = rowType;

        for (int i = 0; i < columnVectors.length; i++) {
            this.convertors[i] =
                    Arrow2PaimonVectorConverter.construct(visitor, rowType.getTypeAt(i));
        }
        this.caseSensitive = caseSensitive;
    }

    /** A {@link ColumnVector} that always returns null for any position. */
    private static final ColumnVector NULL_COLUMN_VECTOR = i -> true;

    public Iterable<InternalRow> readBatch(VectorSchemaRoot vsr) {
        int[] mapping = new int[projectedRowType.getFieldCount()];
        Schema arrowSchema = vsr.getSchema();
        Map<String, Integer> arrowFieldIndex = new HashMap<>();
        List<Field> arrowFields = arrowSchema.getFields();
        for (int j = 0; j < arrowFields.size(); j++) {
            arrowFieldIndex.put(toLowerCaseIfNeed(arrowFields.get(j).getName(), caseSensitive), j);
        }
        List<DataField> dataFields = projectedRowType.getFields();
        for (int i = 0; i < dataFields.size(); ++i) {
            String fieldName = toLowerCaseIfNeed(dataFields.get(i).name(), caseSensitive);
            mapping[i] = arrowFieldIndex.getOrDefault(fieldName, -1);
        }

        for (int i = 0; i < batch.columns.length; i++) {
            if (mapping[i] >= 0) {
                batch.columns[i] = convertors[i].convertVector(vsr.getVector(mapping[i]));
            } else {
                batch.columns[i] = NULL_COLUMN_VECTOR;
            }
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
                        return columnarRow;
                    }
                };
    }
}
