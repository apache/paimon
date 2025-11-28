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

package org.apache.paimon.format.orc.writer;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.util.List;

/** A {@link Vectorizer} of {@link InternalRow} type element. */
public class RowDataVectorizer extends Vectorizer<InternalRow> {

    private final FieldWriter[] fieldWriters;
    private final String[] fieldNames;
    private final boolean[] isNullable;

    public RowDataVectorizer(
            TypeDescription schema, List<DataField> dataFields, boolean legacyTimestampLtzType) {
        super(schema);
        FieldWriterFactory fieldWriterFactory = new FieldWriterFactory(legacyTimestampLtzType);
        this.fieldWriters = new FieldWriter[dataFields.size()];
        this.fieldNames = new String[dataFields.size()];
        this.isNullable = new boolean[dataFields.size()];
        for (int i = 0; i < dataFields.size(); i++) {
            DataField field = dataFields.get(i);
            fieldWriters[i] = field.type().accept(fieldWriterFactory);
            fieldNames[i] = field.name();
            isNullable[i] = field.type().isNullable();
        }
    }

    public long vectorize(InternalRow row, VectorizedRowBatch batch) {
        int rowId = batch.size++;
        int memBytes = 0;
        for (int i = 0; i < fieldNames.length; ++i) {
            ColumnVector fieldColumn = batch.cols[i];
            if (row.isNullAt(i)) {
                if (!isNullable[i]) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Field '%s' expected not null but found null value. A possible cause is that the "
                                            + "table used %s or %s merge-engine and the aggregate function produced "
                                            + "null value when retracting.",
                                    fieldNames[i],
                                    CoreOptions.MergeEngine.PARTIAL_UPDATE,
                                    CoreOptions.MergeEngine.AGGREGATE));
                }
                fieldColumn.noNulls = false;
                fieldColumn.isNull[rowId] = true;
            } else {
                memBytes += fieldWriters[i].write(rowId, fieldColumn, row, i);
            }
        }
        return memBytes;
    }
}
