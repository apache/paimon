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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.format.orc.writer.FieldWriterFactory.WRITER_FACTORY;

/** A {@link Vectorizer} of {@link InternalRow} type element. */
public class RowDataVectorizer extends Vectorizer<InternalRow> {

    private final List<FieldWriter> fieldWriters;

    public RowDataVectorizer(TypeDescription schema, DataType[] fieldTypes) {
        super(schema);
        this.fieldWriters =
                Arrays.stream(fieldTypes)
                        .map(t -> t.accept(WRITER_FACTORY))
                        .collect(Collectors.toList());
    }

    @Override
    public void vectorize(InternalRow row, VectorizedRowBatch batch) {
        int rowId = batch.size++;
        for (int i = 0; i < row.getFieldCount(); ++i) {
            ColumnVector fieldColumn = batch.cols[i];
            if (row.isNullAt(i)) {
                fieldColumn.noNulls = false;
                fieldColumn.isNull[rowId] = true;
            } else {
                fieldWriters.get(i).write(rowId, fieldColumn, row, i);
            }
        }
    }
}
