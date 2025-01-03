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

package org.apache.paimon.format.orc.reader;

import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/** This column vector is used to adapt hive's StructColumnVector to Flink's RowColumnVector. */
public class OrcRowColumnVector extends AbstractOrcColumnVector
        implements org.apache.paimon.data.columnar.RowColumnVector {

    private final VectorizedColumnBatch batch;

    public OrcRowColumnVector(
            StructColumnVector hiveVector, VectorizedRowBatch orcBatch, RowType type) {
        super(hiveVector, orcBatch);
        int len = hiveVector.fields.length;
        ColumnVector[] paimonVectors = new ColumnVector[len];
        for (int i = 0; i < len; i++) {
            paimonVectors[i] =
                    createPaimonVector(hiveVector.fields[i], orcBatch, type.getTypeAt(i));
        }
        this.batch = new VectorizedColumnBatch(paimonVectors);
    }

    @Override
    public ColumnarRow getRow(int i) {
        // no need to call rowMapper here .
        return new ColumnarRow(batch, i);
    }

    @Override
    public VectorizedColumnBatch getBatch() {
        return batch;
    }
}
