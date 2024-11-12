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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarArray;
import org.apache.paimon.types.ArrayType;

import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/** This column vector is used to adapt hive's ListColumnVector to Paimon's ArrayColumnVector. */
public class OrcArrayColumnVector extends AbstractOrcColumnVector
        implements org.apache.paimon.data.columnar.ArrayColumnVector {

    private final ListColumnVector hiveVector;
    private final ColumnVector paimonVector;

    public OrcArrayColumnVector(
            ListColumnVector hiveVector, VectorizedRowBatch orcBatch, ArrayType type) {
        super(hiveVector, orcBatch);
        this.hiveVector = hiveVector;
        this.paimonVector = createPaimonVector(hiveVector.child, orcBatch, type.getElementType());
    }

    @Override
    public InternalArray getArray(int i) {
        i = rowMapper(i);
        long offset = hiveVector.offsets[i];
        long length = hiveVector.lengths[i];
        return new ColumnarArray(paimonVector, (int) offset, (int) length);
    }

    @Override
    public ColumnVector getColumnVector() {
        return paimonVector;
    }
}
