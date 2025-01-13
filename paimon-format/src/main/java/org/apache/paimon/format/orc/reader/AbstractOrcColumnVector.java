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

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/** This column vector is used to adapt hive's ColumnVector to Paimon's ColumnVector. */
public abstract class AbstractOrcColumnVector
        implements org.apache.paimon.data.columnar.ColumnVector {

    private final ColumnVector vector;

    private final VectorizedRowBatch orcBatch;

    AbstractOrcColumnVector(ColumnVector vector, VectorizedRowBatch orcBatch) {
        this.vector = vector;
        this.orcBatch = orcBatch;
    }

    protected int rowMapper(int r) {
        if (vector.isRepeating) {
            return 0;
        }
        return this.orcBatch.selectedInUse ? this.orcBatch.getSelected()[r] : r;
    }

    @Override
    public boolean isNullAt(int i) {
        return !vector.noNulls && vector.isNull[rowMapper(i)];
    }

    public static org.apache.paimon.data.columnar.ColumnVector createPaimonVector(
            ColumnVector vector, VectorizedRowBatch orcBatch, DataType dataType) {
        if (vector instanceof LongColumnVector) {
            if (dataType.getTypeRoot() == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                return new OrcLegacyTimestampColumnVector((LongColumnVector) vector, orcBatch);
            } else {
                return new OrcLongColumnVector((LongColumnVector) vector, orcBatch);
            }
        } else if (vector instanceof DoubleColumnVector) {
            return new OrcDoubleColumnVector((DoubleColumnVector) vector, orcBatch);
        } else if (vector instanceof BytesColumnVector) {
            return new OrcBytesColumnVector((BytesColumnVector) vector, orcBatch);
        } else if (vector instanceof DecimalColumnVector) {
            return new OrcDecimalColumnVector((DecimalColumnVector) vector, orcBatch);
        } else if (vector instanceof TimestampColumnVector) {
            return new OrcTimestampColumnVector(vector, orcBatch);
        } else if (vector instanceof ListColumnVector) {
            return new OrcArrayColumnVector(
                    (ListColumnVector) vector, orcBatch, (ArrayType) dataType);
        } else if (vector instanceof StructColumnVector) {
            return new OrcRowColumnVector(
                    (StructColumnVector) vector, orcBatch, (RowType) dataType);
        } else if (vector instanceof MapColumnVector) {
            return new OrcMapColumnVector((MapColumnVector) vector, orcBatch, (MapType) dataType);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported vector: " + vector.getClass().getName());
        }
    }
}
