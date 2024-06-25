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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.columnar.BytesColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.DecimalColumnVector;
import org.apache.paimon.data.columnar.IntColumnVector;
import org.apache.paimon.data.columnar.LongColumnVector;
import org.apache.paimon.format.parquet.ParquetSchemaConverter;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Parquet write decimal as int32 and int64 and binary, this class wrap the real vector to provide
 * {@link DecimalColumnVector} interface.
 */
public class ParquetDecimalVector implements DecimalColumnVector {

    private final ColumnVector vector;

    public ParquetDecimalVector(ColumnVector vector) {
        this.vector = vector;
    }

    @Override
    public Decimal getDecimal(int i, int precision, int scale) {
        if (ParquetSchemaConverter.is32BitDecimal(precision) && vector instanceof IntColumnVector) {
            return Decimal.fromUnscaledLong(((IntColumnVector) vector).getInt(i), precision, scale);
        } else if (ParquetSchemaConverter.is64BitDecimal(precision)
                && vector instanceof LongColumnVector) {
            return Decimal.fromUnscaledLong(
                    ((LongColumnVector) vector).getLong(i), precision, scale);
        } else {
            checkArgument(
                    vector instanceof BytesColumnVector,
                    "Reading decimal type occur unsupported vector type: %s",
                    vector.getClass());
            return Decimal.fromUnscaledBytes(
                    ((BytesColumnVector) vector).getBytes(i).getBytes(), precision, scale);
        }
    }

    public ColumnVector getVector() {
        return vector;
    }

    @Override
    public boolean isNullAt(int i) {
        return vector.isNullAt(i);
    }
}
