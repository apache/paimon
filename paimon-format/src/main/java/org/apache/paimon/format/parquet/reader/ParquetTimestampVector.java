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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.LongColumnVector;
import org.apache.paimon.data.columnar.TimestampColumnVector;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Parquet write timestamp precision 0-3 as int64 mills, 4-6 as int64 micros, 7-9 as int96, this
 * class wrap the real vector to provide {@link TimestampColumnVector} interface.
 */
public class ParquetTimestampVector implements TimestampColumnVector {

    private final ColumnVector vector;

    public ParquetTimestampVector(ColumnVector vector) {
        this.vector = vector;
    }

    @Override
    public Timestamp getTimestamp(int i, int precision) {
        if (precision <= 3 && vector instanceof LongColumnVector) {
            return Timestamp.fromEpochMillis(((LongColumnVector) vector).getLong(i));
        } else if (precision <= 6 && vector instanceof LongColumnVector) {
            return Timestamp.fromMicros(((LongColumnVector) vector).getLong(i));
        } else {
            checkArgument(
                    vector instanceof TimestampColumnVector,
                    "Reading timestamp type occur unsupported vector type: %s",
                    vector.getClass());
            return ((TimestampColumnVector) vector).getTimestamp(i, precision);
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
