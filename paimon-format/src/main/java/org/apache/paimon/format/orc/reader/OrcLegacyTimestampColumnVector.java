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

import org.apache.paimon.data.Timestamp;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.time.LocalDateTime;

/**
 * This class is used to adapt to Hive's legacy (2.0.x) timestamp column vector which is a
 * LongColumnVector.
 */
public class OrcLegacyTimestampColumnVector extends AbstractOrcColumnVector
        implements org.apache.paimon.data.columnar.TimestampColumnVector {

    private final LongColumnVector hiveVector;

    OrcLegacyTimestampColumnVector(LongColumnVector vector, VectorizedRowBatch orcBatch) {
        super(vector, orcBatch);
        this.hiveVector = vector;
    }

    @Override
    public Timestamp getTimestamp(int i, int precision) {
        i = rowMapper(i);
        java.sql.Timestamp timestamp = toTimestamp(hiveVector.vector[i]);
        return Timestamp.fromSQLTimestamp(timestamp);
    }

    // creates a Hive ColumnVector of constant timestamp value
    public static ColumnVector createFromConstant(int batchSize, Object value) {
        LongColumnVector res = new LongColumnVector(batchSize);
        if (value == null) {
            res.noNulls = false;
            res.isNull[0] = true;
            res.isRepeating = true;
        } else {
            java.sql.Timestamp timestamp =
                    value instanceof LocalDateTime
                            ? java.sql.Timestamp.valueOf((LocalDateTime) value)
                            : (java.sql.Timestamp) value;
            res.fill(fromTimestamp(timestamp));
            res.isNull[0] = false;
        }
        return res;
    }

    // converting from/to Timestamp is copied from Hive 2.0.0 TimestampUtils
    private static long fromTimestamp(java.sql.Timestamp timestamp) {
        long time = timestamp.getTime();
        int nanos = timestamp.getNanos();
        return (time * 1000000) + (nanos % 1000000);
    }

    private static java.sql.Timestamp toTimestamp(long timeInNanoSec) {
        long integralSecInMillis =
                (timeInNanoSec / 1000000000) * 1000; // Full seconds converted to millis.
        long nanos = timeInNanoSec % 1000000000; // The nanoseconds.
        if (nanos < 0) {
            nanos =
                    1000000000
                            + nanos; // The positive nano-part that will be added to milliseconds.
            integralSecInMillis =
                    ((timeInNanoSec / 1000000000) - 1) * 1000; // Reduce by one second.
        }
        java.sql.Timestamp res = new java.sql.Timestamp(0);
        res.setTime(integralSecInMillis);
        res.setNanos((int) nanos);
        return res;
    }
}
