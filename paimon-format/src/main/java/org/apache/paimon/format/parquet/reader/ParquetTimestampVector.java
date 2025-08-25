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

import java.util.TimeZone;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Parquet write timestamp precision 0-3 as int64 mills, 4-6 as int64 micros, 7-9 as int96, this
 * class wrap the real vector to provide {@link TimestampColumnVector} interface.
 */
public class ParquetTimestampVector implements TimestampColumnVector {

    private final ColumnVector vector;

    private final TimeZone sourceTimezone;
    private final TimeZone targetTimezone;

    public ParquetTimestampVector(ColumnVector vector) {
        this(vector, null, null);
    }

    public ParquetTimestampVector(
            ColumnVector vector, TimeZone sourceTimezone, TimeZone targetTimezone) {
        this.vector = vector;
        this.sourceTimezone = sourceTimezone;
        this.targetTimezone = targetTimezone;
    }

    @Override
    public Timestamp getTimestamp(int i, int precision) {
        if (precision <= 3 && vector instanceof LongColumnVector) {
            long millis = ((LongColumnVector) vector).getLong(i);
            if (needsTimezoneConversion()) {
                millis = convertTimezone(millis, true);
            }
            return Timestamp.fromEpochMillis(millis);
        } else if (precision <= 6 && vector instanceof LongColumnVector) {
            long micros = ((LongColumnVector) vector).getLong(i);
            if (needsTimezoneConversion()) {
                micros = convertTimezone(micros, false);
            }
            return Timestamp.fromMicros(micros);
        } else {
            checkArgument(
                    vector instanceof TimestampColumnVector,
                    "Reading timestamp type occur unsupported vector type: %s",
                    vector.getClass());
            return ((TimestampColumnVector) vector).getTimestamp(i, precision);
        }
    }

    private boolean needsTimezoneConversion() {
        return sourceTimezone != null && targetTimezone != null;
    }

    private long convertTimezone(long timestamp, boolean isMillis) {
        // Convert to milliseconds if needed
        long millis = isMillis ? timestamp : timestamp / 1000;

        // Apply timezone conversion
        // Hive stores timestamps in UTC in Parquet files, so source timezone is UTC
        // Target timezone is the system default timezone
        TimeZone sourceTz = sourceTimezone;
        TimeZone targetTz = targetTimezone;

        int sourceOffset = sourceTz.getOffset(millis);
        int targetOffset = targetTz.getOffset(millis);
        int offsetDiff = targetOffset - sourceOffset;

        long convertedMillis = millis + offsetDiff;

        // Convert back to original precision
        return isMillis ? convertedMillis : convertedMillis * 1000;
    }

    public ColumnVector getVector() {
        return vector;
    }

    @Override
    public boolean isNullAt(int i) {
        return vector.isNullAt(i);
    }
}
