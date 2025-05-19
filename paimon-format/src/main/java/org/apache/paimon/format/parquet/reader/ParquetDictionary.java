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
import org.apache.paimon.data.columnar.Dictionary;

import org.apache.parquet.io.api.Binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.paimon.format.parquet.writer.ParquetRowDataWriter.JULIAN_EPOCH_OFFSET_DAYS;
import static org.apache.paimon.format.parquet.writer.ParquetRowDataWriter.MILLIS_IN_DAY;
import static org.apache.paimon.format.parquet.writer.ParquetRowDataWriter.NANOS_PER_MILLISECOND;
import static org.apache.paimon.format.parquet.writer.ParquetRowDataWriter.NANOS_PER_SECOND;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Parquet dictionary. */
public final class ParquetDictionary implements Dictionary {

    private final org.apache.parquet.column.Dictionary dictionary;

    public ParquetDictionary(org.apache.parquet.column.Dictionary dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public int decodeToInt(int id) {
        return dictionary.decodeToInt(id);
    }

    @Override
    public long decodeToLong(int id) {
        return dictionary.decodeToLong(id);
    }

    @Override
    public float decodeToFloat(int id) {
        return dictionary.decodeToFloat(id);
    }

    @Override
    public double decodeToDouble(int id) {
        return dictionary.decodeToDouble(id);
    }

    @Override
    public byte[] decodeToBinary(int id) {
        return dictionary.decodeToBinary(id).getBytesUnsafe();
    }

    @Override
    public Timestamp decodeToTimestamp(int id) {
        return decodeInt96ToTimestamp(true, dictionary, id);
    }

    public static Timestamp decodeInt96ToTimestamp(
            boolean utcTimestamp, org.apache.parquet.column.Dictionary dictionary, int id) {
        Binary binary = dictionary.decodeToBinary(id);
        checkArgument(binary.length() == 12, "Timestamp with int96 should be 12 bytes.");
        ByteBuffer buffer = binary.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
        return int96ToTimestamp(utcTimestamp, buffer.getLong(), buffer.getInt());
    }

    public static Timestamp int96ToTimestamp(boolean utcTimestamp, long nanosOfDay, int julianDay) {
        long millisecond = julianDayToMillis(julianDay) + (nanosOfDay / NANOS_PER_MILLISECOND);

        if (utcTimestamp) {
            int nanoOfMillisecond = (int) (nanosOfDay % NANOS_PER_MILLISECOND);
            return Timestamp.fromEpochMillis(millisecond, nanoOfMillisecond);
        } else {
            java.sql.Timestamp timestamp = new java.sql.Timestamp(millisecond);
            timestamp.setNanos((int) (nanosOfDay % NANOS_PER_SECOND));
            return Timestamp.fromSQLTimestamp(timestamp);
        }
    }

    private static long julianDayToMillis(int julianDay) {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
    }
}
