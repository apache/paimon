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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.utils.DateTimeUtils;

import java.io.IOException;

/**
 * Serializer for {@link Timestamp}.
 *
 * <p>A {@link Timestamp} instance can be compactly serialized as a long value(= millisecond) when
 * the Timestamp type is compact. Otherwise it's serialized as a long value and a int value.
 */
public class TimestampSerializer implements Serializer<Timestamp> {

    private static final long serialVersionUID = 1L;

    private final int precision;

    public TimestampSerializer(int precision) {
        this.precision = precision;
    }

    @Override
    public Serializer<Timestamp> duplicate() {
        return new TimestampSerializer(precision);
    }

    @Override
    public Timestamp copy(Timestamp from) {
        return from;
    }

    @Override
    public void serialize(Timestamp record, DataOutputView target) throws IOException {
        if (Timestamp.isCompact(precision)) {
            assert record.getNanoOfMillisecond() == 0;
            target.writeLong(record.getMillisecond());
        } else {
            target.writeLong(record.getMillisecond());
            target.writeInt(record.getNanoOfMillisecond());
        }
    }

    @Override
    public Timestamp deserialize(DataInputView source) throws IOException {
        if (Timestamp.isCompact(precision)) {
            long val = source.readLong();
            return Timestamp.fromEpochMillis(val);
        } else {
            long longVal = source.readLong();
            int intVal = source.readInt();
            return Timestamp.fromEpochMillis(longVal, intVal);
        }
    }

    @Override
    public String serializeToString(Timestamp record) {
        return DateTimeUtils.formatTimestamp(record, precision);
    }

    @Override
    public Timestamp deserializeFromString(String s) {
        return DateTimeUtils.parseTimestampData(s, precision);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TimestampSerializer that = (TimestampSerializer) obj;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return precision;
    }
}
