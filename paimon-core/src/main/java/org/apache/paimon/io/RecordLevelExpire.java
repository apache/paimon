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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A factory to create {@link RecordReader} expires records by time. */
public class RecordLevelExpire {

    private final int expireTime;
    private final Function<InternalRow, Integer> fieldGetter;

    @Nullable
    public static RecordLevelExpire create(CoreOptions options, RowType rowType) {
        Duration expireTime = options.recordLevelExpireTime();
        if (expireTime == null) {
            return null;
        }

        String timeFieldName = options.recordLevelTimeField();
        if (timeFieldName == null) {
            throw new IllegalArgumentException(
                    "You should set time field for record-level expire.");
        }

        // should no project here, record level expire only works in compaction
        int fieldIndex = rowType.getFieldIndex(timeFieldName);
        if (fieldIndex == -1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can not find time field %s for record level expire.", timeFieldName));
        }

        DataType dataType = rowType.getField(timeFieldName).type();
        Function<InternalRow, Integer> fieldGetter = createFieldGetter(dataType, fieldIndex);
        return new RecordLevelExpire((int) expireTime.getSeconds(), fieldGetter);
    }

    private RecordLevelExpire(int expireTime, Function<InternalRow, Integer> fieldGetter) {
        this.expireTime = expireTime;
        this.fieldGetter = fieldGetter;
    }

    public FileReaderFactory<KeyValue> wrap(FileReaderFactory<KeyValue> readerFactory) {
        return file -> wrap(readerFactory.createRecordReader(file));
    }

    private RecordReader<KeyValue> wrap(RecordReader<KeyValue> reader) {
        int currentTime = (int) (System.currentTimeMillis() / 1000);
        return reader.filter(kv -> currentTime <= fieldGetter.apply(kv.value()) + expireTime);
    }

    private static Function<InternalRow, Integer> createFieldGetter(
            DataType dataType, int fieldIndex) {
        final Function<InternalRow, Integer> fieldGetter;
        if (dataType instanceof IntType) {
            fieldGetter = row -> row.getInt(fieldIndex);
        } else if (dataType instanceof BigIntType) {
            fieldGetter =
                    row -> {
                        long value = row.getLong(fieldIndex);
                        // If it is milliseconds, convert it to seconds.
                        return (int) (value >= 1_000_000_000_000L ? value / 1000 : value);
                    };
        } else if (dataType instanceof TimestampType
                || dataType instanceof LocalZonedTimestampType) {
            int precision = DataTypeChecks.getPrecision(dataType);
            fieldGetter =
                    row -> (int) (row.getTimestamp(fieldIndex, precision).getMillisecond() / 1000);
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "The record level time field type should be one of INT, BIGINT, or TIMESTAMP, but field type is %s.",
                            dataType));
        }

        return row -> {
            checkArgument(
                    !row.isNullAt(fieldIndex),
                    "Time field for record-level expire should not be null.");
            return fieldGetter.apply(row);
        };
    }
}
