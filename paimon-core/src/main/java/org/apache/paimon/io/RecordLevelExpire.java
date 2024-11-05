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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import javax.annotation.Nullable;

import java.time.Duration;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A factory to create {@link RecordReader} expires records by time. */
public class RecordLevelExpire {

    private final int timeFieldIndex;
    private final int expireTime;
    private final CoreOptions.TimeFieldType timeFieldType;
    private final DataField rawDataField;

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

        CoreOptions.TimeFieldType timeFieldType = options.recordLevelTimeFieldType();
        DataField field = rowType.getField(timeFieldName);
        if (!isValidateFieldType(timeFieldType, field)) {
            throw new IllegalArgumentException(
                    String.format(
                            "The record level time field type should be one of SECONDS_INT, SECONDS_LONG, MILLIS_LONG or TIMESTAMP, "
                                    + "but time field type is %s, field type is %s. You can specify the type through the config '%s'.",
                            timeFieldType,
                            field.type(),
                            CoreOptions.RECORD_LEVEL_TIME_FIELD_TYPE.key()));
        }

        return new RecordLevelExpire(
                fieldIndex, (int) expireTime.getSeconds(), timeFieldType, field);
    }

    private static boolean isValidateFieldType(
            CoreOptions.TimeFieldType timeFieldType, DataField field) {
        DataType dataType = field.type();
        return ((timeFieldType == CoreOptions.TimeFieldType.SECONDS_INT
                        && dataType instanceof IntType)
                || (timeFieldType == CoreOptions.TimeFieldType.SECONDS_LONG
                        && dataType instanceof BigIntType)
                || (timeFieldType == CoreOptions.TimeFieldType.MILLIS_LONG
                        && dataType instanceof BigIntType)
                || (timeFieldType == CoreOptions.TimeFieldType.TIMESTAMP
                        && dataType instanceof TimestampType)
                || (timeFieldType == CoreOptions.TimeFieldType.TIMESTAMP
                        && dataType instanceof LocalZonedTimestampType));
    }

    private RecordLevelExpire(
            int timeFieldIndex,
            int expireTime,
            CoreOptions.TimeFieldType timeFieldType,
            DataField rawDataField) {
        this.timeFieldIndex = timeFieldIndex;
        this.expireTime = expireTime;
        this.timeFieldType = timeFieldType;
        this.rawDataField = rawDataField;
    }

    public FileReaderFactory<KeyValue> wrap(FileReaderFactory<KeyValue> readerFactory) {
        return file -> wrap(readerFactory.createRecordReader(file));
    }

    private RecordReader<KeyValue> wrap(RecordReader<KeyValue> reader) {
        int currentTime = (int) (System.currentTimeMillis() / 1000);
        return reader.filter(
                kv -> {
                    checkArgument(
                            !kv.value().isNullAt(timeFieldIndex),
                            "Time field for record-level expire should not be null.");
                    final int recordTime;
                    switch (timeFieldType) {
                        case SECONDS_INT:
                            recordTime = kv.value().getInt(timeFieldIndex);
                            break;
                        case SECONDS_LONG:
                            recordTime = (int) kv.value().getLong(timeFieldIndex);
                            break;
                        case MILLIS_LONG:
                            recordTime = (int) (kv.value().getLong(timeFieldIndex) / 1000);
                            break;
                        case TIMESTAMP:
                            Timestamp timestamp;
                            if (rawDataField.type() instanceof TimestampType) {
                                TimestampType timestampType = (TimestampType) rawDataField.type();
                                timestamp =
                                        kv.value()
                                                .getTimestamp(
                                                        timeFieldIndex,
                                                        timestampType.getPrecision());
                            } else if (rawDataField.type() instanceof LocalZonedTimestampType) {
                                LocalZonedTimestampType timestampType =
                                        (LocalZonedTimestampType) rawDataField.type();
                                timestamp =
                                        kv.value()
                                                .getTimestamp(
                                                        timeFieldIndex,
                                                        timestampType.getPrecision());
                            } else {
                                throw new UnsupportedOperationException(
                                        "Unsupported timestamp type: " + rawDataField.type());
                            }
                            recordTime = (int) (timestamp.getMillisecond() / 1000);
                            break;
                        default:
                            String msg =
                                    String.format(
                                            "type %s not support in %s",
                                            timeFieldType,
                                            CoreOptions.TimeFieldType.class.getName());
                            throw new IllegalArgumentException(msg);
                    }
                    return currentTime <= recordTime + expireTime;
                });
    }
}
