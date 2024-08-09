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
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.time.Duration;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A factory to create {@link RecordReader} expires records by time. */
public class RecordLevelExpire {

    private final int timeField;
    private final int expireTime;

    @Nullable
    public static RecordLevelExpire create(CoreOptions options, RowType rowType) {
        Duration expireTime = options.recordLevelExpireTime();
        if (expireTime == null) {
            return null;
        }

        String timeField = options.recordLevelTimeField();
        if (timeField == null) {
            throw new IllegalArgumentException(
                    "You should set time field for record-level expire.");
        }

        // should no project here, record level expire only works in compaction
        int fieldIndex = rowType.getFieldIndex(timeField);
        if (fieldIndex == -1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can not find time field %s for record level expire.", timeField));
        }

        DataField field = rowType.getField(timeField);
        if (!(field.type() instanceof IntType)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Record level time field should be INT type, but is %s.",
                            field.type()));
        }

        return new RecordLevelExpire(fieldIndex, (int) expireTime.getSeconds());
    }

    private RecordLevelExpire(int timeField, int expireTime) {
        this.timeField = timeField;
        this.expireTime = expireTime;
    }

    public FileReaderFactory<KeyValue> wrap(FileReaderFactory<KeyValue> readerFactory) {
        return file -> wrap(readerFactory.createRecordReader(file));
    }

    private RecordReader<KeyValue> wrap(RecordReader<KeyValue> reader) {
        int currentTime = (int) (System.currentTimeMillis() / 1000);
        return reader.filter(
                kv -> {
                    checkArgument(
                            !kv.value().isNullAt(timeField),
                            "Time field for record-level expire should not be null.");
                    int recordTime = kv.value().getInt(timeField);
                    return currentTime <= recordTime + expireTime;
                });
    }
}
