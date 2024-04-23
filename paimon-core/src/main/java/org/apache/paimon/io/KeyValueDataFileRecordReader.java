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

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;

/** {@link RecordReader} for reading {@link KeyValue} data files. */
public class KeyValueDataFileRecordReader implements RecordReader<KeyValue> {

    private final RecordReader<InternalRow> reader;
    private final KeyValueSerializer serializer;
    private final int level;
    private final boolean ignoreDelete;

    public KeyValueDataFileRecordReader(
            RecordReader<InternalRow> reader,
            RowType keyType,
            RowType valueType,
            int level,
            boolean ignoreDelete) {
        this.reader = reader;
        this.serializer = new KeyValueSerializer(keyType, valueType);
        this.level = level;
        this.ignoreDelete = ignoreDelete;
    }

    @Nullable
    @Override
    public RecordIterator<KeyValue> readBatch() throws IOException {
        RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }

        RecordIterator<KeyValue> transformed =
                iterator.transform(
                        internalRow ->
                                internalRow == null
                                        ? null
                                        : serializer.fromRow(internalRow).setLevel(level));
        // In 0.7- versions, the delete records might be written into data file even when
        // ignore-delete configured, so the reader should also filter the delete records
        return ignoreDelete ? transformed.filter(KeyValue::isAdd) : transformed;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
