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

package org.apache.flink.table.store.table.source;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.types.RowKind;

import java.io.IOException;

/** A {@link RecordReader.RecordIterator} mapping a {@link KeyValue} to its value. */
public class ValueContentRowDataRecordIterator implements RecordReader.RecordIterator<RowData> {

    private final RecordReader.RecordIterator<KeyValue> kvIterator;

    public ValueContentRowDataRecordIterator(RecordReader.RecordIterator<KeyValue> kvIterator) {
        this.kvIterator = kvIterator;
    }

    @Override
    public RowData next() throws IOException {
        KeyValue kv = kvIterator.next();
        if (kv == null) {
            return null;
        }

        RowData rowData = kv.value();
        // kv.value() is reused, so we need to set row kind each time
        switch (kv.valueKind()) {
            case ADD:
                rowData.setRowKind(RowKind.INSERT);
                break;
            case DELETE:
                rowData.setRowKind(RowKind.DELETE);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown value kind " + kv.valueKind().name());
        }
        return rowData;
    }

    @Override
    public void releaseBatch() {
        kvIterator.releaseBatch();
    }
}
