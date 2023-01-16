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

import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.types.RowKind;

import java.io.IOException;

/**
 * An {@link RecordReader.RecordIterator} mapping a {@link KeyValue} to several {@link InternalRow}
 * according to its key. These {@link InternalRow}s are the same. The number of rows depends on the
 * value of {@link KeyValue}.
 */
public class ValueCountRowDataRecordIterator extends ResetRowKindRecordIterator {

    private InternalRow rowData;
    private long count;

    public ValueCountRowDataRecordIterator(RecordReader.RecordIterator<KeyValue> kvIterator) {
        super(kvIterator);
        this.rowData = null;
        this.count = 0;
    }

    @Override
    public InternalRow next() throws IOException {
        while (true) {
            if (count > 0) {
                count--;
                return rowData;
            } else {
                KeyValue kv = nextKeyValue();
                if (kv == null) {
                    return null;
                }

                rowData = kv.key();
                long value = kv.value().getLong(0);
                if (value < 0) {
                    rowData.setRowKind(RowKind.DELETE);
                }
                count = Math.abs(value);
            }
        }
    }
}
