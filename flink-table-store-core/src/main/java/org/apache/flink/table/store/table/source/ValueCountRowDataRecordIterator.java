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
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * An {@link RecordReader.RecordIterator} mapping a {@link KeyValue} to several {@link RowData}
 * according to its key. These {@link RowData}s are the same. The number of rows depends on the
 * value of {@link KeyValue}.
 */
public class ValueCountRowDataRecordIterator extends ResetRowKindRecordIterator {

    private final @Nullable ProjectedRowData projectedRowData;

    private RowData rowData;
    private long count;

    public ValueCountRowDataRecordIterator(
            RecordReader.RecordIterator<KeyValue> kvIterator, @Nullable int[][] projection) {
        super(kvIterator);
        this.projectedRowData = projection == null ? null : ProjectedRowData.from(projection);

        this.rowData = null;
        this.count = 0;
    }

    @Override
    public RowData next() throws IOException {
        while (true) {
            if (count > 0) {
                count--;
                return rowData;
            } else {
                KeyValue kv = nextKeyValue();
                if (kv == null) {
                    return null;
                }

                rowData =
                        projectedRowData == null ? kv.key() : projectedRowData.replaceRow(kv.key());
                long value = kv.value().getLong(0);
                if (value < 0) {
                    rowData.setRowKind(RowKind.DELETE);
                }
                count = Math.abs(value);
            }
        }
    }
}
