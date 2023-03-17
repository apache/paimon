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

package org.apache.paimon.table.source;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;

import java.io.IOException;

/** A {@link RecordReader.RecordIterator} mapping a {@link KeyValue} to its value. */
public class ValueContentRowDataRecordIterator extends ResetRowKindRecordIterator {

    public ValueContentRowDataRecordIterator(RecordReader.RecordIterator<KeyValue> kvIterator) {
        super(kvIterator);
    }

    @Override
    public InternalRow next() throws IOException {
        KeyValue kv = nextKeyValue();
        if (kv == null) {
            return null;
        }

        InternalRow rowData = kv.value();
        rowData.setRowKind(kv.valueKind());
        return rowData;
    }
}
