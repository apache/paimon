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
import org.apache.paimon.types.RowKind;

import java.io.IOException;

/**
 * A {@link RecordReader.RecordIterator} which resets {@link RowKind#INSERT} to previous key value.
 */
public abstract class ResetRowKindRecordIterator
        implements RecordReader.RecordIterator<InternalRow> {

    private final RecordReader.RecordIterator<KeyValue> kvIterator;

    private KeyValue keyValue;

    public ResetRowKindRecordIterator(RecordReader.RecordIterator<KeyValue> kvIterator) {
        this.kvIterator = kvIterator;
    }

    public final KeyValue nextKeyValue() throws IOException {
        // The RowData is reused in kvIterator, we should set back to insert kind
        // Failure to do so will result in uncontrollable exceptions
        if (keyValue != null) {
            keyValue.key().setRowKind(RowKind.INSERT);
            keyValue.value().setRowKind(RowKind.INSERT);
        }

        keyValue = kvIterator.next();
        return keyValue;
    }

    @Override
    public final void releaseBatch() {
        kvIterator.releaseBatch();
    }
}
