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

package org.apache.paimon;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;

/** Serialize KeyValue to InternalRow with ignorance of key. Only used to write KeyValue to disk. */
public class KeyValueThinSerializer extends ObjectSerializer<KeyValue> {

    private static final long serialVersionUID = 1L;

    private final GenericRow reusedMeta;
    private final JoinedRow reusedKeyWithMeta;

    public KeyValueThinSerializer(RowType keyType, RowType valueType) {
        super(KeyValue.schema(keyType, valueType));

        this.reusedMeta = new GenericRow(2);
        this.reusedKeyWithMeta = new JoinedRow();
    }

    public InternalRow toRow(KeyValue record) {
        return toRow(record.sequenceNumber(), record.valueKind(), record.value());
    }

    public InternalRow toRow(long sequenceNumber, RowKind valueKind, InternalRow value) {
        reusedMeta.setField(0, sequenceNumber);
        reusedMeta.setField(1, valueKind.toByteValue());
        return reusedKeyWithMeta.replace(reusedMeta, value);
    }

    @Override
    public KeyValue fromRow(InternalRow row) {
        throw new UnsupportedOperationException(
                "KeyValue cannot be deserialized from InternalRow by this serializer.");
    }
}
