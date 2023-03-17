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
import org.apache.paimon.utils.OffsetRow;

/**
 * Serializer for {@link KeyValue}.
 *
 * <p>NOTE: {@link InternalRow} and {@link KeyValue} produced by this serializer are reused.
 */
public class KeyValueSerializer extends ObjectSerializer<KeyValue> {

    private static final long serialVersionUID = 1L;

    private final int keyArity;

    private final GenericRow reusedMeta;
    private final JoinedRow reusedKeyWithMeta;
    private final JoinedRow reusedRow;

    private final OffsetRow reusedKey;
    private final OffsetRow reusedValue;
    private final KeyValue reusedKv;

    public KeyValueSerializer(RowType keyType, RowType valueType) {
        super(KeyValue.schema(keyType, valueType));

        this.keyArity = keyType.getFieldCount();
        int valueArity = valueType.getFieldCount();

        this.reusedMeta = new GenericRow(2);
        this.reusedKeyWithMeta = new JoinedRow();
        this.reusedRow = new JoinedRow();

        this.reusedKey = new OffsetRow(keyArity, 0);
        this.reusedValue = new OffsetRow(valueArity, keyArity + 2);
        this.reusedKv = new KeyValue().replace(reusedKey, -1, null, reusedValue);
    }

    @Override
    public InternalRow toRow(KeyValue record) {
        return toRow(record.key(), record.sequenceNumber(), record.valueKind(), record.value());
    }

    public InternalRow toRow(
            InternalRow key, long sequenceNumber, RowKind valueKind, InternalRow value) {
        reusedMeta.setField(0, sequenceNumber);
        reusedMeta.setField(1, valueKind.toByteValue());
        return reusedRow.replace(reusedKeyWithMeta.replace(key, reusedMeta), value);
    }

    @Override
    public KeyValue fromRow(InternalRow row) {
        reusedKey.replace(row);
        reusedValue.replace(row);
        long sequenceNumber = row.getLong(keyArity);
        RowKind valueKind = RowKind.fromByteValue(row.getByte(keyArity + 1));
        reusedKv.replace(reusedKey, sequenceNumber, valueKind, reusedValue);
        return reusedKv;
    }

    public KeyValue getReusedKv() {
        return reusedKv;
    }
}
