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

package org.apache.flink.table.store.file;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.store.file.utils.ObjectSerializer;
import org.apache.flink.table.store.file.utils.OffsetRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * Serializer for {@link KeyValue}.
 *
 * <p>NOTE: {@link RowData} and {@link KeyValue} produced by this serializer are reused.
 */
public class KeyValueSerializer extends ObjectSerializer<KeyValue> {

    private static final long serialVersionUID = 1L;

    private final int keyArity;

    private final GenericRowData reusedMeta;
    private final JoinedRowData reusedKeyWithMeta;
    private final JoinedRowData reusedRow;

    private final OffsetRowData reusedKey;
    private final OffsetRowData reusedValue;
    private final KeyValue reusedKv;

    public KeyValueSerializer(RowType keyType, RowType valueType) {
        super(KeyValue.schema(keyType, valueType));

        this.keyArity = keyType.getFieldCount();
        int valueArity = valueType.getFieldCount();

        this.reusedMeta = new GenericRowData(2);
        this.reusedKeyWithMeta = new JoinedRowData();
        this.reusedRow = new JoinedRowData();

        this.reusedKey = new OffsetRowData(keyArity, 0);
        this.reusedValue = new OffsetRowData(valueArity, keyArity + 2);
        this.reusedKv = new KeyValue().replace(reusedKey, -1, null, reusedValue);
    }

    @Override
    public RowData toRow(KeyValue record) {
        return toRow(record.key(), record.sequenceNumber(), record.valueKind(), record.value());
    }

    public RowData toRow(RowData key, long sequenceNumber, RowKind valueKind, RowData value) {
        reusedMeta.setField(0, sequenceNumber);
        reusedMeta.setField(1, valueKind.toByteValue());
        return reusedRow.replace(reusedKeyWithMeta.replace(key, reusedMeta), value);
    }

    @Override
    public KeyValue fromRow(RowData row) {
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
