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
import org.apache.flink.table.store.codegen.CodeGenUtils;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.ObjectSerializer;
import org.apache.flink.table.store.file.utils.OffsetRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.stream.IntStream;

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

    private TableSchema tableSchema;

    public KeyValueSerializer(RowType keyType, RowType valueType, TableSchema tableSchema) {
        this(keyType, valueType);
        this.tableSchema = tableSchema;
    }

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
        RowData keyRow = key;
        if (keyRow == null) {
            keyRow = new GenericRowData(1);
            keyRow.setRowKind(valueKind);
        }
        return reusedRow.replace(reusedKeyWithMeta.replace(keyRow, reusedMeta), value);
    }

    @Override
    public KeyValue fromRow(RowData row) {
        reusedValue.replace(row);
        long sequenceNumber = row.getLong(keyArity);
        RowKind valueKind = RowKind.fromByteValue(row.getByte(keyArity + 1));
        if (IntStream.range(0, keyArity).allMatch(row::isNullAt)) {
            reusedKey.replace(
                    CodeGenUtils.newProjection(
                                    tableSchema.logicalRowType(),
                                    tableSchema.projection(tableSchema.trimmedPrimaryKeys()))
                            .apply(reusedValue));
        } else {
            reusedKey.replace(row);
        }
        reusedKv.replace(reusedKey, sequenceNumber, valueKind, reusedValue);
        return reusedKv;
    }

    public KeyValue getReusedKv() {
        return reusedKv;
    }
}
