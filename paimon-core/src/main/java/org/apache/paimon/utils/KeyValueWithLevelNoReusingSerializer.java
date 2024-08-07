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

package org.apache.paimon.utils;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import static org.apache.paimon.data.JoinedRow.join;

/** Serializer for {@link KeyValue} with Level. */
public class KeyValueWithLevelNoReusingSerializer extends ObjectSerializer<KeyValue> {

    private static final long serialVersionUID = 1L;

    private final int keyArity;
    private final int valueArity;

    public KeyValueWithLevelNoReusingSerializer(RowType keyType, RowType valueType) {
        super(KeyValue.schemaWithLevel(keyType, valueType));

        this.keyArity = keyType.getFieldCount();
        this.valueArity = valueType.getFieldCount();
    }

    @Override
    public InternalRow toRow(KeyValue kv) {
        GenericRow meta = GenericRow.of(kv.sequenceNumber(), kv.valueKind().toByteValue());
        return join(join(join(kv.key(), meta), kv.value()), GenericRow.of(kv.level()));
    }

    @Override
    public KeyValue fromRow(InternalRow row) {
        return new KeyValue()
                .replace(
                        new OffsetRow(keyArity, 0).replace(row),
                        row.getLong(keyArity),
                        RowKind.fromByteValue(row.getByte(keyArity + 1)),
                        new OffsetRow(valueArity, keyArity + 2).replace(row))
                .setLevel(row.getInt(keyArity + 2 + valueArity));
    }
}
