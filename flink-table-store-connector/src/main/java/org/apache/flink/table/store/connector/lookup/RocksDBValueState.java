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

package org.apache.flink.table.store.connector.lookup;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Rocksdb state for key -> a single value. */
public class RocksDBValueState extends RocksDBState {

    public RocksDBValueState(
            RocksDB db,
            ColumnFamilyHandle columnFamily,
            TypeSerializer<RowData> keySerializer,
            TypeSerializer<RowData> valueSerializer) {
        super(db, columnFamily, keySerializer, valueSerializer);
    }

    @Nullable
    public RowData get(RowData key) throws IOException {
        byte[] valueBytes;
        try {
            valueBytes = db.get(columnFamily, serializeKey(key));
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
        if (valueBytes == null) {
            return null;
        }

        return deserializeValue(valueBytes);
    }

    public void put(RowData key, RowData value) throws IOException {
        checkArgument(value != null);

        try {
            db.put(columnFamily, writeOptions, serializeKey(key), serializeValue(value));
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    public void delete(RowData key) throws IOException {
        try {
            byte[] keyBytes = serializeKey(key);
            if (db.get(columnFamily, keyBytes) != null) {
                db.delete(columnFamily, writeOptions, keyBytes);
            }
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    private RowData deserializeValue(byte[] valueBytes) throws IOException {
        valueInputView.setBuffer(valueBytes);
        return valueSerializer.deserialize(valueInputView);
    }

    private byte[] serializeValue(RowData value) throws IOException {
        valueOutputView.clear();
        valueSerializer.serialize(value, valueOutputView);
        return valueOutputView.getCopyOfBuffer();
    }
}
