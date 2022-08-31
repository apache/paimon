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
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Rocksdb state for key -> Set values. */
public class RocksDBSetState extends RocksDBState {

    private static final byte[] EMPTY = new byte[0];

    public RocksDBSetState(
            RocksDB db,
            ColumnFamilyHandle columnFamily,
            TypeSerializer<RowData> keySerializer,
            TypeSerializer<RowData> valueSerializer) {
        super(db, columnFamily, keySerializer, valueSerializer);
    }

    public List<RowData> get(RowData key) throws IOException {
        try (RocksIterator iterator = db.newIterator(columnFamily)) {
            byte[] keyBytes = serializeKey(key);
            iterator.seek(keyBytes);

            List<RowData> values = new ArrayList<>();
            while (iterator.isValid() && startWithKeyPrefix(keyBytes, iterator.key())) {
                byte[] rawKeyBytes = iterator.key();
                valueInputView.setBuffer(
                        rawKeyBytes, keyBytes.length, rawKeyBytes.length - keyBytes.length);
                values.add(valueSerializer.deserialize(valueInputView));

                iterator.next();
            }
            return values;
        }
    }

    public void retract(RowData key, RowData value) throws IOException {
        checkArgument(value != null);

        byte[] bytes = serializeKeyAndValue(key, value);
        try {
            if (db.get(columnFamily, bytes) != null) {
                db.delete(columnFamily, writeOptions, bytes);
            }
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    public void add(RowData key, RowData value) throws IOException {
        checkArgument(value != null);

        try {
            db.put(columnFamily, writeOptions, serializeKeyAndValue(key, value), EMPTY);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    private byte[] serializeKeyAndValue(RowData key, RowData value) throws IOException {
        keyOutView.clear();
        keySerializer.serialize(key, keyOutView);
        valueSerializer.serialize(value, keyOutView);
        return keyOutView.getCopyOfBuffer();
    }

    private boolean startWithKeyPrefix(byte[] keyPrefixBytes, byte[] rawKeyBytes) {
        if (rawKeyBytes.length < keyPrefixBytes.length) {
            return false;
        }

        for (int i = keyPrefixBytes.length; --i >= 0; ) {
            if (rawKeyBytes[i] != keyPrefixBytes[i]) {
                return false;
            }
        }

        return true;
    }
}
