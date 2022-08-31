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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.IOException;

/** Rocksdb state for key value. */
public abstract class RocksDBState {

    protected final RocksDB db;

    protected final WriteOptions writeOptions;

    protected final ColumnFamilyHandle columnFamily;

    protected final TypeSerializer<RowData> keySerializer;

    protected final TypeSerializer<RowData> valueSerializer;

    protected final DataOutputSerializer keyOutView;

    protected final DataInputDeserializer valueInputView;

    protected final DataOutputSerializer valueOutputView;

    public RocksDBState(
            RocksDB db,
            ColumnFamilyHandle columnFamily,
            TypeSerializer<RowData> keySerializer,
            TypeSerializer<RowData> valueSerializer) {
        this.db = db;
        this.columnFamily = columnFamily;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyOutView = new DataOutputSerializer(32);
        this.valueInputView = new DataInputDeserializer();
        this.valueOutputView = new DataOutputSerializer(32);
        this.writeOptions = new WriteOptions().setDisableWAL(true);
    }

    protected byte[] serializeKey(RowData key) throws IOException {
        keyOutView.clear();
        keySerializer.serialize(key, keyOutView);
        return keyOutView.getCopyOfBuffer();
    }
}
