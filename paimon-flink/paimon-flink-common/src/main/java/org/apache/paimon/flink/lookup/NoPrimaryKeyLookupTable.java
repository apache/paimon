/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.lookup.BulkLoader;
import org.apache.paimon.lookup.RocksDBListState;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.KeyProjectedRow;
import org.apache.paimon.utils.TypeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/** A {@link LookupTable} for table without primary key. */
public class NoPrimaryKeyLookupTable extends FullCacheLookupTable {

    private final RocksDBListState<InternalRow, InternalRow> state;

    private final KeyProjectedRow joinKeyRow;

    public NoPrimaryKeyLookupTable(Context context, long lruCacheSize) throws IOException {
        super(context);
        List<String> fieldNames = projectedType.getFieldNames();
        int[] joinKeyMapping = context.joinKey.stream().mapToInt(fieldNames::indexOf).toArray();
        this.joinKeyRow = new KeyProjectedRow(joinKeyMapping);
        this.state =
                stateFactory.listState(
                        "join-key-index",
                        InternalSerializers.create(
                                TypeUtils.project(projectedType, joinKeyMapping)),
                        InternalSerializers.create(projectedType),
                        lruCacheSize);
    }

    @Override
    public List<InternalRow> innerGet(InternalRow key) throws IOException {
        return state.get(key);
    }

    @Override
    public void refresh(Iterator<InternalRow> incremental, boolean orderByLastField)
            throws IOException {
        if (orderByLastField) {
            throw new IllegalArgumentException(
                    "Append table does not support order by last field.");
        }

        Predicate predicate = projectedPredicate();
        while (incremental.hasNext()) {
            InternalRow row = incremental.next();
            joinKeyRow.replaceRow(row);
            if (row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER) {
                if (predicate == null || predicate.test(row)) {
                    state.add(joinKeyRow, row);
                }
            } else {
                throw new RuntimeException(
                        String.format(
                                "Received %s message. Only INSERT/UPDATE_AFTER values are expected here.",
                                row.getRowKind()));
            }
        }
    }

    @Override
    public byte[] toKeyBytes(InternalRow row) throws IOException {
        joinKeyRow.replaceRow(row);
        return state.serializeKey(joinKeyRow);
    }

    @Override
    public byte[] toValueBytes(InternalRow row) throws IOException {
        return state.serializeValue(row);
    }

    @Override
    public TableBulkLoader createBulkLoader() {
        BulkLoader bulkLoader = state.createBulkLoader();
        return new TableBulkLoader() {

            private final List<byte[]> values = new ArrayList<>();

            private byte[] currentKey;

            @Override
            public void write(byte[] key, byte[] value) throws IOException {
                if (currentKey != null && !Arrays.equals(key, currentKey)) {
                    flush();
                }
                currentKey = key;
                values.add(value);
            }

            @Override
            public void finish() throws IOException {
                flush();
                bulkLoader.finish();
            }

            private void flush() throws IOException {
                if (currentKey != null && values.size() > 0) {
                    try {
                        bulkLoader.write(currentKey, state.serializeList(values));
                    } catch (BulkLoader.WriteException e) {
                        throw new RuntimeException(e);
                    }
                }

                currentKey = null;
                values.clear();
            }
        };
    }
}
