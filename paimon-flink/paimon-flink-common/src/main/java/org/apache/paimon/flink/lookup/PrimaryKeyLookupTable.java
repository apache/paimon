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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.lookup.BulkLoader;
import org.apache.paimon.lookup.RocksDBStateFactory;
import org.apache.paimon.lookup.RocksDBValueState;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyProjectedRow;
import org.apache.paimon.utils.TypeUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/** A {@link LookupTable} for primary key table. */
public class PrimaryKeyLookupTable implements LookupTable {

    protected final RocksDBValueState<InternalRow, InternalRow> tableState;

    protected final Predicate<InternalRow> recordFilter;

    protected int[] primaryKeyMapping;

    protected final KeyProjectedRow primaryKey;
    protected final boolean sequenceFieldEnabled;

    protected final int sequenceIndex;

    public PrimaryKeyLookupTable(
            RocksDBStateFactory stateFactory,
            RowType rowType,
            List<String> primaryKey,
            Predicate<InternalRow> recordFilter,
            long lruCacheSize,
            boolean sequenceFieldEnabled)
            throws IOException {
        List<String> fieldNames = rowType.getFieldNames();
        this.primaryKeyMapping = primaryKey.stream().mapToInt(fieldNames::indexOf).toArray();
        this.primaryKey = new KeyProjectedRow(primaryKeyMapping);
        this.sequenceFieldEnabled = sequenceFieldEnabled;
        // append sequence number at last column when sequence field is attached.
        RowType adjustedRowType = appendSequenceNumber(rowType);
        this.sequenceIndex = adjustedRowType.getFieldCount() - 1;

        this.tableState =
                stateFactory.valueState(
                        "table",
                        InternalSerializers.create(TypeUtils.project(rowType, primaryKeyMapping)),
                        InternalSerializers.create(adjustedRowType),
                        lruCacheSize);
        this.recordFilter = recordFilter;
    }

    public static RowType appendSequenceNumber(RowType rowType) {
        List<DataType> types = rowType.getFieldTypes();
        types.add(DataTypes.BIGINT());
        return RowType.of(types.toArray(new DataType[0]));
    }

    @Override
    public List<InternalRow> get(InternalRow key) throws IOException {
        InternalRow value = tableState.get(key);
        return value == null ? Collections.emptyList() : Collections.singletonList(value);
    }

    @Override
    public void refresh(Iterator<InternalRow> incremental) throws IOException {
        while (incremental.hasNext()) {
            InternalRow row = incremental.next();
            primaryKey.replaceRow(row);
            if (sequenceFieldEnabled) {
                InternalRow previous = tableState.get(primaryKey);
                // only update the kv when the new value's sequence number is higher.
                if (previous != null
                        && previous.getLong(sequenceIndex) > row.getLong(sequenceIndex)) {
                    continue;
                }
            }

            if (row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER) {
                if (recordFilter.test(row)) {
                    tableState.put(primaryKey, row);
                } else {
                    // The new record under primary key is filtered
                    // We need to delete this primary key as it no longer exists.
                    tableState.delete(primaryKey);
                }
            } else {
                tableState.delete(primaryKey);
            }
        }
    }

    @Override
    public Predicate<InternalRow> recordFilter() {
        return recordFilter;
    }

    @Override
    public byte[] toKeyBytes(InternalRow row) throws IOException {
        primaryKey.replaceRow(row);
        return tableState.serializeKey(primaryKey);
    }

    @Override
    public byte[] toValueBytes(InternalRow row) throws IOException {
        return tableState.serializeValue(row);
    }

    @Override
    public TableBulkLoader createBulkLoader() {
        BulkLoader bulkLoader = tableState.createBulkLoader();
        return new TableBulkLoader() {

            @Override
            public void write(byte[] key, byte[] value)
                    throws BulkLoader.WriteException, IOException {
                bulkLoader.write(key, value);
                bulkLoadWritePlus(key, value);
            }

            @Override
            public void finish() {
                bulkLoader.finish();
            }
        };
    }

    public void bulkLoadWritePlus(byte[] key, byte[] value) throws IOException {}
}
