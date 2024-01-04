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
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyProjectedRow;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.TypeUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/** A {@link LookupTable} for primary key table. */
public class PrimaryKeyLookupTable implements LookupTable {

    protected final RocksDBValueState<InternalRow, InternalRow> tableState;

    protected final RowType rowType;

    protected final Predicate<InternalRow> recordFilter;

    protected int[] primaryKeyMapping;

    protected final KeyProjectedRow primaryKey;

    protected final Projection valueProjection;

    public PrimaryKeyLookupTable(
            RocksDBStateFactory stateFactory,
            RowType rowType,
            List<String> primaryKey,
            Predicate<InternalRow> recordFilter,
            long lruCacheSize,
            Projection valueProjection)
            throws IOException {
        this.rowType = rowType;
        List<String> fieldNames = rowType.getFieldNames();
        this.primaryKeyMapping = primaryKey.stream().mapToInt(fieldNames::indexOf).toArray();
        this.primaryKey = new KeyProjectedRow(primaryKeyMapping);

        this.tableState =
                stateFactory.valueState(
                        "table",
                        InternalSerializers.create(TypeUtils.project(rowType, primaryKeyMapping)),
                        InternalSerializers.create(rowType),
                        lruCacheSize);
        this.recordFilter = recordFilter;
        this.valueProjection = valueProjection;
    }

    @Override
    public List<InternalRow> get(InternalRow key) throws IOException {
        InternalRow value = tableState.get(key);
        return value == null
                ? Collections.emptyList()
                : Collections.singletonList(ProjectedRow.from(valueProjection).replaceRow(value));
    }

    @Override
    public void refresh(Iterator<InternalRow> incremental, boolean orderByLastField)
            throws IOException {
        while (incremental.hasNext()) {
            InternalRow row = incremental.next();
            primaryKey.replaceRow(row);
            if (orderByLastField) {
                InternalRow previous = tableState.get(primaryKey);
                int orderIndex = rowType.getFieldCount() - 1;
                if (previous != null && previous.getLong(orderIndex) > row.getLong(orderIndex)) {
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
