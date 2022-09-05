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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.store.utils.KeyProjectedRowData;
import org.apache.flink.table.store.utils.TypeUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/** A {@link LookupTable} for primary key table. */
public class PrimaryKeyLookupTable implements LookupTable {

    protected final RocksDBValueState tableState;

    protected final Predicate<RowData> recordFilter;

    protected int[] primaryKeyMapping;

    protected final KeyProjectedRowData primaryKey;

    public PrimaryKeyLookupTable(
            RocksDBStateFactory stateFactory,
            RowType rowType,
            List<String> primaryKey,
            Predicate<RowData> recordFilter,
            long lruCacheSize)
            throws IOException {
        List<String> fieldNames = rowType.getFieldNames();
        this.primaryKeyMapping = primaryKey.stream().mapToInt(fieldNames::indexOf).toArray();
        this.primaryKey = new KeyProjectedRowData(primaryKeyMapping);
        this.tableState =
                stateFactory.valueState(
                        "table",
                        InternalSerializers.create(TypeUtils.project(rowType, primaryKeyMapping)),
                        InternalSerializers.create(rowType),
                        lruCacheSize);
        this.recordFilter = recordFilter;
    }

    @Override
    public List<RowData> get(RowData key) throws IOException {
        RowData value = tableState.get(key);
        return value == null ? Collections.emptyList() : Collections.singletonList(value);
    }

    @Override
    public void refresh(Iterator<RowData> incremental) throws IOException {
        while (incremental.hasNext()) {
            RowData row = incremental.next();
            primaryKey.replaceRow(row);
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
}
