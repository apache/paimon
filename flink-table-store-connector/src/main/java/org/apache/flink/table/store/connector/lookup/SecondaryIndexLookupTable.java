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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/** A {@link LookupTable} for primary key table which provides lookup by secondary key. */
public class SecondaryIndexLookupTable extends PrimaryKeyLookupTable {

    private final RocksDBSetState indexState;

    private final KeyProjectedRowData secKeyRow;

    public SecondaryIndexLookupTable(
            RocksDBStateFactory stateFactory,
            RowType rowType,
            List<String> primaryKey,
            List<String> secKey,
            Predicate<RowData> recordFilter,
            long lruCacheSize)
            throws IOException {
        super(stateFactory, rowType, primaryKey, recordFilter, lruCacheSize / 2);
        List<String> fieldNames = rowType.getFieldNames();
        int[] secKeyMapping = secKey.stream().mapToInt(fieldNames::indexOf).toArray();
        this.secKeyRow = new KeyProjectedRowData(secKeyMapping);
        this.indexState =
                stateFactory.setState(
                        "sec-index",
                        InternalSerializers.create(TypeUtils.project(rowType, secKeyMapping)),
                        InternalSerializers.create(TypeUtils.project(rowType, primaryKeyMapping)),
                        lruCacheSize / 2);
    }

    @Override
    public List<RowData> get(RowData key) throws IOException {
        List<RowData> pks = indexState.get(key);
        List<RowData> values = new ArrayList<>(pks.size());
        for (RowData pk : pks) {
            RowData value = tableState.get(pk);
            if (value != null) {
                values.add(value);
            }
        }
        return values;
    }

    @Override
    public void refresh(Iterator<RowData> incremental) throws IOException {
        while (incremental.hasNext()) {
            RowData row = incremental.next();
            primaryKey.replaceRow(row);
            if (row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER) {
                RowData previous = tableState.get(primaryKey);
                if (previous != null) {
                    indexState.retract(secKeyRow.replaceRow(previous), primaryKey);
                }

                if (recordFilter.test(row)) {
                    tableState.put(primaryKey, row);
                    indexState.add(secKeyRow.replaceRow(row), primaryKey);
                } else {
                    tableState.delete(primaryKey);
                }
            } else {
                tableState.delete(primaryKey);
                indexState.retract(secKeyRow.replaceRow(row), primaryKey);
            }
        }
    }
}
