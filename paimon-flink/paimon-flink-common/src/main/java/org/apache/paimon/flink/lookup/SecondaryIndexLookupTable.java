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
import org.apache.paimon.lookup.RocksDBSetState;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.KeyProjectedRow;
import org.apache.paimon.utils.TypeUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** A {@link LookupTable} for primary key table which provides lookup by secondary key. */
public class SecondaryIndexLookupTable extends PrimaryKeyLookupTable {

    private final KeyProjectedRow secKeyRow;

    private RocksDBSetState<InternalRow, InternalRow> indexState;

    public SecondaryIndexLookupTable(Context context, long lruCacheSize) {
        super(context, lruCacheSize / 2, context.table.primaryKeys());
        List<String> fieldNames = projectedType.getFieldNames();
        int[] secKeyMapping = context.joinKey.stream().mapToInt(fieldNames::indexOf).toArray();
        this.secKeyRow = new KeyProjectedRow(secKeyMapping);
    }

    @Override
    public void open() throws Exception {
        openStateFactory();
        createTableState();
        this.indexState =
                stateFactory.setState(
                        "sec-index",
                        InternalSerializers.create(
                                TypeUtils.project(projectedType, secKeyRow.indexMapping())),
                        InternalSerializers.create(
                                TypeUtils.project(projectedType, primaryKeyRow.indexMapping())),
                        lruCacheSize);
        bootstrap();
    }

    @Override
    public List<InternalRow> innerGet(InternalRow key) throws IOException {
        List<InternalRow> pks = indexState.get(key);
        List<InternalRow> values = new ArrayList<>(pks.size());
        for (InternalRow pk : pks) {
            InternalRow row = tableState.get(pk);
            if (row != null) {
                values.add(row);
            }
        }
        return values;
    }

    @Override
    public void refresh(Iterator<InternalRow> incremental) throws IOException {
        Predicate predicate = projectedPredicate();
        while (incremental.hasNext()) {
            InternalRow row = incremental.next();
            primaryKeyRow.replaceRow(row);

            boolean previousFetched = false;
            InternalRow previous = null;
            if (userDefinedSeqComparator != null) {
                previous = tableState.get(primaryKeyRow);
                previousFetched = true;
                if (previous != null && userDefinedSeqComparator.compare(previous, row) > 0) {
                    continue;
                }
            }

            if (row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER) {
                if (!previousFetched) {
                    previous = tableState.get(primaryKeyRow);
                }
                if (previous != null) {
                    indexState.retract(secKeyRow.replaceRow(previous), primaryKeyRow);
                }

                if (predicate == null || predicate.test(row)) {
                    tableState.put(primaryKeyRow, row);
                    indexState.add(secKeyRow.replaceRow(row), primaryKeyRow);
                } else {
                    tableState.delete(primaryKeyRow);
                }
            } else {
                tableState.delete(primaryKeyRow);
                indexState.retract(secKeyRow.replaceRow(row), primaryKeyRow);
            }
        }
    }

    @Override
    public void bulkLoadWritePlus(byte[] key, byte[] value) throws IOException {
        InternalRow row = tableState.deserializeValue(value);
        indexState.add(secKeyRow.replaceRow(row), primaryKeyRow.replaceRow(row));
    }
}
