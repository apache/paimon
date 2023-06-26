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
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyProjectedRow;
import org.apache.paimon.utils.TypeUtils;
import org.apache.paimon.utils.ValueEqualiserSupplier;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/** A {@link LookupTable} for table without primary key. */
public class NoPrimaryKeyLookupTable implements LookupTable {

    private final RocksDBListState state;

    private final Predicate<InternalRow> recordFilter;

    private final KeyProjectedRow joinKeyRow;

    public NoPrimaryKeyLookupTable(
            RocksDBStateFactory stateFactory,
            RowType rowType,
            List<String> joinKeys,
            Predicate<InternalRow> recordFilter,
            long lruCacheSize)
            throws IOException {
        List<String> fieldNames = rowType.getFieldNames();
        int[] joinKeyMapping = joinKeys.stream().mapToInt(fieldNames::indexOf).toArray();
        this.joinKeyRow = new KeyProjectedRow(joinKeyMapping);
        this.state =
                stateFactory.listState(
                        "join-key-index",
                        InternalSerializers.create(TypeUtils.project(rowType, joinKeyMapping)),
                        InternalSerializers.create(rowType),
                        new ValueEqualiserSupplier(rowType).get(),
                        lruCacheSize);
        this.recordFilter = recordFilter;
    }

    @Override
    public List<InternalRow> get(InternalRow key) throws IOException {
        return state.get(key);
    }

    @Override
    public void refresh(Iterator<InternalRow> incremental) throws IOException {
        while (incremental.hasNext()) {
            InternalRow row = incremental.next();
            joinKeyRow.replaceRow(row);
            if (row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER) {
                if (recordFilter.test(row)) {
                    state.add(joinKeyRow, row);
                }
            } else {
                state.retract(joinKeyRow, row);
            }
        }
    }
}
