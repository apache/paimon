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

package org.apache.paimon.flink.lakestream;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The in-memory "storage" of the fake lake-stream store. Rows written through {@link FakeStoreSink}
 * are appended here per store table (keyed by {@code database.table}), and {@link FakeStoreSource}
 * reads them back, so tests can assert end-to-end routing without any external store.
 *
 * <p>The fake store schema is fixed to a single {@code INT} column, so only single-int rows are
 * supported.
 */
public class FakeStoreData {

    private static final Map<String, List<RowData>> ROWS = new HashMap<>();

    private FakeStoreData() {}

    public static synchronized void add(String tableKey, RowData row) {
        // copy defensively: the runtime may reuse row instances between records
        GenericRowData copy = new GenericRowData(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            copy.setField(i, row.getInt(i));
        }
        List<RowData> rows = ROWS.get(tableKey);
        if (rows == null) {
            rows = new ArrayList<>();
            ROWS.put(tableKey, rows);
        }
        rows.add(copy);
    }

    public static synchronized List<RowData> rows(String tableKey) {
        List<RowData> rows = ROWS.get(tableKey);
        return rows == null ? new ArrayList<RowData>() : new ArrayList<>(rows);
    }

    public static synchronized void reset() {
        ROWS.clear();
    }
}
