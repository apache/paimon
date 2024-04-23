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

package org.apache.paimon.flink.sorter;

import org.apache.paimon.flink.action.SortCompactAction;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import java.util.List;

/** An abstract TableSorter for {@link SortCompactAction}. */
public abstract class TableSorter {

    protected final StreamExecutionEnvironment batchTEnv;
    protected final DataStream<RowData> origin;
    protected final FileStoreTable table;
    protected final List<String> orderColNames;

    public TableSorter(
            StreamExecutionEnvironment batchTEnv,
            DataStream<RowData> origin,
            FileStoreTable table,
            List<String> orderColNames) {
        this.batchTEnv = batchTEnv;
        this.origin = origin;
        this.table = table;
        this.orderColNames = orderColNames;
        checkColNames();
    }

    private void checkColNames() {
        if (orderColNames.size() < 1) {
            throw new IllegalArgumentException("order column names should not be empty.");
        }
        List<String> columnNames = table.rowType().getFieldNames();
        for (String zColumn : orderColNames) {
            if (!columnNames.contains(zColumn)) {
                throw new RuntimeException(
                        "Can't find column "
                                + zColumn
                                + " in table columns. Possible columns are ["
                                + columnNames.stream().reduce((a, b) -> a + "," + b).get()
                                + "]");
            }
        }
    }

    public abstract DataStream<RowData> sort();

    public static TableSorter getSorter(
            StreamExecutionEnvironment batchTEnv,
            DataStream<RowData> origin,
            FileStoreTable fileStoreTable,
            String sortStrategy,
            List<String> orderColumns) {
        switch (OrderType.of(sortStrategy)) {
            case ORDER:
                return new OrderSorter(batchTEnv, origin, fileStoreTable, orderColumns);
            case ZORDER:
                return new ZorderSorter(batchTEnv, origin, fileStoreTable, orderColumns);
            case HILBERT:
                return new HilbertSorter(batchTEnv, origin, fileStoreTable, orderColumns);
            default:
                throw new IllegalArgumentException("cannot match order type: " + sortStrategy);
        }
    }

    enum OrderType {
        ORDER("order"),
        ZORDER("zorder"),
        HILBERT("hilbert");

        private final String orderType;

        OrderType(String orderType) {
            this.orderType = orderType;
        }

        @Override
        public String toString() {
            return "order type: " + orderType;
        }

        public static OrderType of(String orderType) {
            if (ORDER.orderType.equalsIgnoreCase(orderType)) {
                return ORDER;
            } else if (ZORDER.orderType.equalsIgnoreCase(orderType)) {
                return ZORDER;
            } else if (HILBERT.orderType.equalsIgnoreCase(orderType)) {
                return HILBERT;
            }

            throw new IllegalArgumentException("cannot match type: " + orderType + " for ordering");
        }
    }
}
