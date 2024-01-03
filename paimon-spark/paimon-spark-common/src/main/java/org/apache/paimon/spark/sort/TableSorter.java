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

package org.apache.paimon.spark.sort;

import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/** Abstract sorter for sorting table. see {@link ZorderSorter} and {@link OrderSorter}. */
public abstract class TableSorter {

    protected final FileStoreTable table;
    protected final List<String> orderColNames;

    public TableSorter(FileStoreTable table, List<String> orderColNames) {
        this.table = table;
        this.orderColNames = orderColNames;
        checkColNames();
    }

    protected void checkNotEmpty() {
        if (orderColNames.isEmpty()) {
            throw new IllegalArgumentException("order column could not be empty");
        }
    }

    private void checkColNames() {
        if (!orderColNames.isEmpty()) {
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
    }

    public abstract Dataset<Row> sort(Dataset<Row> input);

    public static TableSorter getSorter(
            FileStoreTable table, TableSorter.OrderType orderType, List<String> orderColumns) {
        switch (orderType) {
            case ORDER:
                return new OrderSorter(table, orderColumns);
            case ZORDER:
                return new ZorderSorter(table, orderColumns);
            case HILBERT:
                return new HilbertSorter(table, orderColumns);
            case NONE:
                return new TableSorter(table, orderColumns) {
                    @Override
                    public Dataset<Row> sort(Dataset<Row> input) {
                        return input;
                    }
                };
            default:
                throw new IllegalArgumentException("cannot match order type: " + orderType);
        }
    }

    /** order type for sorting. */
    public enum OrderType {
        ORDER("order"),
        ZORDER("zorder"),
        HILBERT("hilbert"),
        NONE("none");

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
            } else if (NONE.orderType.equalsIgnoreCase(orderType)) {
                return NONE;
            }

            throw new IllegalArgumentException("cannot match type: " + orderType + " for ordering");
        }
    }
}
