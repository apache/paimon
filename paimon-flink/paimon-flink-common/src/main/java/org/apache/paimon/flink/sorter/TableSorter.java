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

import org.apache.paimon.flink.action.OrderRewriteAction;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;

import java.util.List;
import java.util.stream.Collectors;

/** An abstract TableSorter for {@link OrderRewriteAction}. */
public abstract class TableSorter {

    protected final StreamTableEnvironment batchTEnv;
    protected final Table origin;
    protected final List<String> orderColNames;

    public TableSorter(StreamTableEnvironment batchTEnv, Table origin, List<String> orderColNames) {
        this.batchTEnv = batchTEnv;
        this.origin = origin;
        this.orderColNames = orderColNames;
        checkColNames();
    }

    private void checkColNames() {
        if (orderColNames.size() < 1) {
            throw new IllegalArgumentException("order column names should not be empty.");
        }
        List<String> columnNames =
                origin.getResolvedSchema().getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
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

    public abstract Table sort();
}
