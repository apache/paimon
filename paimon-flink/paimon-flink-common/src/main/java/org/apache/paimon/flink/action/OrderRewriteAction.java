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

package org.apache.paimon.flink.action;

import org.apache.paimon.flink.sorter.SorterFactory;
import org.apache.paimon.flink.sorter.TableSorter;

import org.apache.flink.table.api.Table;

import java.util.Map;

/**
 * Rewrite the target table by z-order with specified columns.
 *
 * <p>The effect is similar to sql: "INSERT OVERWRITE target_table SELECT * FROM SOURCE ZORDER BY
 * col1,col2,... "
 *
 * <p>Example usage: order-rewrite --warehouse /tmp/paimon/warehouse --database my_db --table
 * Orders1 --sql-select "SELECT * FROM my_db.Orders1 WHERE f0 < 10" --sql-order-by
 * "order(f0,f1,f2,f3,f4,f7,f8,f9,f10,f11,f12,f13,f14,f15)"
 */
public class OrderRewriteAction extends FlinkActionEnvironmentBase {

    private final String sqlSelect;
    private final String sqlOrderBy;

    OrderRewriteAction(
            String warehouse,
            String databaseName,
            String tableName,
            String sqlSelect,
            String sqlOrderBy,
            Map<String, String> catalogConfig) {
        super(warehouse, databaseName, tableName, catalogConfig);
        this.sqlSelect = sqlSelect;
        this.sqlOrderBy = sqlOrderBy;
    }

    @Override
    public void run() throws Exception {
        Table origin = batchTEnv.sqlQuery(sqlSelect);
        TableSorter sorter = SorterFactory.getSorter(batchTEnv, origin, sqlOrderBy);
        Table sorted = sorter.sort();
        sorted.executeInsert(identifier.getFullName(), true).await();
    }
}
