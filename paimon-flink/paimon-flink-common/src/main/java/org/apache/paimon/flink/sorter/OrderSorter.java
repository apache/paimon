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

import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Order Sorter to sort the table of origin. Table will be ordered by given columns.
 *
 * <p>The effect is similar to sql: "INSERT OVERWRITE target_table SELECT * FROM SOURCE ORDER BY
 * col1,col2,... "
 */
public class OrderSorter extends TableSorter {

    public OrderSorter(StreamTableEnvironment batchTEnv, Table origin, List<String> orderColNames) {
        super(batchTEnv, origin, orderColNames);
    }

    @Override
    public Table sort() {
        ApiExpression e = $(orderColNames.get(0));
        for (int i = 1; i < orderColNames.size(); i++) {
            e.and($(orderColNames.get(i)));
        }
        return origin.orderBy(e);
    }
}
