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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.Map;
import java.util.Optional;

/** Action Factory to create {@link OrderRewriteAction}. */
public class OrderRewriteActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "order-rewrite";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        if (params.has("help")) {
            printHelp();
            return Optional.empty();
        }

        Tuple3<String, String, String> tablePath = getTablePath(params);

        String sqlSelect = getSqlSelect(params);

        Map<String, String> catalogConfig = optionalConfigMap(params, "catalog-conf");

        String sqlOrderBy = getSqlOrderBy(params);

        return Optional.of(
                new OrderRewriteAction(
                        tablePath.f0,
                        tablePath.f1,
                        tablePath.f2,
                        sqlSelect,
                        sqlOrderBy,
                        catalogConfig));
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"order-rewrite\" is similar to sql \"INSERT OVERWRITE target_table SELECT * FROM SOURCE ZORDER BY col1,col2,... \".");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  order-rewrite --warehouse <warehouse-path>\n"
                        + "             --database <database-name>\n"
                        + "             --warehouse <warehouse-path>\n"
                        + "             --table <target-table-name>\n"
                        + "             --sql-select \"sql\"\n"
                        + "             --sql-order-by \"<orderType>(col1,col2,...)\"\n");

        System.out.println("orderType could by: zorder, order.");
        System.out.println(
                "This action is only work for unaware-bucket append-only table, which is append-only with property bucket=-1.");
        System.out.println("You can add WHERE sub-clause in sql-select.");
        System.out.println(
                "The sink-parallelism of the target table will infect the action process time and the result. "
                        + "If you want better aggregation result, you should turn down the sink-parallelism of target table."
                        + "If you want this action finish faster, you should turn up that.");
    }
}
