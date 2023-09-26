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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.CompactAction;
import org.apache.paimon.flink.action.SortCompactAction;

import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Compact procedure. Usage:
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *
 *  -- compact a table (tableId should be 'database_name.table_name')
 *  CALL compact('tableId')
 *
 *  -- compact a table with sorting
 *  CALL compact('tableId', 'orderStrategy', 'orderByColumns')
 *
 *  -- compact specific partitions ('pt1=A,pt2=a', 'pt1=B,pt2=b', ...)
 *  CALL compact('tableId', '', '', partition1, partition2, ...)
 * </code></pre>
 */
public class CompactProcedure extends ProcedureBase {

    public static final String NAME = "compact";

    public CompactProcedure(Catalog catalog) {
        super(catalog);
    }

    public String[] call(ProcedureContext procedureContext, String tableId) throws Exception {
        return call(procedureContext, tableId, "", "");
    }

    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String orderStrategy,
            String orderByColumns)
            throws Exception {
        return call(procedureContext, tableId, orderStrategy, orderByColumns, new String[0]);
    }

    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String orderStrategy,
            String orderByColumns,
            String... partitionStrings)
            throws Exception {
        Identifier identifier = Identifier.fromString(tableId);
        CompactAction action;
        String jobName;
        if (orderStrategy.isEmpty() && orderByColumns.isEmpty()) {
            action =
                    new CompactAction(
                            identifier.getDatabaseName(), identifier.getObjectName(), catalog);
            jobName = "Compact Job";
        } else if (!orderStrategy.isEmpty() && !orderByColumns.isEmpty()) {
            action =
                    new SortCompactAction(
                                    identifier.getDatabaseName(),
                                    identifier.getObjectName(),
                                    catalog)
                            .withOrderStrategy(orderStrategy)
                            .withOrderColumns(orderByColumns.split(","));
            jobName = "Sort Compact Job";
        } else {
            throw new IllegalArgumentException(
                    "You must specify 'order strategy' and 'order by columns' both.");
        }

        if (partitionStrings.length != 0) {
            action.withPartitions(getPartitions(partitionStrings));
        }

        return execute(procedureContext, action, jobName);
    }
}
