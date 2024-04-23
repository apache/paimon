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

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.CompactAction;
import org.apache.paimon.flink.action.SortCompactAction;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Collections;
import java.util.Map;

import static org.apache.paimon.utils.ParameterUtils.getPartitions;
import static org.apache.paimon.utils.ParameterUtils.parseCommaSeparatedKeyValues;
import static org.apache.paimon.utils.StringUtils.isBlank;

/** Compact procedure. */
public class CompactProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "compact";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "partitions",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "order_strategy",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(name = "order_by", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "options", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String partitions,
            String orderStrategy,
            String orderByColumns,
            String tableOptions)
            throws Exception {
        String warehouse = ((AbstractCatalog) catalog).warehouse();
        Map<String, String> catalogOptions = ((AbstractCatalog) catalog).options();
        Map<String, String> tableConf =
                isBlank(tableOptions)
                        ? Collections.emptyMap()
                        : parseCommaSeparatedKeyValues(tableOptions);
        Identifier identifier = Identifier.fromString(tableId);
        CompactAction action;
        String jobName;
        if (isBlank(orderStrategy) && isBlank(orderByColumns)) {
            action =
                    new CompactAction(
                            warehouse,
                            identifier.getDatabaseName(),
                            identifier.getObjectName(),
                            catalogOptions,
                            tableConf);
            jobName = "Compact Job";
        } else if (!isBlank(orderStrategy) && !isBlank(orderByColumns)) {
            action =
                    new SortCompactAction(
                                    warehouse,
                                    identifier.getDatabaseName(),
                                    identifier.getObjectName(),
                                    catalogOptions,
                                    tableConf)
                            .withOrderStrategy(orderStrategy)
                            .withOrderColumns(orderByColumns.split(","));
            jobName = "Sort Compact Job";
        } else {
            throw new IllegalArgumentException(
                    "You must specify 'order strategy' and 'order by columns' both.");
        }

        if (!(isBlank(partitions))) {
            action.withPartitions(getPartitions(partitions.split(";")));
        }

        return execute(procedureContext, action, jobName);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
