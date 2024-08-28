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

import org.apache.paimon.flink.action.CompactDatabaseAction;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.utils.TimeUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Map;

import static org.apache.paimon.utils.ParameterUtils.parseCommaSeparatedKeyValues;

/**
 * Compact database procedure. Usage:
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *
 *  -- compact all databases
 *  CALL sys.compact_database()
 *
 *  -- compact some databases (accept regular expression)
 *  CALL sys.compact_database('includingDatabases')
 *
 *  -- set compact mode
 *  CALL sys.compact_database('includingDatabases', 'mode')
 *
 *  -- compact some tables (accept regular expression)
 *  CALL sys.compact_database('includingDatabases', 'mode', 'includingTables')
 *
 *  -- exclude some tables (accept regular expression)
 *  CALL sys.compact_database('includingDatabases', 'mode', 'includingTables', 'excludingTables')
 *
 *  -- set table options ('k=v,...')
 *  CALL sys.compact_database('includingDatabases', 'mode', 'includingTables', 'excludingTables', 'tableOptions')
 * </code></pre>
 */
public class CompactDatabaseProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "compact_database";

    @ProcedureHint(
            argument = {
                @ArgumentHint(
                        name = "including_databases",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(name = "mode", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(
                        name = "including_tables",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "excluding_tables",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "table_options",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "partition_idle_time",
                        type = @DataTypeHint("STRING"),
                        isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String includingDatabases,
            String mode,
            String includingTables,
            String excludingTables,
            String tableOptions,
            String partitionIdleTime)
            throws Exception {
        partitionIdleTime = notnull(partitionIdleTime);
        String warehouse = catalog.warehouse();
        Map<String, String> catalogOptions = catalog.options();
        CompactDatabaseAction action =
                new CompactDatabaseAction(warehouse, catalogOptions)
                        .includingDatabases(nullable(includingDatabases))
                        .includingTables(nullable(includingTables))
                        .excludingTables(nullable(excludingTables))
                        .withDatabaseCompactMode(nullable(mode));
        if (!StringUtils.isBlank(tableOptions)) {
            action.withTableOptions(parseCommaSeparatedKeyValues(tableOptions));
        }
        if (!StringUtils.isBlank(partitionIdleTime)) {
            action.withPartitionIdleTime(TimeUtils.parseDuration(partitionIdleTime));
        }

        return execute(procedureContext, action, "Compact database job");
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
