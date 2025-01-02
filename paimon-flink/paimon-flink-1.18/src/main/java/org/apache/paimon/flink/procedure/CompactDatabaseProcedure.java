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

import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Map;

import static org.apache.paimon.flink.action.ActionFactory.FULL;
import static org.apache.paimon.flink.action.CompactActionFactory.checkCompactStrategy;
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
 *
 * </code></pre>
 */
public class CompactDatabaseProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "compact_database";

    public String[] call(ProcedureContext procedureContext) throws Exception {
        return call(procedureContext, "");
    }

    public String[] call(ProcedureContext procedureContext, String includingDatabases)
            throws Exception {
        return call(procedureContext, includingDatabases, "");
    }

    public String[] call(ProcedureContext procedureContext, String includingDatabases, String mode)
            throws Exception {
        return call(procedureContext, includingDatabases, mode, "");
    }

    public String[] call(
            ProcedureContext procedureContext,
            String includingDatabases,
            String mode,
            String includingTables)
            throws Exception {
        return call(procedureContext, includingDatabases, mode, includingTables, "");
    }

    public String[] call(
            ProcedureContext procedureContext,
            String includingDatabases,
            String mode,
            String includingTables,
            String excludingTables)
            throws Exception {
        return call(
                procedureContext, includingDatabases, mode, includingTables, excludingTables, "");
    }

    public String[] call(
            ProcedureContext procedureContext,
            String includingDatabases,
            String mode,
            String includingTables,
            String excludingTables,
            String tableOptions)
            throws Exception {
        return call(
                procedureContext,
                includingDatabases,
                mode,
                includingTables,
                excludingTables,
                tableOptions,
                "",
                null);
    }

    public String[] call(
            ProcedureContext procedureContext,
            String includingDatabases,
            String mode,
            String includingTables,
            String excludingTables,
            String tableOptions,
            String partitionIdleTime,
            String compactStrategy)
            throws Exception {
        Map<String, String> catalogOptions = catalog.options();
        CompactDatabaseAction action =
                new CompactDatabaseAction(catalogOptions)
                        .includingDatabases(nullable(includingDatabases))
                        .includingTables(nullable(includingTables))
                        .excludingTables(nullable(excludingTables))
                        .withDatabaseCompactMode(nullable(mode));
        if (!StringUtils.isNullOrWhitespaceOnly(tableOptions)) {
            action.withTableOptions(parseCommaSeparatedKeyValues(tableOptions));
        }
        if (!StringUtils.isNullOrWhitespaceOnly(partitionIdleTime)) {
            action.withPartitionIdleTime(TimeUtils.parseDuration(partitionIdleTime));
        }

        if (checkCompactStrategy(compactStrategy)) {
            action.withFullCompaction(compactStrategy.trim().equalsIgnoreCase(FULL));
        }

        return execute(procedureContext, action, "Compact database job");
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
