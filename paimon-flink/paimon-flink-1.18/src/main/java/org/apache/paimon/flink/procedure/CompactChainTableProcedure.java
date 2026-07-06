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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.CompactChainTableAction;

import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Map;

/**
 * Compact database procedure. Usage:
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *
 *  -- compact chain table (tableId should be 'database_name.table_name')
 *  CALL sys.compact_chain_table('tableId', 'partition')
 *
 *  -- compact chain table with overwrite
 *  CALL sys.compact_chain_table('tableId', 'partition', true)
 *
 * </code></pre>
 */
public class CompactChainTableProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "compact_chain_table";

    public String[] call(ProcedureContext procedureContext, String tableId, String partition)
            throws Exception {
        return call(procedureContext, tableId, partition, null);
    }

    public String[] call(
            ProcedureContext procedureContext, String tableId, String partition, Boolean overwrite)
            throws Exception {
        Map<String, String> catalogOptions = catalog.options();
        Identifier identifier = Identifier.fromString(tableId);
        CompactChainTableAction action =
                new CompactChainTableAction(
                        identifier.getDatabaseName(),
                        identifier.getObjectName(),
                        catalogOptions,
                        partition,
                        overwrite);
        return execute(
                procedureContext, action, "Compact chain table Job : " + identifier.getFullName());
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
