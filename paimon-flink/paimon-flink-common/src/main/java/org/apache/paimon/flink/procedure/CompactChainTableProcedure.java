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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Map;

/**
 * Procedure to compact chain table. Usage:
 *
 * <pre><code>
 *  -- Compact chain table, overwrite default is false
 *  CALL sys.compact_chain_table('db.table', 'dt=20250810,hour=22', [true])
 * </code></pre>
 */
public class CompactChainTableProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "compact_chain_table";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "partition", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "overwrite",
                        type = @DataTypeHint("BOOLEAN"),
                        isOptional = true)
            })
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
