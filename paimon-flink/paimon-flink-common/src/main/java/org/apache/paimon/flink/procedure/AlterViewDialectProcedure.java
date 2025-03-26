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
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.view.ViewChange;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import static org.apache.paimon.flink.FlinkCatalog.DIALECT;

/**
 * alter view procedure. Usage:
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *
 *  -- add dialect in the view
 *  CALL sys.alter_view_dialect('view_identifier', 'add', 'flink', 'query')
 *  CALL sys.alter_view_dialect(`view` => 'view_identifier', `action` => 'add', `query` => 'query')
 *
 *  -- update dialect in the view
 *  CALL sys.alter_view_dialect('view_identifier', 'update', 'flink', 'query')
 *  CALL sys.alter_view_dialect(`view` => 'view_identifier', `action` => 'update', `query` => 'query')
 *
 *  -- drop dialect in the view
 *  CALL sys.alter_view_dialect('view_identifier', 'drop', 'flink')
 *  CALL sys.alter_view_dialect(`view` => 'view_identifier', `action` => 'drop')
 *
 * </code></pre>
 */
public class AlterViewDialectProcedure extends ProcedureBase {
    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "view", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "action", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "engine", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "query", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String view,
            String action,
            String engine,
            String query)
            throws Catalog.ViewNotExistException, Catalog.DialectAlreadyExistException,
                    Catalog.DialectNotExistException {
        Identifier identifier = Identifier.fromString(view);
        ViewChange viewChange;
        String dialect = StringUtils.isNullOrWhitespaceOnly(engine) ? DIALECT : engine;
        switch (action) {
            case "add":
                {
                    if (StringUtils.isNullOrWhitespaceOnly(query)) {
                        throw new IllegalArgumentException("query is required for add action.");
                    }
                    viewChange = ViewChange.addDialect(dialect, query);
                    break;
                }
            case "update":
                {
                    if (StringUtils.isNullOrWhitespaceOnly(query)) {
                        throw new IllegalArgumentException("query is required for update action.");
                    }
                    viewChange = ViewChange.updateDialect(dialect, query);
                    break;
                }
            case "drop":
                {
                    viewChange = ViewChange.dropDialect(dialect);
                    break;
                }
            default:
                {
                    throw new IllegalArgumentException("Unsupported action: " + action);
                }
        }
        catalog.alterView(identifier, ImmutableList.of(viewChange), false);
        return new String[] {"Success"};
    }

    @Override
    public String identifier() {
        return "alter_view_dialect";
    }
}
