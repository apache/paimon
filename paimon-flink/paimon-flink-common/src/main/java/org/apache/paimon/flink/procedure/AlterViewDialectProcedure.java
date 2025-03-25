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
import org.apache.paimon.view.DialectChange;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/**
 * alter view procedure. Usage:
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *
 *  -- add dialect in the view
 *  CALL sys.alter_view_dialect('viewId', 'add', 'dialect', 'query')
 *
 * </code></pre>
 */
public class AlterViewDialectProcedure extends ProcedureBase {
    @Override
    public String identifier() {
        return "alter_view_dialect";
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "view", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "action", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "dialect", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "query", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String viewId,
            String action,
            String dialect,
            String query)
            throws Catalog.ViewNotExistException, Catalog.DialectAlreadyExistException,
                    Catalog.DialectNotExistException {
        Identifier identifier = Identifier.fromString(viewId);
        DialectChange dialectChange;
        switch (action) {
            case "add":
                {
                    dialectChange = DialectChange.add(dialect, query);
                    break;
                }
            case "update":
                {
                    dialectChange = DialectChange.update(dialect, query);
                    break;
                }
            case "drop":
                {
                    dialectChange = DialectChange.drop(dialect);
                    break;
                }
            default:
                {
                    throw new IllegalArgumentException("Unsupported action: " + action);
                }
        }
        catalog.alterView(identifier, dialectChange, false);
        return new String[] {"Success"};
    }
}
