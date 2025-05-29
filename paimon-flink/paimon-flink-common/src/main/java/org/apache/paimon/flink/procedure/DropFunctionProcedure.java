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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/**
 * drop function procedure. Usage:
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *
 *  CALL sys.drop_function('function_identifier')
 *
 * </code></pre>
 */
public class DropFunctionProcedure extends ProcedureBase {
    @ProcedureHint(argument = {@ArgumentHint(name = "function", type = @DataTypeHint("STRING"))})
    public String[] call(ProcedureContext procedureContext, String function)
            throws Catalog.ViewNotExistException, Catalog.DialectAlreadyExistException,
                    Catalog.DialectNotExistException, Catalog.FunctionAlreadyExistException,
                    Catalog.DatabaseNotExistException {
        Identifier identifier = Identifier.fromString(function);
        try {
            catalog.dropFunction(identifier, false);
        } catch (Catalog.FunctionNotExistException e) {
            return new String[] {"Fail"};
        }
        return new String[] {"Success"};
    }

    @Override
    public String identifier() {
        return "drop_function";
    }
}
