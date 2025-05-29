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
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/**
 * alter function procedure. Usage:
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *
 *  CALL sys.alter_function('function_identifier', '{"action" : "addDefinition", "name" : "flink", "definition" : {"type" : "file", "fileResources" : [{"resourceType": "JAR", "uri": "oss://mybucket/xxxx.jar"}], "language": "JAVA", "className": "xxxx", "functionName": "functionName" } }')
 *
 * </code></pre>
 */
public class AlterFunctionProcedure extends ProcedureBase {
    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "function", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "change", type = @DataTypeHint("STRING")),
            })
    public String[] call(ProcedureContext procedureContext, String function, String change)
            throws Catalog.FunctionNotExistException, Catalog.DefinitionAlreadyExistException,
                    Catalog.DefinitionNotExistException {
        Identifier identifier = Identifier.fromString(function);
        FunctionChange functionChange = JsonSerdeUtil.fromJson(change, FunctionChange.class);
        catalog.alterFunction(identifier, ImmutableList.of(functionChange), false);
        return new String[] {"Success"};
    }

    @Override
    public String identifier() {
        return "alter_function";
    }
}
