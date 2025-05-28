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
import org.apache.paimon.function.FunctionImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeJsonParser;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * create function procedure. Usage:
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *
 *  CALL sys.create_function('function_identifier',
 *     '[{"id": 0, "name":"length", "type":"INT"}', '{"id": 1, "name":"width", "type":"INT"}]',
 *     '[{"id": 0, "name":"area", "type":"BIGINT"]',
 *     true, 'comment'
 *    )
 *
 * </code></pre>
 */
public class CreateFunctionProcedure extends ProcedureBase {
    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "function", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "inputParams", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "returnParams", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "deterministic",
                        type = @DataTypeHint("BOOLEAN"),
                        isOptional = true),
                @ArgumentHint(name = "comment", type = @DataTypeHint("STRING"), isOptional = true),
            })
    public String[] call(
            ProcedureContext procedureContext,
            String function,
            String inputParams,
            String returnParams,
            Boolean deterministic,
            String comment)
            throws Catalog.ViewNotExistException, Catalog.DialectAlreadyExistException,
                    Catalog.DialectNotExistException, Catalog.FunctionAlreadyExistException,
                    Catalog.DatabaseNotExistException {
        Identifier identifier = Identifier.fromString(function);
        FunctionImpl functionImpl =
                new FunctionImpl(
                        identifier,
                        getParameters(inputParams),
                        getParameters(returnParams),
                        Optional.ofNullable(deterministic).orElse(true),
                        Maps.newHashMap(),
                        comment,
                        Maps.newHashMap());
        catalog.createFunction(identifier, functionImpl, false);
        return new String[] {"Success"};
    }

    @Override
    public String identifier() {
        return "create_function";
    }

    public static List<DataField> getParameters(String data) {
        List<DataField> list = new ArrayList<>();
        if (data != null) {
            JsonNode jsonArray = JsonSerdeUtil.fromJson(data, JsonNode.class);
            if (jsonArray.isArray()) {
                for (JsonNode objNode : jsonArray) {
                    DataField dataField = DataTypeJsonParser.parseDataField(objNode);
                    list.add(dataField);
                }
            }
        }
        return list;
    }
}
