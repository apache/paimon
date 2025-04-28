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

package org.apache.paimon.function;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.types.DataField;

import java.util.List;
import java.util.Map;

/** Function implementation. * */
public class FunctionImpl implements Function {

    private final Identifier identifier;
    private final String uuid;
    private final FunctionSchema schema;

    public FunctionImpl(
            String uuid,
            Identifier identifier,
            List<DataField> inputParams,
            List<DataField> returnParams,
            boolean deterministic,
            Map<String, FunctionDefinition> definitions,
            String comment,
            Map<String, String> options) {
        this.identifier = identifier;
        this.uuid = uuid;
        this.schema =
                new FunctionSchema(
                        inputParams, returnParams, deterministic, definitions, comment, options);
    }

    public FunctionImpl(Identifier identifier, String uuid, FunctionSchema schema) {
        this.identifier = identifier;
        this.uuid = uuid;
        this.schema = schema;
    }

    @Override
    public String uuid() {
        return this.uuid;
    }

    @Override
    public String name() {
        return this.identifier.getObjectName();
    }

    @Override
    public List<DataField> inputParams() {
        return this.schema.inputParams();
    }

    @Override
    public List<DataField> returnParams() {
        return this.schema.returnParams();
    }

    @Override
    public boolean isDeterministic() {
        return this.schema.isDeterministic();
    }

    @Override
    public Map<String, FunctionDefinition> definitions() {
        return this.schema.definitions();
    }

    @Override
    public FunctionDefinition definition(String dialect) {
        return this.schema.definition(dialect);
    }

    @Override
    public String comment() {
        return this.schema.comment();
    }

    @Override
    public Map<String, String> options() {
        return this.schema.options();
    }
}
