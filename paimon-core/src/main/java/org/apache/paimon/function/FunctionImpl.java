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

import org.apache.paimon.rest.responses.GetFunctionResponse;
import org.apache.paimon.types.DataField;

import java.util.List;
import java.util.Map;

/** Function implementation. */
public class FunctionImpl implements Function {

    private final String uuid;

    private final String name;

    private final List<DataField> inputParams;

    private final List<DataField> returnParams;

    private final boolean deterministic;

    private final Map<String, FunctionDefinition> definitions;

    private final String comment;

    private final Map<String, String> options;

    public FunctionImpl(
            String uuid,
            String functionName,
            List<DataField> inputParams,
            List<DataField> returnParams,
            boolean deterministic,
            Map<String, FunctionDefinition> definitions,
            String comment,
            Map<String, String> options) {
        this.uuid = uuid;
        this.name = functionName;
        this.inputParams = inputParams;
        this.returnParams = returnParams;
        this.deterministic = deterministic;
        this.definitions = definitions;
        this.comment = comment;
        this.options = options;
    }

    public FunctionImpl(GetFunctionResponse response) {
        this.uuid = response.uuid();
        this.name = response.name();
        this.inputParams = response.inputParams();
        this.returnParams = response.returnParams();
        this.deterministic = response.isDeterministic();
        this.definitions = response.definitions();
        this.comment = response.comment();
        this.options = response.options();
    }

    @Override
    public String uuid() {
        return this.uuid;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public List<DataField> inputParams() {
        return inputParams;
    }

    @Override
    public List<DataField> returnParams() {
        return returnParams;
    }

    @Override
    public boolean isDeterministic() {
        return deterministic;
    }

    @Override
    public Map<String, FunctionDefinition> definitions() {
        return definitions;
    }

    @Override
    public FunctionDefinition definition(String name) {
        return definitions.get(name);
    }

    @Override
    public String comment() {
        return comment;
    }

    @Override
    public Map<String, String> options() {
        return options;
    }
}
