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

import org.apache.paimon.types.DataField;

import java.util.List;
import java.util.Map;

/** Function schema. */
public class FunctionSchema {
    private final List<DataField> inputParams;
    private final List<DataField> returnParams;
    private final boolean deterministic;
    private final Map<String, FunctionDefinition> definitions;
    private final String comment;
    private final Map<String, String> options;

    public FunctionSchema(
            List<DataField> inputParams,
            List<DataField> returnParams,
            boolean deterministic,
            Map<String, FunctionDefinition> definitions,
            String comment,
            Map<String, String> options) {
        this.inputParams = inputParams;
        this.returnParams = returnParams;
        this.deterministic = deterministic;
        this.definitions = definitions;
        this.comment = comment;
        this.options = options;
    }

    public List<DataField> inputParams() {
        return inputParams;
    }

    public List<DataField> returnParams() {
        return returnParams;
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    public Map<String, FunctionDefinition> definitions() {
        return definitions;
    }

    public FunctionDefinition definition(String dialect) {
        return definitions.get(dialect);
    }

    public String comment() {
        return comment;
    }

    public Map<String, String> options() {
        return options;
    }
}
