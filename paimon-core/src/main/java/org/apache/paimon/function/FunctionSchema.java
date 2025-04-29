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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/** Function schema. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FunctionSchema {

    private static final String FIELD_INPUT_PARAMETERS = "inputParams";
    private static final String FIELD_RETURN_PARAMETERS = "returnParams";
    private static final String FIELD_DEFINITIONS = "definitions";
    private static final String FIELD_DETERMINISTIC = "deterministic";
    private static final String FIELD_COMMENT = "comment";
    private static final String FIELD_OPTIONS = "options";

    @JsonProperty(FIELD_INPUT_PARAMETERS)
    private final List<DataField> inputParams;

    @JsonProperty(FIELD_RETURN_PARAMETERS)
    private final List<DataField> returnParams;

    @JsonProperty(FIELD_DETERMINISTIC)
    private final boolean deterministic;

    @JsonProperty(FIELD_DEFINITIONS)
    private final Map<String, FunctionDefinition> definitions;

    @JsonProperty(FIELD_COMMENT)
    private final String comment;

    @JsonProperty(FIELD_OPTIONS)
    private final Map<String, String> options;

    @JsonCreator
    public FunctionSchema(
            @JsonProperty(FIELD_INPUT_PARAMETERS) List<DataField> inputParams,
            @JsonProperty(FIELD_RETURN_PARAMETERS) List<DataField> returnParams,
            @JsonProperty(FIELD_DETERMINISTIC) boolean deterministic,
            @JsonProperty(FIELD_DEFINITIONS) Map<String, FunctionDefinition> definitions,
            @JsonProperty(FIELD_COMMENT) String comment,
            @JsonProperty(FIELD_OPTIONS) Map<String, String> options) {
        this.inputParams = inputParams;
        this.returnParams = returnParams;
        this.deterministic = deterministic;
        this.definitions = definitions;
        this.comment = comment;
        this.options = options;
    }

    @JsonGetter(FIELD_INPUT_PARAMETERS)
    public List<DataField> inputParams() {
        return inputParams;
    }

    @JsonGetter(FIELD_RETURN_PARAMETERS)
    public List<DataField> returnParams() {
        return returnParams;
    }

    @JsonGetter(FIELD_DETERMINISTIC)
    public boolean isDeterministic() {
        return deterministic;
    }

    @JsonGetter(FIELD_DEFINITIONS)
    public Map<String, FunctionDefinition> definitions() {
        return definitions;
    }

    public FunctionDefinition definition(String dialect) {
        return definitions.get(dialect);
    }

    @JsonGetter(FIELD_COMMENT)
    public String comment() {
        return comment;
    }

    @JsonGetter(FIELD_OPTIONS)
    public Map<String, String> options() {
        return options;
    }
}
