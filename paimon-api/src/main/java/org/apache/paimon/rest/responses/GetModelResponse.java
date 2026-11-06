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

package org.apache.paimon.rest.responses;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.Model;
import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

/** Response containing database-scoped model metadata. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetModelResponse implements RESTResponse {

    private static final String FIELD_NAME = "name";
    private static final String FIELD_PROVIDER = "provider";
    private static final String FIELD_MODEL = "model";
    private static final String FIELD_INPUTS = "inputs";
    private static final String FIELD_OUTPUTS = "outputs";
    private static final String FIELD_PROPERTIES = "properties";

    private final String name;
    private final String provider;
    private final String model;
    private final Map<String, Map<String, Object>> inputs;
    private final @Nullable Map<String, Map<String, Object>> outputs;
    private final Map<String, Object> properties;

    @JsonCreator
    public GetModelResponse(
            @JsonProperty(FIELD_NAME) String name,
            @JsonProperty(FIELD_PROVIDER) String provider,
            @JsonProperty(FIELD_MODEL) String model,
            @JsonProperty(FIELD_INPUTS) Map<String, Map<String, Object>> inputs,
            @Nullable @JsonProperty(FIELD_OUTPUTS) Map<String, Map<String, Object>> outputs,
            @Nullable @JsonProperty(FIELD_PROPERTIES) Map<String, Object> properties) {
        this.name = name;
        this.provider = provider;
        this.model = model;
        this.inputs = inputs;
        this.outputs = outputs;
        this.properties = properties == null ? Collections.emptyMap() : properties;
    }

    @JsonGetter(FIELD_NAME)
    public String getName() {
        return name;
    }

    @JsonGetter(FIELD_PROVIDER)
    public String getProvider() {
        return provider;
    }

    @JsonGetter(FIELD_MODEL)
    public String getModel() {
        return model;
    }

    @JsonGetter(FIELD_INPUTS)
    public Map<String, Map<String, Object>> getInputs() {
        return inputs;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter(FIELD_OUTPUTS)
    public Map<String, Map<String, Object>> getOutputs() {
        return outputs;
    }

    @JsonGetter(FIELD_PROPERTIES)
    public Map<String, Object> getProperties() {
        return properties;
    }

    public Model toModel(String databaseName) {
        return new Model(
                Identifier.create(databaseName, name),
                provider,
                model,
                inputs,
                outputs,
                properties);
    }
}
