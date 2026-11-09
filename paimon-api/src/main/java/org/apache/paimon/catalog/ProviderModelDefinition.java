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

package org.apache.paimon.catalog;

import org.apache.paimon.annotation.Experimental;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.catalog.ModelField.checkNonBlank;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Definition of a model served by an external model provider. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProviderModelDefinition implements ModelDefinition {

    public static final String TYPE = "provider";

    private final String provider;
    private final String providerModel;
    private final List<ModelField> inputs;
    private final @Nullable List<ModelField> outputs;
    private final Map<String, String> options;

    public ProviderModelDefinition(
            String provider,
            String providerModel,
            List<ModelField> inputs,
            @Nullable List<ModelField> outputs,
            @Nullable Map<String, String> options) {
        this(TYPE, provider, providerModel, inputs, outputs, options);
    }

    @JsonCreator
    public ProviderModelDefinition(
            @JsonProperty("type") String type,
            @JsonProperty("provider") String provider,
            @JsonProperty("providerModel") String providerModel,
            @JsonProperty("inputs") List<ModelField> inputs,
            @Nullable @JsonProperty("outputs") List<ModelField> outputs,
            @Nullable @JsonProperty("options") Map<String, String> options) {
        checkArgument(TYPE.equals(type), "type must be '%s'", TYPE);
        checkNonBlank(provider, "provider");
        checkNonBlank(providerModel, "providerModel");
        this.provider = provider;
        this.providerModel = providerModel;
        this.inputs = copyFields(inputs, "inputs", true);
        this.outputs = copyFields(outputs, "outputs", false);
        this.options = copyOptions(options);
    }

    @Override
    @JsonGetter("type")
    public String type() {
        return TYPE;
    }

    @JsonGetter("provider")
    public String provider() {
        return provider;
    }

    @JsonGetter("providerModel")
    public String providerModel() {
        return providerModel;
    }

    @JsonGetter("inputs")
    public List<ModelField> inputs() {
        return inputs;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter("outputs")
    public List<ModelField> outputs() {
        return outputs;
    }

    @JsonGetter("options")
    public Map<String, String> options() {
        return options;
    }

    @Nullable
    private static List<ModelField> copyFields(
            @Nullable List<ModelField> fields, String name, boolean required) {
        if (fields == null) {
            checkArgument(!required, "%s must be a non-empty list", name);
            return null;
        }
        checkArgument(!required || !fields.isEmpty(), "%s must be a non-empty list", name);
        List<ModelField> copy = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (ModelField field : fields) {
            checkArgument(field != null, "%s must not contain null fields", name);
            checkArgument(
                    names.add(field.name()),
                    "%s contains duplicate field '%s'",
                    name,
                    field.name());
            copy.add(field);
        }
        return Collections.unmodifiableList(copy);
    }

    private static Map<String, String> copyOptions(@Nullable Map<String, String> options) {
        if (options == null) {
            return Collections.emptyMap();
        }
        Map<String, String> copy = new LinkedHashMap<>();
        options.forEach(
                (key, value) -> {
                    checkNonBlank(key, "option name");
                    checkArgument(value != null, "option value must not be null");
                    copy.put(key, value);
                });
        return Collections.unmodifiableMap(copy);
    }
}
