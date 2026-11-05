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

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Metadata for a model registered in a catalog database. */
@Experimental
public class Model {

    private final Identifier identifier;
    private final String provider;
    private final String model;
    private final Map<String, Map<String, Object>> inputs;
    private final @Nullable Map<String, Map<String, Object>> outputs;
    private final Map<String, Object> properties;

    public Model(
            Identifier identifier,
            String provider,
            String model,
            Map<String, Map<String, Object>> inputs,
            @Nullable Map<String, Map<String, Object>> outputs,
            @Nullable Map<String, Object> properties) {
        checkArgument(identifier != null, "identifier must not be null");
        checkArgument(provider != null && !provider.trim().isEmpty(), "provider must not be blank");
        checkArgument(model != null && !model.trim().isEmpty(), "model must not be blank");
        this.identifier = identifier;
        this.provider = provider;
        this.model = model;
        this.inputs = ModelValidation.copyAndValidateFields(inputs, "inputs", true);
        this.outputs = ModelValidation.copyAndValidateFields(outputs, "outputs", false);
        this.properties = ModelValidation.copyAndValidateProperties(properties);
    }

    public Identifier identifier() {
        return identifier;
    }

    public String name() {
        return identifier.getObjectName();
    }

    public String fullName() {
        return identifier.getFullName();
    }

    public String provider() {
        return provider;
    }

    public String model() {
        return model;
    }

    public Map<String, Map<String, Object>> inputs() {
        return inputs;
    }

    @Nullable
    public Map<String, Map<String, Object>> outputs() {
        return outputs;
    }

    public Map<String, Object> properties() {
        return properties;
    }
}
