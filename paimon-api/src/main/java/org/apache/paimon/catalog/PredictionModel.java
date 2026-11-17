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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Registered model used by a prediction job. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class PredictionModel {

    private final String database;
    private final String model;

    @JsonCreator
    public PredictionModel(
            @JsonProperty("database") String database, @JsonProperty("model") String model) {
        checkNonBlank(database, "database");
        checkNonBlank(model, "model");
        this.database = database;
        this.model = model;
    }

    public PredictionModel(Identifier identifier) {
        this(identifier.getDatabaseName(), identifier.getObjectName());
    }

    @JsonGetter("database")
    public String database() {
        return database;
    }

    @JsonGetter("model")
    public String model() {
        return model;
    }

    public Identifier toIdentifier() {
        return Identifier.create(database, model);
    }

    private static void checkNonBlank(String value, String name) {
        checkArgument(value != null && !value.trim().isEmpty(), "%s must not be blank", name);
    }
}
