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

package org.apache.paimon.rest.requests;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PredictionSource;
import org.apache.paimon.catalog.PredictionSpec;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Request for submitting an asynchronous prediction job. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SubmitPredictionRequest implements RESTRequest {

    private static final String FIELD_NAME = "name";
    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_IDEMPOTENCY_KEY = "idempotencyKey";
    private static final String FIELD_MODEL = "model";
    private static final String FIELD_INPUTS = "inputs";
    private static final String FIELD_MERGE_KEY_COLUMNS = "mergeKeyColumns";
    private static final String FIELD_INSTRUCTIONS = "instructions";
    private static final String FIELD_WHERE = "where";
    private static final String FIELD_OPTIONS = "options";

    private final String idempotencyKey;
    private final PredictionSource source;
    private final PredictionSpec spec;

    @JsonCreator
    public SubmitPredictionRequest(
            @JsonProperty(FIELD_SOURCE) PredictionSource source,
            @Nullable @JsonProperty(FIELD_NAME) String name,
            @Nullable @JsonProperty(FIELD_IDEMPOTENCY_KEY) String idempotencyKey,
            @JsonProperty(FIELD_MODEL) Identifier model,
            @JsonProperty(FIELD_INPUTS) Map<String, String> inputs,
            @Nullable @JsonProperty(FIELD_MERGE_KEY_COLUMNS) List<String> mergeKeyColumns,
            @JsonProperty(FIELD_INSTRUCTIONS) String instructions,
            @Nullable @JsonProperty(FIELD_WHERE) String where,
            @Nullable @JsonProperty(FIELD_OPTIONS) Map<String, String> options) {
        checkArgument(source != null, "source must not be null");
        checkArgument(model != null, "model must not be null");
        checkArgument(
                idempotencyKey == null || !idempotencyKey.trim().isEmpty(),
                "idempotencyKey must not be blank");
        this.source = source;
        this.idempotencyKey =
                idempotencyKey == null ? UUID.randomUUID().toString() : idempotencyKey;
        this.spec =
                new PredictionSpec(
                        name, model, inputs, mergeKeyColumns, instructions, where, options);
    }

    public SubmitPredictionRequest(Identifier source, PredictionSpec spec) {
        this(
                new PredictionSource(source),
                spec.name(),
                null,
                spec.model(),
                spec.inputs(),
                spec.mergeKeyColumns(),
                spec.instructions(),
                spec.where(),
                spec.options());
    }

    @JsonGetter(FIELD_SOURCE)
    public PredictionSource getSource() {
        return source;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter(FIELD_NAME)
    public String getName() {
        return spec.name();
    }

    @JsonGetter(FIELD_IDEMPOTENCY_KEY)
    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    @JsonGetter(FIELD_MODEL)
    public Identifier getModel() {
        return spec.model();
    }

    @JsonGetter(FIELD_INPUTS)
    public Map<String, String> getInputs() {
        return spec.inputs();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonGetter(FIELD_MERGE_KEY_COLUMNS)
    public List<String> getMergeKeyColumns() {
        return spec.mergeKeyColumns();
    }

    @JsonGetter(FIELD_INSTRUCTIONS)
    public String getInstructions() {
        return spec.instructions();
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter(FIELD_WHERE)
    public String getWhere() {
        return spec.where();
    }

    @JsonGetter(FIELD_OPTIONS)
    public Map<String, String> getOptions() {
        return spec.options();
    }

    public PredictionSpec toPredictionSpec() {
        return spec;
    }
}
