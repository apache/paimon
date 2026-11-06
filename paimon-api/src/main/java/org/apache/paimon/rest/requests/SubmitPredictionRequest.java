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

/** Request for submitting an asynchronous table prediction. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SubmitPredictionRequest implements RESTRequest {

    private static final String FIELD_NAME = "name";
    private static final String FIELD_REQUEST_ID = "requestId";
    private static final String FIELD_MODEL = "model";
    private static final String FIELD_INPUTS = "inputs";
    private static final String FIELD_RESULT_COLUMNS = "resultColumns";
    private static final String FIELD_PROMPT = "prompt";
    private static final String FIELD_OUTPUT_SCHEMA = "outputSchema";
    private static final String FIELD_WHERE = "where";
    private static final String FIELD_SNAPSHOT_ID = "snapshotId";
    private static final String FIELD_TAG_NAME = "tagName";
    private static final String FIELD_OPTIONS = "options";

    private final PredictionSpec spec;

    @JsonCreator
    public SubmitPredictionRequest(
            @Nullable @JsonProperty(FIELD_NAME) String name,
            @Nullable @JsonProperty(FIELD_REQUEST_ID) String requestId,
            @JsonProperty(FIELD_MODEL) String model,
            @JsonProperty(FIELD_INPUTS) Map<String, String> inputs,
            @JsonProperty(FIELD_RESULT_COLUMNS) List<String> resultColumns,
            @JsonProperty(FIELD_PROMPT) String prompt,
            @JsonProperty(FIELD_OUTPUT_SCHEMA) Map<String, String> outputSchema,
            @Nullable @JsonProperty(FIELD_WHERE) String where,
            @Nullable @JsonProperty(FIELD_SNAPSHOT_ID) Long snapshotId,
            @Nullable @JsonProperty(FIELD_TAG_NAME) String tagName,
            @Nullable @JsonProperty(FIELD_OPTIONS) Map<String, Object> options) {
        this.spec =
                new PredictionSpec(
                        name,
                        requestId == null ? UUID.randomUUID().toString() : requestId,
                        model,
                        inputs,
                        resultColumns,
                        prompt,
                        outputSchema,
                        where,
                        snapshotId,
                        tagName,
                        options);
    }

    public SubmitPredictionRequest(PredictionSpec spec) {
        this(
                spec.name(),
                spec.requestId(),
                spec.model(),
                spec.inputs(),
                spec.resultColumns(),
                spec.prompt(),
                spec.outputSchema(),
                spec.where(),
                spec.snapshotId(),
                spec.tagName(),
                spec.options());
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter(FIELD_NAME)
    public String getName() {
        return spec.name();
    }

    @JsonGetter(FIELD_REQUEST_ID)
    public String getRequestId() {
        return spec.requestId();
    }

    @JsonGetter(FIELD_MODEL)
    public String getModel() {
        return spec.model();
    }

    @JsonGetter(FIELD_INPUTS)
    public Map<String, String> getInputs() {
        return spec.inputs();
    }

    @JsonGetter(FIELD_RESULT_COLUMNS)
    public List<String> getResultColumns() {
        return spec.resultColumns();
    }

    @JsonGetter(FIELD_PROMPT)
    public String getPrompt() {
        return spec.prompt();
    }

    @JsonGetter(FIELD_OUTPUT_SCHEMA)
    public Map<String, String> getOutputSchema() {
        return spec.outputSchema();
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter(FIELD_WHERE)
    public String getWhere() {
        return spec.where();
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter(FIELD_SNAPSHOT_ID)
    public Long getSnapshotId() {
        return spec.snapshotId();
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter(FIELD_TAG_NAME)
    public String getTagName() {
        return spec.tagName();
    }

    @JsonGetter(FIELD_OPTIONS)
    public Map<String, Object> getOptions() {
        return spec.options();
    }

    public PredictionSpec toPredictionSpec() {
        return spec;
    }
}
