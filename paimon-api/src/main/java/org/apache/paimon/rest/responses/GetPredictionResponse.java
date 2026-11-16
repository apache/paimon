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

import org.apache.paimon.catalog.Prediction;
import org.apache.paimon.catalog.PredictionSource;
import org.apache.paimon.catalog.PredictionStatus;
import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Response containing durable metadata for one asynchronous prediction job. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetPredictionResponse implements RESTResponse {

    private final String predictionId;
    private final PredictionSource source;
    private final @Nullable String name;
    private final PredictionStatus status;
    private final long snapshotId;
    private final String model;
    private final String modelVersion;
    private final String outputTable;
    private final @Nullable Long startTime;
    private final @Nullable Long endTime;

    @JsonCreator
    public GetPredictionResponse(
            @JsonProperty("predictionId") String predictionId,
            @JsonProperty("source") PredictionSource source,
            @Nullable @JsonProperty("name") String name,
            @JsonProperty("status") PredictionStatus status,
            @JsonProperty("snapshotId") long snapshotId,
            @JsonProperty("model") String model,
            @JsonProperty("modelVersion") String modelVersion,
            @JsonProperty("outputTable") String outputTable,
            @Nullable @JsonProperty("startTime") Long startTime,
            @Nullable @JsonProperty("endTime") Long endTime) {
        this.predictionId = predictionId;
        this.source = source;
        this.name = name;
        this.status = status;
        this.snapshotId = snapshotId;
        this.model = model;
        this.modelVersion = modelVersion;
        this.outputTable = outputTable;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public GetPredictionResponse(Prediction prediction) {
        this(
                prediction.predictionId(),
                prediction.source(),
                prediction.name(),
                prediction.status(),
                prediction.snapshotId(),
                prediction.model(),
                prediction.modelVersion(),
                prediction.outputTable(),
                prediction.startTime(),
                prediction.endTime());
    }

    @JsonGetter("predictionId")
    public String getPredictionId() {
        return predictionId;
    }

    @JsonGetter("source")
    public PredictionSource getSource() {
        return source;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter("name")
    public String getName() {
        return name;
    }

    @JsonGetter("status")
    public PredictionStatus getStatus() {
        return status;
    }

    @JsonGetter("snapshotId")
    public long getSnapshotId() {
        return snapshotId;
    }

    @JsonGetter("model")
    public String getModel() {
        return model;
    }

    @JsonGetter("modelVersion")
    public String getModelVersion() {
        return modelVersion;
    }

    @JsonGetter("outputTable")
    public String getOutputTable() {
        return outputTable;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter("startTime")
    public Long getStartTime() {
        return startTime;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter("endTime")
    public Long getEndTime() {
        return endTime;
    }

    public Prediction toPrediction() {
        return new Prediction(
                predictionId,
                source,
                name,
                status,
                snapshotId,
                model,
                modelVersion,
                outputTable,
                startTime,
                endTime);
    }
}
