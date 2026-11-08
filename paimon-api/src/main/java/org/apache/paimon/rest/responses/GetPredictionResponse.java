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
import org.apache.paimon.catalog.PredictionStatus;
import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Response containing durable metadata for one asynchronous table prediction. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetPredictionResponse implements RESTResponse {

    private final String jobId;
    private final @Nullable String name;
    private final PredictionStatus status;
    private final long snapshotId;
    private final String model;
    private final String modelVersion;
    private final String outputTable;
    private final long createdAt;
    private final @Nullable Long startedAt;
    private final @Nullable Long finishedAt;
    private final @Nullable String error;
    private final @Nullable Long total;
    private final @Nullable Long succeeded;
    private final @Nullable Long failed;

    @JsonCreator
    public GetPredictionResponse(
            @JsonProperty("jobId") String jobId,
            @Nullable @JsonProperty("name") String name,
            @JsonProperty("status") PredictionStatus status,
            @JsonProperty("snapshotId") long snapshotId,
            @JsonProperty("model") String model,
            @JsonProperty("modelVersion") String modelVersion,
            @JsonProperty("outputTable") String outputTable,
            @JsonProperty("createdAt") long createdAt,
            @Nullable @JsonProperty("startedAt") Long startedAt,
            @Nullable @JsonProperty("finishedAt") Long finishedAt,
            @Nullable @JsonProperty("error") String error,
            @Nullable @JsonProperty("total") Long total,
            @Nullable @JsonProperty("succeeded") Long succeeded,
            @Nullable @JsonProperty("failed") Long failed) {
        this.jobId = jobId;
        this.name = name;
        this.status = status;
        this.snapshotId = snapshotId;
        this.model = model;
        this.modelVersion = modelVersion;
        this.outputTable = outputTable;
        this.createdAt = createdAt;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.error = error;
        this.total = total;
        this.succeeded = succeeded;
        this.failed = failed;
    }

    public GetPredictionResponse(Prediction prediction) {
        this(
                prediction.jobId(),
                prediction.name(),
                prediction.status(),
                prediction.snapshotId(),
                prediction.model(),
                prediction.modelVersion(),
                prediction.outputTable(),
                prediction.createdAt(),
                prediction.startedAt(),
                prediction.finishedAt(),
                prediction.error(),
                prediction.total(),
                prediction.succeeded(),
                prediction.failed());
    }

    @JsonGetter("jobId")
    public String getJobId() {
        return jobId;
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

    @JsonGetter("createdAt")
    public long getCreatedAt() {
        return createdAt;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter("startedAt")
    public Long getStartedAt() {
        return startedAt;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter("finishedAt")
    public Long getFinishedAt() {
        return finishedAt;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter("error")
    public String getError() {
        return error;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter("total")
    public Long getTotal() {
        return total;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter("succeeded")
    public Long getSucceeded() {
        return succeeded;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter("failed")
    public Long getFailed() {
        return failed;
    }

    public Prediction toPrediction() {
        return new Prediction(
                jobId,
                name,
                status,
                snapshotId,
                model,
                modelVersion,
                outputTable,
                createdAt,
                startedAt,
                finishedAt,
                error,
                total,
                succeeded,
                failed);
    }
}
