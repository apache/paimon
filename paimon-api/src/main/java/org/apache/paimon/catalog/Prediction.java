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

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Durable metadata for one asynchronous table prediction. */
@Experimental
public class Prediction {

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

    public Prediction(
            String jobId,
            @Nullable String name,
            PredictionStatus status,
            long snapshotId,
            String model,
            String modelVersion,
            String outputTable,
            long createdAt,
            @Nullable Long startedAt,
            @Nullable Long finishedAt,
            @Nullable String error,
            @Nullable Long total,
            @Nullable Long succeeded,
            @Nullable Long failed) {
        checkNonBlank(jobId, "jobId");
        checkArgument(status != null, "status must not be null");
        checkArgument(snapshotId > 0, "snapshotId must be positive");
        checkNonBlank(model, "model");
        checkNonBlank(modelVersion, "modelVersion");
        checkNonBlank(outputTable, "outputTable");
        checkArgument(createdAt >= 0, "createdAt must not be negative");
        checkOptionalNonNegative(startedAt, "startedAt");
        checkOptionalNonNegative(finishedAt, "finishedAt");
        checkOptionalNonNegative(total, "total");
        checkOptionalNonNegative(succeeded, "succeeded");
        checkOptionalNonNegative(failed, "failed");
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

    public String jobId() {
        return jobId;
    }

    @Nullable
    public String name() {
        return name;
    }

    public PredictionStatus status() {
        return status;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public String model() {
        return model;
    }

    public String modelVersion() {
        return modelVersion;
    }

    public String outputTable() {
        return outputTable;
    }

    public long createdAt() {
        return createdAt;
    }

    @Nullable
    public Long startedAt() {
        return startedAt;
    }

    @Nullable
    public Long finishedAt() {
        return finishedAt;
    }

    @Nullable
    public String error() {
        return error;
    }

    @Nullable
    public Long total() {
        return total;
    }

    @Nullable
    public Long succeeded() {
        return succeeded;
    }

    @Nullable
    public Long failed() {
        return failed;
    }

    private static void checkNonBlank(String value, String name) {
        checkArgument(value != null && !value.trim().isEmpty(), "%s must not be blank", name);
    }

    private static void checkOptionalNonNegative(@Nullable Long value, String name) {
        checkArgument(value == null || value >= 0, "%s must not be negative", name);
    }
}
