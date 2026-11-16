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

/** Durable metadata for one asynchronous prediction job. */
@Experimental
public class Prediction {

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

    public Prediction(
            String predictionId,
            PredictionSource source,
            @Nullable String name,
            PredictionStatus status,
            long snapshotId,
            String model,
            String modelVersion,
            String outputTable,
            @Nullable Long startTime,
            @Nullable Long endTime) {
        checkNonBlank(predictionId, "predictionId");
        checkArgument(source != null, "source must not be null");
        checkArgument(status != null, "status must not be null");
        checkArgument(snapshotId > 0, "snapshotId must be positive");
        checkNonBlank(model, "model");
        checkNonBlank(modelVersion, "modelVersion");
        checkNonBlank(outputTable, "outputTable");
        checkOptionalNonNegative(startTime, "startTime");
        checkOptionalNonNegative(endTime, "endTime");
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

    public String predictionId() {
        return predictionId;
    }

    public PredictionSource source() {
        return source;
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

    public Long startTime() {
        return startTime;
    }

    @Nullable
    public Long endTime() {
        return endTime;
    }

    private static void checkNonBlank(String value, String name) {
        checkArgument(value != null && !value.trim().isEmpty(), "%s must not be blank", name);
    }

    private static void checkOptionalNonNegative(@Nullable Long value, String name) {
        checkArgument(value == null || value >= 0, "%s must not be negative", name);
    }
}
