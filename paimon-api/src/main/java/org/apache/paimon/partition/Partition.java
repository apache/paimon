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

package org.apache.paimon.partition;

import org.apache.paimon.annotation.Public;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_CREATED_AT;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_CREATED_BY;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_UPDATED_AT;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_UPDATED_BY;

/** Represent a partition, including statistics and done flag. */
@JsonIgnoreProperties(ignoreUnknown = true)
@Public
public class Partition extends PartitionStatistics {

    private static final long serialVersionUID = 3L;

    public static final String FIELD_DONE = "done";
    public static final String FIELD_OPTIONS = "options";

    @JsonProperty(FIELD_DONE)
    private final boolean done;

    @JsonProperty(FIELD_CREATED_AT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Long createdAt;

    @JsonProperty(FIELD_CREATED_BY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final String createdBy;

    @JsonProperty(FIELD_UPDATED_AT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Long updatedAt;

    @JsonProperty(FIELD_UPDATED_BY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final String updatedBy;

    @JsonProperty(FIELD_OPTIONS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Map<String, String> options;

    @JsonCreator
    public Partition(
            @JsonProperty(FIELD_SPEC) Map<String, String> spec,
            @JsonProperty(FIELD_RECORD_COUNT) long recordCount,
            @JsonProperty(FIELD_FILE_SIZE_IN_BYTES) long fileSizeInBytes,
            @JsonProperty(FIELD_FILE_COUNT) long fileCount,
            @JsonProperty(FIELD_LAST_FILE_CREATION_TIME) long lastFileCreationTime,
            @JsonProperty(FIELD_DONE) boolean done,
            @JsonProperty(FIELD_CREATED_AT) @Nullable Long createdAt,
            @JsonProperty(FIELD_CREATED_BY) @Nullable String createdBy,
            @JsonProperty(FIELD_UPDATED_AT) @Nullable Long updatedAt,
            @JsonProperty(FIELD_UPDATED_BY) @Nullable String updatedBy,
            @JsonProperty(FIELD_OPTIONS) @Nullable Map<String, String> options) {
        super(spec, recordCount, fileSizeInBytes, fileCount, lastFileCreationTime);
        this.done = done;
        this.createdAt = createdAt;
        this.createdBy = createdBy;
        this.updatedAt = updatedAt;
        this.updatedBy = updatedBy;
        this.options = options;
    }

    public Partition(
            Map<String, String> spec,
            long recordCount,
            long fileSizeInBytes,
            long fileCount,
            long lastFileCreationTime,
            boolean done) {
        this(
                spec,
                recordCount,
                fileSizeInBytes,
                fileCount,
                lastFileCreationTime,
                done,
                null,
                null,
                null,
                null,
                null);
    }

    @JsonGetter(FIELD_DONE)
    public boolean done() {
        return done;
    }

    @Nullable
    @JsonGetter(FIELD_CREATED_AT)
    public Long createdAt() {
        return createdAt;
    }

    @Nullable
    @JsonGetter(FIELD_CREATED_BY)
    public String createdBy() {
        return createdBy;
    }

    @Nullable
    @JsonGetter(FIELD_UPDATED_AT)
    public Long updatedAt() {
        return updatedAt;
    }

    @Nullable
    @JsonGetter(FIELD_UPDATED_BY)
    public String updatedBy() {
        return updatedBy;
    }

    @Nullable
    @JsonGetter(FIELD_OPTIONS)
    public Map<String, String> options() {
        return options;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        Partition partition = (Partition) o;
        return done == partition.done
                && Objects.equals(createdAt, partition.createdAt)
                && Objects.equals(createdBy, partition.createdBy)
                && Objects.equals(updatedAt, partition.updatedAt)
                && Objects.equals(updatedBy, partition.updatedBy)
                && Objects.equals(options, partition.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(), done, createdAt, createdBy, updatedAt, updatedBy, options);
    }

    @Override
    public String toString() {
        return "{"
                + "spec="
                + spec
                + ", recordCount="
                + recordCount
                + ", fileSizeInBytes="
                + fileSizeInBytes
                + ", fileCount="
                + fileCount
                + ", lastFileCreationTime="
                + lastFileCreationTime
                + ", done="
                + done
                + ", createdAt="
                + createdAt
                + ", createdBy="
                + createdBy
                + ", updatedAt="
                + updatedAt
                + ", updatedBy="
                + updatedBy
                + ", options="
                + options
                + '}';
    }
}
