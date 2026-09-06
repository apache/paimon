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

import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Request for creating partitions.
 *
 * <p>Statistics ride along optionally, matched to {@code partitionSpecs} by {@link
 * PartitionStatistics#spec()} rather than by position, so they may cover only some of them. Both
 * statistics fields are absent unless the client reports.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreatePartitionsRequest implements RESTRequest {

    private static final String FIELD_PARTITION_SPECS = "partitionSpecs";
    private static final String FIELD_IGNORE_IF_EXISTS = "ignoreIfExists";
    private static final String FIELD_PARTITION_STATISTICS = "partitionStatistics";
    private static final String FIELD_REPLACE_STATISTICS = "replaceStatistics";
    private static final String FIELD_PARTITION_OPTIONS = "partitionOptions";

    @JsonProperty(FIELD_PARTITION_SPECS)
    private final List<Map<String, String>> partitionSpecs;

    @JsonProperty(FIELD_IGNORE_IF_EXISTS)
    private final boolean ignoreIfExists;

    @JsonProperty(FIELD_PARTITION_STATISTICS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final List<PartitionStatistics> partitionStatistics;

    @JsonProperty(FIELD_REPLACE_STATISTICS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Boolean replaceStatistics;

    @JsonProperty(FIELD_PARTITION_OPTIONS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final List<Map<String, String>> partitionOptions;

    public CreatePartitionsRequest(List<Map<String, String>> partitionSpecs) {
        this(partitionSpecs, true);
    }

    public CreatePartitionsRequest(
            List<Map<String, String>> partitionSpecs, @Nullable Boolean ignoreIfExists) {
        this(partitionSpecs, ignoreIfExists, null, null, null);
    }

    public CreatePartitionsRequest(
            List<Map<String, String>> partitionSpecs,
            @Nullable Boolean ignoreIfExists,
            @Nullable List<PartitionStatistics> partitionStatistics,
            @Nullable Boolean replaceStatistics) {
        this(partitionSpecs, ignoreIfExists, partitionStatistics, replaceStatistics, null);
    }

    @JsonCreator
    public CreatePartitionsRequest(
            @JsonProperty(FIELD_PARTITION_SPECS) List<Map<String, String>> partitionSpecs,
            @JsonProperty(FIELD_IGNORE_IF_EXISTS) @Nullable Boolean ignoreIfExists,
            @JsonProperty(FIELD_PARTITION_STATISTICS) @Nullable
                    List<PartitionStatistics> partitionStatistics,
            @JsonProperty(FIELD_REPLACE_STATISTICS) @Nullable Boolean replaceStatistics,
            @JsonProperty(FIELD_PARTITION_OPTIONS) @Nullable
                    List<Map<String, String>> partitionOptions) {
        checkArgument(
                partitionOptions == null
                        || (partitionSpecs != null
                                && partitionOptions.size() == partitionSpecs.size()),
                "partitionOptions must be null or have the same size as partitionSpecs.");
        checkArgument(
                partitionOptions == null || !partitionOptions.contains(null),
                "partitionOptions must not contain null maps.");
        checkArgument(
                partitionOptions == null
                        || partitionOptions.stream()
                                .flatMap(options -> options.entrySet().stream())
                                .noneMatch(
                                        entry ->
                                                entry.getKey() == null || entry.getValue() == null),
                "partitionOptions must not contain null keys or values.");
        this.partitionSpecs = partitionSpecs;
        this.ignoreIfExists = ignoreIfExists == null || ignoreIfExists;
        this.partitionStatistics = partitionStatistics;
        this.replaceStatistics = replaceStatistics;
        this.partitionOptions = partitionOptions;
    }

    @JsonGetter(FIELD_PARTITION_SPECS)
    public List<Map<String, String>> getPartitionSpecs() {
        return partitionSpecs;
    }

    @JsonGetter(FIELD_IGNORE_IF_EXISTS)
    public boolean ignoreIfExists() {
        return ignoreIfExists;
    }

    /** Reported statistics, or null when the client reports none. */
    @JsonGetter(FIELD_PARTITION_STATISTICS)
    @Nullable
    public List<PartitionStatistics> getPartitionStatistics() {
        return partitionStatistics;
    }

    /**
     * Whether the reported statistics replace what the catalog holds rather than adding to it; null
     * when none are reported.
     */
    @JsonGetter(FIELD_REPLACE_STATISTICS)
    @Nullable
    public Boolean replaceStatistics() {
        return replaceStatistics;
    }

    /** Options aligned with partition specs; a null list omits the field. */
    @JsonGetter(FIELD_PARTITION_OPTIONS)
    @Nullable
    public List<Map<String, String>> getPartitionOptions() {
        return partitionOptions;
    }

    /**
     * Registering is an upsert and replacing lands on the same value twice, so both survive being
     * sent again. Adding does not: a second delivery is counted again. A request that reports no
     * statistics increments nothing and so keeps its retry, which is the shape batching a create
     * leaves behind.
     */
    @JsonIgnore
    @Override
    public boolean isRetrySafe() {
        return partitionStatistics == null
                || partitionStatistics.isEmpty()
                || Boolean.TRUE.equals(replaceStatistics);
    }
}
