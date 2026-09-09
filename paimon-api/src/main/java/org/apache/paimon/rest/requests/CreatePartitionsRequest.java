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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Request for creating partitions.
 *
 * <p>Statistics ride along optionally, matched to {@code partitionSpecs} by {@link
 * PartitionStatistics#spec()} rather than by position, so they may cover only some of them. Both
 * statistics fields are absent unless the client reports. Partition options align with {@code
 * partitionSpecs} by position; {@code path:null} resets a partition to its default location and
 * needs replacement statistics for that partition.
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
        Set<Map<String, String>> resetSpecs = new HashSet<>();
        if (partitionOptions != null) {
            for (int i = 0; i < partitionOptions.size(); i++) {
                Map<String, String> options = partitionOptions.get(i);
                checkOptionValues(options);
                if (options.containsKey(CoreOptions.PATH.key())
                        && options.get(CoreOptions.PATH.key()) == null) {
                    Map<String, String> spec = partitionSpecs.get(i);
                    checkArgument(spec != null, "path=null requires a non-null partition spec.");
                    resetSpecs.add(spec);
                }
            }
        }
        if (!resetSpecs.isEmpty()) {
            checkArgument(
                    Boolean.TRUE.equals(replaceStatistics),
                    "path=null requires replaceStatistics=true.");
            checkArgument(
                    partitionStatistics != null,
                    "path=null requires replacement statistics for the same partition.");
            Set<Map<String, String>> requestSpecs = new HashSet<>(partitionSpecs);
            Set<Map<String, String>> reportedSpecs = new HashSet<>();
            for (PartitionStatistics statistic : partitionStatistics) {
                checkArgument(
                        statistic != null && statistic.spec() != null,
                        "partitionStatistics must not contain null entries or specs.");
                checkArgument(
                        requestSpecs.contains(statistic.spec()),
                        "Statistics for partition %s do not match any partition in this request.",
                        statistic.spec());
                checkArgument(
                        reportedSpecs.add(statistic.spec()),
                        "Statistics for partition %s are reported more than once.",
                        statistic.spec());
                resetSpecs.remove(statistic.spec());
            }
            checkArgument(
                    resetSpecs.isEmpty(),
                    "path=null requires replacement statistics for the same partition; missing %s.",
                    resetSpecs);
        }
        this.partitionSpecs = partitionSpecs;
        this.ignoreIfExists = ignoreIfExists == null || ignoreIfExists;
        this.partitionStatistics = partitionStatistics;
        this.replaceStatistics = replaceStatistics;
        this.partitionOptions = partitionOptions;
    }

    /**
     * Null is allowed only for {@code path}, where it resets the partition to its default location.
     */
    public static void checkOptionValues(Map<String, String> options) {
        for (Map.Entry<String, String> entry : options.entrySet()) {
            checkArgument(entry.getKey() != null, "Partition options must not contain null keys.");
            checkArgument(
                    entry.getValue() != null || CoreOptions.PATH.key().equals(entry.getKey()),
                    "Partition option %s must not be null; only path may be null, which resets the location.",
                    entry.getKey());
        }
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

    /**
     * Options aligned with partition specs; {@code path:null} resets the location. A null list
     * omits the field.
     */
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
