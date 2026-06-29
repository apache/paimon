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

package org.apache.paimon.stats;

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.OptionalUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/** Metadata of a statistics blob stored in a sidecar file. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class StatisticsBlobMetadata {

    private static final String FIELD_TYPE = "type";
    private static final String FIELD_FIELD_IDS = "fieldIds";
    private static final String FIELD_SNAPSHOT_ID = "snapshotId";
    private static final String FIELD_SEQUENCE_NUMBER = "sequenceNumber";
    private static final String FIELD_PROPERTIES = "properties";
    private static final String FIELD_FILE_LOCATION = "fileLocation";
    private static final String FIELD_OFFSET = "offset";
    private static final String FIELD_LENGTH = "length";

    @JsonProperty(FIELD_TYPE)
    private final String type;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty(FIELD_FIELD_IDS)
    private final List<Integer> fieldIds;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_SNAPSHOT_ID)
    private final @Nullable Long snapshotId;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_SEQUENCE_NUMBER)
    private final @Nullable Long sequenceNumber;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty(FIELD_PROPERTIES)
    private final Map<String, String> properties;

    @JsonProperty(FIELD_FILE_LOCATION)
    private final String fileLocation;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_OFFSET)
    private final @Nullable Long offset;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_LENGTH)
    private final @Nullable Long length;

    @JsonCreator
    public StatisticsBlobMetadata(
            @JsonProperty(FIELD_TYPE) String type,
            @JsonProperty(FIELD_FIELD_IDS) @Nullable List<Integer> fieldIds,
            @JsonProperty(FIELD_SNAPSHOT_ID) @Nullable Long snapshotId,
            @JsonProperty(FIELD_SEQUENCE_NUMBER) @Nullable Long sequenceNumber,
            @JsonProperty(FIELD_PROPERTIES) @Nullable Map<String, String> properties,
            @JsonProperty(FIELD_FILE_LOCATION) String fileLocation,
            @JsonProperty(FIELD_OFFSET) @Nullable Long offset,
            @JsonProperty(FIELD_LENGTH) @Nullable Long length) {
        this.type = requireField(type, FIELD_TYPE);
        this.fieldIds =
                fieldIds == null
                        ? Collections.emptyList()
                        : Collections.unmodifiableList(new ArrayList<>(fieldIds));
        this.snapshotId = snapshotId;
        this.sequenceNumber = sequenceNumber;
        this.properties =
                properties == null
                        ? Collections.emptyMap()
                        : Collections.unmodifiableMap(new LinkedHashMap<>(properties));
        this.fileLocation = requireField(fileLocation, FIELD_FILE_LOCATION);
        this.offset = offset;
        this.length = length;
    }

    public String type() {
        return type;
    }

    public List<Integer> fieldIds() {
        return fieldIds;
    }

    public OptionalLong snapshotId() {
        return OptionalUtils.ofNullable(snapshotId);
    }

    public OptionalLong sequenceNumber() {
        return OptionalUtils.ofNullable(sequenceNumber);
    }

    public Map<String, String> properties() {
        return properties;
    }

    public String fileLocation() {
        return fileLocation;
    }

    public OptionalLong offset() {
        return OptionalUtils.ofNullable(offset);
    }

    public OptionalLong length() {
        return OptionalUtils.ofNullable(length);
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public static StatisticsBlobMetadata fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, StatisticsBlobMetadata.class);
    }

    private static String requireField(@Nullable String value, String field) {
        if (value == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Missing required field '%s' in statistics blob metadata.", field));
        }
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StatisticsBlobMetadata that = (StatisticsBlobMetadata) o;
        return Objects.equals(type, that.type)
                && Objects.equals(fieldIds, that.fieldIds)
                && Objects.equals(snapshotId, that.snapshotId)
                && Objects.equals(sequenceNumber, that.sequenceNumber)
                && Objects.equals(properties, that.properties)
                && Objects.equals(fileLocation, that.fileLocation)
                && Objects.equals(offset, that.offset)
                && Objects.equals(length, that.length);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                type,
                fieldIds,
                snapshotId,
                sequenceNumber,
                properties,
                fileLocation,
                offset,
                length);
    }

    @Override
    public String toString() {
        return JsonSerdeUtil.toJson(this);
    }
}
