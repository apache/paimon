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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.OptionalUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.stream.Collectors;

/**
 * Global stats, supports the following stats.
 *
 * <ul>
 *   <li>mergedRecordCount: the total number of records after merge
 *   <li>mergedRecordSize: the size of the mergedRecordCount in bytes
 *   <li>colStats: column stats map
 * </ul>
 */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class Statistics {

    // ID of the snapshot this statistics collected from
    private static final String FIELD_SNAPSHOT_ID = "snapshotId";
    // Schema ID of the snapshot this statistics collected from
    private static final String FIELD_SCHEMA_ID = "schemaId";
    private static final String FIELD_MERGED_RECORD_COUNT = "mergedRecordCount";
    private static final String FIELD_MERGED_RECORD_SIZE = "mergedRecordSize";
    private static final String FIELD_COL_STATS = "colStats";

    @JsonProperty(FIELD_SNAPSHOT_ID)
    private final long snapshotId;

    @JsonProperty(FIELD_SCHEMA_ID)
    private final long schemaId;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_MERGED_RECORD_COUNT)
    private final @Nullable Long mergedRecordCount;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_MERGED_RECORD_SIZE)
    private final @Nullable Long mergedRecordSize;

    @JsonProperty(FIELD_COL_STATS)
    private final Map<String, ColStats<?>> colStats;

    @JsonCreator
    public Statistics(
            @JsonProperty(FIELD_SNAPSHOT_ID) long snapshotId,
            @JsonProperty(FIELD_SCHEMA_ID) long schemaId,
            @JsonProperty(FIELD_MERGED_RECORD_COUNT) @Nullable Long mergedRecordCount,
            @JsonProperty(FIELD_MERGED_RECORD_SIZE) @Nullable Long mergedRecordSize,
            @JsonProperty(FIELD_COL_STATS) Map<String, ColStats<?>> colStats) {
        this.snapshotId = snapshotId;
        this.schemaId = schemaId;
        this.mergedRecordCount = mergedRecordCount;
        this.mergedRecordSize = mergedRecordSize;
        this.colStats = colStats;
    }

    public Statistics(
            long snapshotId, long schemaId, Long mergedRecordCount, Long mergedRecordSize) {
        this(snapshotId, schemaId, mergedRecordCount, mergedRecordSize, Collections.emptyMap());
    }

    public long snapshotId() {
        return snapshotId;
    }

    public long schemaId() {
        return schemaId;
    }

    public OptionalLong mergedRecordCount() {
        return OptionalUtils.ofNullable(mergedRecordCount);
    }

    public OptionalLong mergedRecordSize() {
        return OptionalUtils.ofNullable(mergedRecordSize);
    }

    public Map<String, ColStats<?>> colStats() {
        return colStats;
    }

    public void serializeFieldsToString(TableSchema schema) {
        try {
            if (colStats != null) {
                Map<String, DataType> fields =
                        schema.fields().stream()
                                .collect(Collectors.toMap(DataField::name, DataField::type));
                for (Map.Entry<String, ColStats<?>> entry : colStats.entrySet()) {
                    String colName = entry.getKey();
                    ColStats<?> colStats = entry.getValue();
                    DataType type = fields.get(colName);
                    if (type == null) {
                        throw new IllegalStateException("Unable to obtain the latest schema");
                    }
                    colStats.serializeFieldsToString(type);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to serialize fields to string", e);
        }
    }

    public void deserializeFieldsFromString(TableSchema schema) {
        try {
            if (colStats != null) {
                Map<String, DataType> fields =
                        schema.fields().stream()
                                .collect(Collectors.toMap(DataField::name, DataField::type));
                for (Map.Entry<String, ColStats<?>> entry : colStats.entrySet()) {
                    String colName = entry.getKey();
                    ColStats<?> colStats = entry.getValue();
                    DataType type = fields.get(colName);
                    if (type == null) {
                        throw new IllegalStateException("Unable to obtain the latest schema");
                    }
                    colStats.deserializeFieldsFromString(type);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to deserialize fields from string", e);
        }
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public static Statistics fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, Statistics.class);
    }

    public static Statistics fromPath(FileIO fileIO, Path path) {
        try {
            String json = fileIO.readFileUtf8(path);
            return Statistics.fromJson(json);
        } catch (IOException e) {
            throw new RuntimeException("Fails to read snapshot from path " + path, e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Statistics stats = (Statistics) o;
        return snapshotId == stats.snapshotId
                && schemaId == stats.schemaId
                && Objects.equals(mergedRecordCount, stats.mergedRecordCount)
                && Objects.equals(mergedRecordSize, stats.mergedRecordSize)
                && Objects.equals(colStats, stats.colStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, schemaId, mergedRecordCount, mergedRecordSize, colStats);
    }

    @Override
    public String toString() {
        return JsonSerdeUtil.toJson(this);
    }
}
