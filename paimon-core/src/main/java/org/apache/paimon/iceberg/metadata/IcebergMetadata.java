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

package org.apache.paimon.iceberg.metadata;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Iceberg's metadata json file.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#table-metadata-fields">Iceberg spec</a>.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergMetadata {

    public static final int CURRENT_FORMAT_VERSION = 2;

    private static final String FIELD_FORMAT_VERSION = "format-version";
    private static final String FIELD_TABLE_UUID = "table-uuid";
    private static final String FIELD_LOCATION = "location";
    private static final String FIELD_LAST_SEQUENCE_NUMBER = "last-sequence-number";
    private static final String FIELD_LAST_UPDATED_MS = "last-updated-ms";
    private static final String FIELD_LAST_COLUMN_ID = "last-column-id";
    private static final String FIELD_SCHEMAS = "schemas";
    private static final String FIELD_CURRENT_SCHEMA_ID = "current-schema-id";
    private static final String FIELD_PARTITION_SPECS = "partition-specs";
    private static final String FIELD_DEFAULT_SPEC_ID = "default-spec-id";
    private static final String FIELD_LAST_PARTITION_ID = "last-partition-id";
    private static final String FIELD_SORT_ORDERS = "sort-orders";
    private static final String FIELD_DEFAULT_SORT_ORDER_ID = "default-sort-order-id";
    private static final String FIELD_SNAPSHOTS = "snapshots";
    private static final String FIELD_CURRENT_SNAPSHOT_ID = "current-snapshot-id";
    private static final String FIELD_PROPERTIES = "properties";

    @JsonProperty(FIELD_FORMAT_VERSION)
    private final int formatVersion;

    @JsonProperty(FIELD_TABLE_UUID)
    private final String tableUuid;

    @JsonProperty(FIELD_LOCATION)
    private final String location;

    @JsonProperty(FIELD_LAST_SEQUENCE_NUMBER)
    private final long lastSequenceNumber;

    @JsonProperty(FIELD_LAST_UPDATED_MS)
    private final long lastUpdatedMs;

    @JsonProperty(FIELD_LAST_COLUMN_ID)
    private final int lastColumnId;

    @JsonProperty(FIELD_SCHEMAS)
    private final List<IcebergSchema> schemas;

    @JsonProperty(FIELD_CURRENT_SCHEMA_ID)
    private final int currentSchemaId;

    @JsonProperty(FIELD_PARTITION_SPECS)
    private final List<IcebergPartitionSpec> partitionSpecs;

    @JsonProperty(FIELD_DEFAULT_SPEC_ID)
    private final int defaultSpecId;

    @JsonProperty(FIELD_LAST_PARTITION_ID)
    private final int lastPartitionId;

    @JsonProperty(FIELD_SORT_ORDERS)
    private final List<IcebergSortOrder> sortOrders;

    @JsonProperty(FIELD_DEFAULT_SORT_ORDER_ID)
    private final int defaultSortOrderId;

    @JsonProperty(FIELD_SNAPSHOTS)
    private final List<IcebergSnapshot> snapshots;

    @JsonProperty(FIELD_CURRENT_SNAPSHOT_ID)
    private final long currentSnapshotId;

    @JsonProperty(FIELD_PROPERTIES)
    @Nullable
    private final Map<String, String> properties;

    public IcebergMetadata(
            String tableUuid,
            String location,
            long lastSequenceNumber,
            int lastColumnId,
            List<IcebergSchema> schemas,
            int currentSchemaId,
            List<IcebergPartitionSpec> partitionSpecs,
            int lastPartitionId,
            List<IcebergSnapshot> snapshots,
            long currentSnapshotId) {
        this(
                CURRENT_FORMAT_VERSION,
                tableUuid,
                location,
                lastSequenceNumber,
                System.currentTimeMillis(),
                lastColumnId,
                schemas,
                currentSchemaId,
                partitionSpecs,
                IcebergPartitionSpec.SPEC_ID,
                lastPartitionId,
                Collections.singletonList(new IcebergSortOrder()),
                IcebergSortOrder.ORDER_ID,
                snapshots,
                currentSnapshotId,
                new HashMap<>());
    }

    @JsonCreator
    public IcebergMetadata(
            @JsonProperty(FIELD_FORMAT_VERSION) int formatVersion,
            @JsonProperty(FIELD_TABLE_UUID) String tableUuid,
            @JsonProperty(FIELD_LOCATION) String location,
            @JsonProperty(FIELD_LAST_SEQUENCE_NUMBER) long lastSequenceNumber,
            @JsonProperty(FIELD_LAST_UPDATED_MS) long lastUpdatedMs,
            @JsonProperty(FIELD_LAST_COLUMN_ID) int lastColumnId,
            @JsonProperty(FIELD_SCHEMAS) List<IcebergSchema> schemas,
            @JsonProperty(FIELD_CURRENT_SCHEMA_ID) int currentSchemaId,
            @JsonProperty(FIELD_PARTITION_SPECS) List<IcebergPartitionSpec> partitionSpecs,
            @JsonProperty(FIELD_DEFAULT_SPEC_ID) int defaultSpecId,
            @JsonProperty(FIELD_LAST_PARTITION_ID) int lastPartitionId,
            @JsonProperty(FIELD_SORT_ORDERS) List<IcebergSortOrder> sortOrders,
            @JsonProperty(FIELD_DEFAULT_SORT_ORDER_ID) int defaultSortOrderId,
            @JsonProperty(FIELD_SNAPSHOTS) List<IcebergSnapshot> snapshots,
            @JsonProperty(FIELD_CURRENT_SNAPSHOT_ID) long currentSnapshotId,
            @JsonProperty(FIELD_PROPERTIES) @Nullable Map<String, String> properties) {
        this.formatVersion = formatVersion;
        this.tableUuid = tableUuid;
        this.location = location;
        this.lastSequenceNumber = lastSequenceNumber;
        this.lastUpdatedMs = lastUpdatedMs;
        this.lastColumnId = lastColumnId;
        this.schemas = schemas;
        this.currentSchemaId = currentSchemaId;
        this.partitionSpecs = partitionSpecs;
        this.defaultSpecId = defaultSpecId;
        this.lastPartitionId = lastPartitionId;
        this.sortOrders = sortOrders;
        this.defaultSortOrderId = defaultSortOrderId;
        this.snapshots = snapshots;
        this.currentSnapshotId = currentSnapshotId;
        this.properties = properties;
    }

    @JsonGetter(FIELD_FORMAT_VERSION)
    public int formatVersion() {
        return formatVersion;
    }

    @JsonGetter(FIELD_TABLE_UUID)
    public String tableUuid() {
        return tableUuid;
    }

    @JsonGetter(FIELD_LOCATION)
    public String location() {
        return location;
    }

    @JsonGetter(FIELD_LAST_SEQUENCE_NUMBER)
    public long lastSequenceNumber() {
        return lastSequenceNumber;
    }

    @JsonGetter(FIELD_LAST_UPDATED_MS)
    public long lastUpdatedMs() {
        return lastUpdatedMs;
    }

    @JsonGetter(FIELD_LAST_COLUMN_ID)
    public int lastColumnId() {
        return lastColumnId;
    }

    @JsonGetter(FIELD_SCHEMAS)
    public List<IcebergSchema> schemas() {
        return schemas;
    }

    @JsonGetter(FIELD_CURRENT_SCHEMA_ID)
    public int currentSchemaId() {
        return currentSchemaId;
    }

    @JsonGetter(FIELD_PARTITION_SPECS)
    public List<IcebergPartitionSpec> partitionSpecs() {
        return partitionSpecs;
    }

    @JsonGetter(FIELD_DEFAULT_SPEC_ID)
    public int defaultSpecId() {
        return defaultSpecId;
    }

    @JsonGetter(FIELD_LAST_PARTITION_ID)
    public int lastPartitionId() {
        return lastPartitionId;
    }

    @JsonGetter(FIELD_SORT_ORDERS)
    public List<IcebergSortOrder> sortOrders() {
        return sortOrders;
    }

    @JsonGetter(FIELD_DEFAULT_SORT_ORDER_ID)
    public int defaultSortOrderId() {
        return defaultSortOrderId;
    }

    @JsonGetter(FIELD_SNAPSHOTS)
    public List<IcebergSnapshot> snapshots() {
        return snapshots;
    }

    @JsonGetter(FIELD_CURRENT_SNAPSHOT_ID)
    public long currentSnapshotId() {
        return currentSnapshotId;
    }

    @JsonGetter(FIELD_PROPERTIES)
    public Map<String, String> properties() {
        return properties == null ? new HashMap<>() : properties;
    }

    public IcebergSnapshot currentSnapshot() {
        for (IcebergSnapshot snapshot : snapshots) {
            if (snapshot.snapshotId() == currentSnapshotId) {
                return snapshot;
            }
        }
        throw new RuntimeException(
                "Cannot find snapshot with id " + currentSnapshotId + ", this is unexpected.");
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public static IcebergMetadata fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, IcebergMetadata.class);
    }

    public static IcebergMetadata fromPath(FileIO fileIO, Path path) {
        try {
            String json = fileIO.readFileUtf8(path);
            return fromJson(json);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read Iceberg metadata from path " + path, e);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                formatVersion,
                tableUuid,
                location,
                lastSequenceNumber,
                lastUpdatedMs,
                lastColumnId,
                schemas,
                currentSchemaId,
                partitionSpecs,
                defaultSpecId,
                lastPartitionId,
                sortOrders,
                defaultSortOrderId,
                snapshots,
                currentSnapshotId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergMetadata)) {
            return false;
        }

        IcebergMetadata that = (IcebergMetadata) o;
        return formatVersion == that.formatVersion
                && Objects.equals(tableUuid, that.tableUuid)
                && Objects.equals(location, that.location)
                && lastSequenceNumber == that.lastSequenceNumber
                && lastUpdatedMs == that.lastUpdatedMs
                && lastColumnId == that.lastColumnId
                && Objects.equals(schemas, that.schemas)
                && currentSchemaId == that.currentSchemaId
                && Objects.equals(partitionSpecs, that.partitionSpecs)
                && defaultSpecId == that.defaultSpecId
                && lastPartitionId == that.lastPartitionId
                && Objects.equals(sortOrders, that.sortOrders)
                && defaultSortOrderId == that.defaultSortOrderId
                && Objects.equals(snapshots, that.snapshots)
                && currentSnapshotId == that.currentSnapshotId;
    }
}
